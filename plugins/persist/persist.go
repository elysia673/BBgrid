// Package persist 状态持久化插件
//
// 内置组件：直接使用 runtime.Core。
package persist

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"BBgrid/common/persist"
	"BBgrid/common/proto"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func init() {
	// 注册到旧的 plugin registry (过渡用)
}

type PersistPlugin struct {
	core     *runtime.Core
	dataDir  string
	stopCh   chan struct{}
	stopOnce sync.Once
	dirtyCh  chan struct{}
}

func New() *PersistPlugin {
	return &PersistPlugin{
		stopCh:  make(chan struct{}),
		dirtyCh: make(chan struct{}, 1),
	}
}

func (p *PersistPlugin) Name() string    { return "persist" }
func (p *PersistPlugin) Version() string { return "1.0.0" }

func (p *PersistPlugin) Init(core *runtime.Core, config map[string]any) error {
	p.core = core
	if dir, ok := config["data_dir"].(string); ok && dir != "" {
		p.dataDir = dir
	} else {
		p.dataDir = "data"
	}

	// 订阅所有资源类型
	for _, rt := range []string{
		proto.ResourceTypeClient,
		proto.ResourceTypeProxy,
		proto.ResourceTypeRelay,
		proto.ResourceTypeNamespace,
	} {
		core.EventBus().Subscribe(rt, func(event proto.GenericEvent) {
			p.NotifyDirty()
		})
	}

	// 注册 Action Schema
	schema := p.Schema()
	for _, s := range schema {
		core.Capability().Register(runtime.Capability{
			Name:        s.Name,
			Description: s.Description,
			Source:      runtime.SourceInternal,
			Schema:      s,
		}, p.handleAction)
	}

	alog.Info(alog.CatSystem, "persist 插件初始化完成")
	return nil
}

func (p *PersistPlugin) Run() error {
	p.core.SetRestoring(true)

	// 1. 从 MetaStore 恢复 actual state（proxy/relay）— 始终执行
	p.restoreFromMetaStore()
	// 2. 从 Snapshot 恢复 desired state — 覆盖 MetaStore 的 desired
	if p.core.Storage() != nil && p.core.Storage().Snapshots().HasSnapshot() {
		p.restoreSnapshot()
	}
	// 3. 从 persist.json 恢复插件状态（tag 等）
	p.restoreAll()

	p.core.SetRestoring(false)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			p.flushAll()
			return nil
		case <-p.dirtyCh:
			select {
			case <-p.stopCh:
				p.flushAll()
				return nil
			case <-time.After(1 * time.Second):
				p.flushAll()
			}
		case <-ticker.C:
			select {
			case <-p.dirtyCh:
				p.flushAll()
			default:
			}
		}
	}
}

func (p *PersistPlugin) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
}

func (p *PersistPlugin) Schema() []runtime.ActionSchema {
	return []runtime.ActionSchema{
		{
			Name:        "persist.status",
			Description: "查看持久化状态",
			Params:      []runtime.ParamSchema{},
		},
	}
}

func (p *PersistPlugin) handleAction(ctx *runtime.ActionContext) (*runtime.ActionResult, error) {
	switch ctx.Action {
	case "persist.status":
		providers := persist.GetAll()
		names := make([]string, 0, len(providers))
		for name := range providers {
			names = append(names, name)
		}
		return &runtime.ActionResult{
			Code: 200,
			Data: map[string]any{"data_dir": p.dataDir, "providers": names},
		}, nil
	default:
		return &runtime.ActionResult{Code: 404, Msg: "unknown action"}, nil
	}
}

func (p *PersistPlugin) NotifyDirty() {
	select {
	case p.dirtyCh <- struct{}{}:
	default:
	}
}

func (p *PersistPlugin) restoreAll() {
	states := p.loadFromDisk()
	providers := persist.GetAll()
	restored := 0
	for name, data := range states {
		if provider, ok := providers[name]; ok {
			provider.Import(data)
			restored++
			alog.Info(alog.CatSystem, "restored provider", "name", name)
		}
	}
	if restored > 0 {
		alog.Info(alog.CatSystem, "restore complete", "count", restored)
	}
}

// restoreSnapshot 从 Snapshot 恢复状态，通过 EventBus 发布事件以触发监听器创建
// restoreSnapshot 从 Snapshot 恢复 desired state（不发事件，避免重复）
func (p *PersistPlugin) restoreSnapshot() {
	storage := p.core.Storage()
	if storage == nil || !storage.Snapshots().HasSnapshot() {
		return
	}
	snapshot, err := storage.Snapshots().Load()
	if err != nil {
		alog.Warn(alog.CatSystem, "加载快照失败", "error", err)
		return
	}

	// 直接恢复 desired state，不通过 EventBus（避免和 restoreFromMetaStore 重复）
	restoreData := make(map[string]any, len(snapshot.State))
	for k, v := range snapshot.State {
		restoreData[k] = v
	}
	p.core.StateStore().Restore(restoreData)
	alog.Info(alog.CatSystem, "快照恢复完成 (desired state only)")
}

// restoreFromMetaStore 从 MetaStore 恢复 proxy/relay，通过 EventBus 发布（恢复模式跳过持久化）
func (p *PersistPlugin) restoreFromMetaStore() {
	storage := p.core.Storage()
	if storage == nil {
		return
	}

	if proxies := storage.Meta().List("proxy"); proxies != nil {
		count := 0
		for metaKey, raw := range proxies {
			rk, err := proto.ParseResourceKey(metaKey)
			if err != nil {
				alog.Warn(alog.CatSystem, "跳过无效 proxy key", "key", metaKey, "error", err)
				continue
			}
			proxy := parseProxy(raw)
			if proxy == nil || proxy.ClientID == "" || proxy.RemotePort == 0 {
				alog.Warn(alog.CatSystem, "跳过无效 proxy 条目", "key", metaKey)
				continue
			}
			p.core.Publish(proto.NewGenericEvent(
				proto.ResourceKey{
					Type:      proto.ResourceTypeProxy,
					Namespace: proto.NamespaceDefault,
					Name:      rk.Name,
				},
				proto.EventAdded,
				*proxy,
			))
			count++
		}
		alog.Info(alog.CatSystem, "proxy 恢复完成", "count", count)
	}

	if relays := storage.Meta().List("relay"); relays != nil {
		count := 0
		for metaKey, raw := range relays {
			rk, err := proto.ParseResourceKey(metaKey)
			if err != nil {
				alog.Warn(alog.CatSystem, "跳过无效 relay key", "key", metaKey, "error", err)
				continue
			}
			relay := parseRelay(raw)
			if relay == nil {
				continue
			}
			p.core.Publish(proto.NewGenericEvent(
				proto.ResourceKey{
					Type:      proto.ResourceTypeRelay,
					Namespace: proto.NamespaceDefault,
					Name:      rk.Name,
				},
				proto.EventAdded,
				*relay,
			))
			count++
		}
		alog.Info(alog.CatSystem, "relay 恢复完成", "count", count)
	}
}

func parseProxy(raw any) *runtime.ProxyState {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	p := &runtime.ProxyState{}
	if v, ok := m["client_id"].(string); ok {
		p.ClientID = v
	}
	if v, ok := m["remote_port"].(float64); ok {
		p.RemotePort = int(v)
	}
	if v, ok := m["local_port"].(float64); ok {
		p.LocalPort = int(v)
	}
	if v, ok := m["local_ip"].(string); ok {
		p.LocalIP = v
	}
	if v, ok := m["protocol"].(string); ok {
		p.Protocol = v
	}
	if v, ok := m["bind_addr"].(string); ok {
		p.BindAddr = v
	}
	return p
}

func parseRelay(raw any) *runtime.RelaySession {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	r := &runtime.RelaySession{}
	if v, ok := m["session_id"].(string); ok {
		r.ID = v
	}
	if v, ok := m["source_client"].(string); ok {
		r.SourceClient = v
	}
	if v, ok := m["target_client"].(string); ok {
		r.TargetClient = v
	}
	if v, ok := m["protocol"].(string); ok {
		r.Protocol = v
	}
	if v, ok := m["source_port"].(float64); ok {
		r.SourcePort = int(v)
	}
	if v, ok := m["target_port"].(float64); ok {
		r.TargetPort = int(v)
	}
	if v, ok := m["target_local_ip"].(string); ok {
		r.TargetLocalIP = v
	}
	if v, ok := m["source_local_ip"].(string); ok {
		r.SourceLocalIP = v
	}
	if v, ok := m["token"].(string); ok {
		r.Token = v
	}
	return r
}

func (p *PersistPlugin) flushAll() {
	providers := persist.GetAll()
	existing := p.loadFromDisk()
	for name, provider := range providers {
		existing[name] = provider.Export()
	}
	p.saveToDisk(existing)
}

func (p *PersistPlugin) filePath() string {
	return filepath.Join(p.dataDir, "persist.json")
}

func (p *PersistPlugin) loadFromDisk() map[string]any {
	data, err := os.ReadFile(p.filePath())
	if err != nil {
		return make(map[string]any)
	}
	var states map[string]any
	if err := json.Unmarshal(data, &states); err != nil {
		return make(map[string]any)
	}
	return states
}

func (p *PersistPlugin) saveToDisk(states map[string]any) {
	data, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		alog.Error(alog.CatSystem, "persist 序列化失败", "error", err)
		return
	}
	if err := os.WriteFile(p.filePath(), data, 0644); err != nil {
		alog.Error(alog.CatSystem, "persist 写入失败", "error", err, "path", p.filePath())
	}
}
