// Package persist 状态持久化插件
//
// 内置组件：直接使用 Dispatcher，不需要 SDK。
package persist

import (
	alog "BBgrid/common/log"
	"BBgrid/common/persist"
	"BBgrid/common/plugin"
	"BBgrid/common/proto"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func init() {
	plugin.Register("persist", func() plugin.Plugin {
		return &PersistPlugin{
			stopCh:  make(chan struct{}),
			dirtyCh: make(chan struct{}, 1),
		}
	})
}

type PersistPlugin struct {
	dispatcher plugin.Dispatcher
	dataDir    string
	stopCh     chan struct{}
	dirtyCh    chan struct{}
}

func (p *PersistPlugin) Name() string    { return "persist" }
func (p *PersistPlugin) Version() string { return "1.0.0" }

func (p *PersistPlugin) Init(dispatcher plugin.Dispatcher, state plugin.StateStore, config map[string]any) error {
	p.dispatcher = dispatcher
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
		dispatcher.SubscribeByType(rt, func(event proto.GenericEvent) {
			p.NotifyDirty()
		})
	}

	alog.Info("persist", "plugin initialized")
	return nil
}

func (p *PersistPlugin) Run() error {
	alog.Info("persist", "plugin started")
	p.restoreAll()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			p.flushAll()
			alog.Info("persist", "plugin stopped")
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

func (p *PersistPlugin) Stop() { close(p.stopCh) }

func (p *PersistPlugin) Actions() []plugin.Action {
	return []plugin.Action{
		{Name: "persist.status", Description: "查看持久化状态"},
	}
}

func (p *PersistPlugin) Capabilities() []plugin.Capability { return nil }

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
			alog.Info("persist", "restored", "name", name)
		}
	}
	if restored > 0 {
		alog.Info("persist", "restore complete", "count", restored)
	}
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
		return
	}
	os.WriteFile(p.filePath(), data, 0644)
}

func (p *PersistPlugin) HandleStatus(w http.ResponseWriter, r *http.Request) {
	providers := persist.GetAll()
	names := make([]string, 0, len(providers))
	for name := range providers {
		names = append(names, name)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"data_dir": p.dataDir, "providers": names}})
}

func (p *PersistPlugin) GetHTTPHandlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"persist.status": p.HandleStatus,
	}
}
