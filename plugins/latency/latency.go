// Package latency 延迟监控插件
//
// 内置组件：直接使用 EventBus 订阅事件。
package latency

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"sync"
	"time"
)

func init() {
	// 注册到旧的 plugin registry (过渡用)
	// 新代码应该直接使用 runtime.CapabilityRegistry
}

type LatencyRecord struct {
	ClientID  string        `json:"client_id"`
	Latency   time.Duration `json:"latency"`
	UpdatedAt time.Time     `json:"updated_at"`
}

type LatencyPlugin struct {
	core       *runtime.Core
	config     map[string]any
	latencyMap map[string]*LatencyRecord
	mu         sync.RWMutex
	stopCh     chan struct{}
}

func New() *LatencyPlugin {
	return &LatencyPlugin{
		latencyMap: make(map[string]*LatencyRecord),
		stopCh:     make(chan struct{}),
	}
}

func (p *LatencyPlugin) Name() string    { return "latency" }
func (p *LatencyPlugin) Version() string { return "1.0.0" }

func (p *LatencyPlugin) Init(core *runtime.Core, config map[string]any) error {
	p.core = core
	p.config = config

	// 订阅客户端事件
	core.EventBus().Subscribe(proto.ResourceTypeClient, p.handleClientEvent)

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

	alog.Info(alog.CatSystem, "latency 插件初始化完成")
	return nil
}

func (p *LatencyPlugin) Run() error {
	// 定期清理过期记录
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.cleanupExpired()
		case <-p.stopCh:
			return nil
		}
	}
}

func (p *LatencyPlugin) Stop() {
	close(p.stopCh)
}

func (p *LatencyPlugin) Schema() []runtime.ActionSchema {
	return []runtime.ActionSchema{
		{
			Name:        "latency.get",
			Description: "获取指定客户端的延迟",
			Params: []runtime.ParamSchema{
				{Name: "client_id", Type: "string", Required: true},
			},
		},
		{
			Name:        "latency.list",
			Description: "列出所有客户端的延迟",
			Params:      []runtime.ParamSchema{},
		},
	}
}

func (p *LatencyPlugin) handleAction(ctx *runtime.ActionContext) (*runtime.ActionResult, error) {
	switch ctx.Action {
	case "latency.get":
		clientID, _ := ctx.Params["client_id"].(string)
		if clientID == "" {
			return &runtime.ActionResult{Code: 400, Msg: "missing client_id"}, nil
		}
		record, ok := p.GetLatency(clientID)
		if !ok {
			return &runtime.ActionResult{Code: 404, Msg: "no latency record"}, nil
		}
		return &runtime.ActionResult{Code: 200, Data: record}, nil

	case "latency.list":
		records := p.GetAllLatency()
		if len(records) == 0 {
			return &runtime.ActionResult{Code: 200, Msg: "没有延迟记录"}, nil
		}
		return &runtime.ActionResult{Code: 200, Data: records}, nil

	default:
		return &runtime.ActionResult{Code: 404, Msg: "unknown action"}, nil
	}
}

func (p *LatencyPlugin) handleClientEvent(event proto.GenericEvent) {
	if event.EventType != proto.EventModified {
		return
	}

	payload, ok := event.Payload.(map[string]any)
	if !ok {
		return
	}

	// 处理 int64 和 float64 两种类型（JSON 反序列化会将数字变为 float64）
	var latencyMs int64
	switch v := payload["latency"].(type) {
	case int64:
		latencyMs = v
	case float64:
		latencyMs = int64(v)
	default:
		return
	}

	clientID := event.Resource.Name
	p.mu.Lock()
	p.latencyMap[clientID] = &LatencyRecord{
		ClientID:  clientID,
		Latency:   time.Duration(latencyMs) * time.Millisecond,
		UpdatedAt: time.Now(),
	}
	p.mu.Unlock()
}

func (p *LatencyPlugin) cleanupExpired() {
	p.mu.Lock()
	defer p.mu.Unlock()

	cutoff := time.Now().Add(-5 * time.Minute)
	for id, record := range p.latencyMap {
		if record.UpdatedAt.Before(cutoff) {
			delete(p.latencyMap, id)
		}
	}
}

func (p *LatencyPlugin) GetLatency(clientID string) (*LatencyRecord, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	record, ok := p.latencyMap[clientID]
	return record, ok
}

func (p *LatencyPlugin) GetAllLatency() []*LatencyRecord {
	p.mu.RLock()
	defer p.mu.RUnlock()
	records := make([]*LatencyRecord, 0, len(p.latencyMap))
	for _, r := range p.latencyMap {
		records = append(records, r)
	}
	return records
}
