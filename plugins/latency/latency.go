// Package latency 延迟监控插件
//
// 内置组件：直接使用 Dispatcher，不需要 SDK。
package latency

import (
	alog "BBgrid/common/log"
	"BBgrid/common/plugin"
	"BBgrid/common/proto"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

func init() {
	plugin.Register("latency", func() plugin.Plugin {
		return &LatencyPlugin{
			latencyMap: make(map[string]*LatencyRecord),
			stopCh:     make(chan struct{}),
		}
	})
}

type LatencyRecord struct {
	ClientID  string        `json:"client_id"`
	Latency   time.Duration `json:"latency"`
	UpdatedAt time.Time     `json:"updated_at"`
}

type LatencyPlugin struct {
	dispatcher plugin.Dispatcher
	config     map[string]any
	latencyMap map[string]*LatencyRecord
	mu         sync.RWMutex
	stopCh     chan struct{}
}

func (p *LatencyPlugin) Name() string    { return "latency" }
func (p *LatencyPlugin) Version() string { return "1.0.0" }

func (p *LatencyPlugin) Init(dispatcher plugin.Dispatcher, state plugin.StateStore, config map[string]any) error {
	p.dispatcher = dispatcher
	p.config = config

	// 直接订阅客户端事件
	dispatcher.SubscribeByType(proto.ResourceTypeClient, p.handleClientEvent)

	alog.Info("latency", "plugin initialized")
	return nil
}

func (p *LatencyPlugin) Run() error {
	alog.Info("latency", "plugin started")
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			alog.Info("latency", "plugin stopped")
			return nil
		case <-ticker.C:
			p.cleanupExpired()
		}
	}
}

func (p *LatencyPlugin) Stop() { close(p.stopCh) }

func (p *LatencyPlugin) Actions() []plugin.Action {
	return []plugin.Action{
		{Name: "latency.get", Description: "获取指定客户端的延迟", Params: []plugin.Param{{Name: "client_id", Type: "string", Required: true}}},
		{Name: "latency.list", Description: "获取所有客户端的延迟"},
	}
}

func (p *LatencyPlugin) Capabilities() []plugin.Capability {
	return []plugin.Capability{
		{ResourceType: proto.ResourceTypeClient, EventTypes: []proto.EventType{proto.EventModified}},
	}
}

func (p *LatencyPlugin) HandleResource(event proto.GenericEvent) {
	p.handleClientEvent(event)
}

func (p *LatencyPlugin) handleClientEvent(event proto.GenericEvent) {
	if event.EventType != proto.EventModified {
		return
	}
	payload, ok := event.Payload.(map[string]any)
	if !ok {
		return
	}
	latencyVal, ok := payload["latency"]
	if !ok {
		return
	}
	var latency time.Duration
	switch v := latencyVal.(type) {
	case time.Duration:
		latency = v
	case float64:
		latency = time.Duration(v)
	default:
		return
	}
	clientID := event.Resource.Name
	if clientID == "" {
		return
	}
	p.mu.Lock()
	p.latencyMap[clientID] = &LatencyRecord{ClientID: clientID, Latency: latency, UpdatedAt: time.Now()}
	p.mu.Unlock()
}

func (p *LatencyPlugin) cleanupExpired() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, record := range p.latencyMap {
		if time.Since(record.UpdatedAt) > 5*time.Minute {
			delete(p.latencyMap, id)
		}
	}
}

func (p *LatencyPlugin) GetLatency(clientID string) (*LatencyRecord, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	r, ok := p.latencyMap[clientID]
	return r, ok
}

func (p *LatencyPlugin) GetAllLatency() map[string]*LatencyRecord {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make(map[string]*LatencyRecord)
	for k, v := range p.latencyMap {
		result[k] = v
	}
	return result
}

func (p *LatencyPlugin) HandleGetLatency(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		http.Error(w, `{"code":400,"msg":"client_id is required"}`, http.StatusBadRequest)
		return
	}
	record, ok := p.GetLatency(clientID)
	if !ok {
		http.Error(w, `{"code":404,"msg":"client not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": record})
}

func (p *LatencyPlugin) HandleListLatency(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": p.GetAllLatency()})
}

func (p *LatencyPlugin) GetHTTPHandlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"latency.get":  p.HandleGetLatency,
		"latency.list": p.HandleListLatency,
	}
}
