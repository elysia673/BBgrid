// Package tag 标签管理插件
//
// 内置组件：直接使用 Dispatcher，不需要 SDK。
package tag

import (
	alog "BBgrid/common/log"
	"BBgrid/common/persist"
	"BBgrid/common/plugin"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

func init() {
	plugin.Register("tag", func() plugin.Plugin {
		return &TagPlugin{
			tags:   make(map[string]map[string]string),
			stopCh: make(chan struct{}),
		}
	})
}

type TagPlugin struct {
	dispatcher plugin.Dispatcher
	tags       map[string]map[string]string
	mu         sync.RWMutex
	stopCh     chan struct{}
}

func (p *TagPlugin) Name() string    { return "tag" }
func (p *TagPlugin) Version() string { return "1.0.0" }

func (p *TagPlugin) Init(dispatcher plugin.Dispatcher, state plugin.StateStore, config map[string]any) error {
	p.dispatcher = dispatcher
	persist.Register(p)
	alog.Info("tag", "plugin initialized")
	return nil
}

func (p *TagPlugin) Run() error {
	alog.Info("tag", "plugin started")
	<-p.stopCh
	alog.Info("tag", "plugin stopped")
	return nil
}

func (p *TagPlugin) Stop() { close(p.stopCh) }

func (p *TagPlugin) Actions() []plugin.Action {
	return []plugin.Action{
		{Name: "tag.set", Description: "设置客户端标签", Params: []plugin.Param{{Name: "client_id", Type: "string", Required: true}, {Name: "key", Type: "string", Required: true}, {Name: "value", Type: "string", Required: true}}},
		{Name: "tag.delete", Description: "删除客户端标签", Params: []plugin.Param{{Name: "client_id", Type: "string", Required: true}, {Name: "key", Type: "string", Required: true}}},
		{Name: "tag.get", Description: "获取客户端的所有标签", Params: []plugin.Param{{Name: "client_id", Type: "string", Required: true}}},
		{Name: "tag.list", Description: "按标签筛选客户端", Params: []plugin.Param{{Name: "tag", Type: "string", Required: false}}},
	}
}

func (p *TagPlugin) Capabilities() []plugin.Capability { return nil }

func (p *TagPlugin) Export() any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make(map[string]map[string]string, len(p.tags))
	for k, v := range p.tags {
		cp := make(map[string]string, len(v))
		for kk, vv := range v {
			cp[kk] = vv
		}
		result[k] = cp
	}
	return result
}

func (p *TagPlugin) Import(data any) {
	raw, ok := data.(map[string]any)
	if !ok {
		return
	}
	result := make(map[string]map[string]string)
	for clientID, tagsRaw := range raw {
		tagsMap, ok := tagsRaw.(map[string]any)
		if !ok {
			continue
		}
		tags := make(map[string]string, len(tagsMap))
		for k, v := range tagsMap {
			if s, ok := v.(string); ok {
				tags[k] = s
			}
		}
		result[clientID] = tags
	}
	p.mu.Lock()
	p.tags = result
	p.mu.Unlock()
}

func (p *TagPlugin) SetTag(clientID, key, value string) {
	p.mu.Lock()
	if p.tags[clientID] == nil {
		p.tags[clientID] = make(map[string]string)
	}
	p.tags[clientID][key] = value
	p.mu.Unlock()
}

func (p *TagPlugin) DeleteTag(clientID, key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	tags, ok := p.tags[clientID]
	if !ok {
		return false
	}
	if _, exists := tags[key]; !exists {
		return false
	}
	delete(tags, key)
	if len(tags) == 0 {
		delete(p.tags, clientID)
	}
	return true
}

func (p *TagPlugin) GetTags(clientID string) map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	tags, ok := p.tags[clientID]
	if !ok {
		return nil
	}
	result := make(map[string]string, len(tags))
	for k, v := range tags {
		result[k] = v
	}
	return result
}

type ClientTags struct {
	ClientID string            `json:"client_id"`
	Tags     map[string]string `json:"tags"`
}

func (p *TagPlugin) ListByTag(filter string) []ClientTags {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var filterKey, filterVal string
	if filter != "" {
		parts := strings.SplitN(filter, "=", 2)
		filterKey = parts[0]
		if len(parts) == 2 {
			filterVal = parts[1]
		}
	}
	var result []ClientTags
	for clientID, tags := range p.tags {
		if filterKey != "" {
			val, exists := tags[filterKey]
			if !exists {
				continue
			}
			if filterVal != "" && val != filterVal {
				continue
			}
		}
		ct := ClientTags{ClientID: clientID, Tags: make(map[string]string, len(tags))}
		for k, v := range tags {
			ct.Tags[k] = v
		}
		result = append(result, ct)
	}
	return result
}

func (p *TagPlugin) HandleSetTag(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if clientID == "" || key == "" || value == "" {
		http.Error(w, `{"code":400,"msg":"client_id, key, value are required"}`, http.StatusBadRequest)
		return
	}
	p.SetTag(clientID, key, value)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"client_id": clientID, "key": key, "value": value}})
}

func (p *TagPlugin) HandleDeleteTag(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	key := r.URL.Query().Get("key")
	if clientID == "" || key == "" {
		http.Error(w, `{"code":400,"msg":"client_id and key are required"}`, http.StatusBadRequest)
		return
	}
	if !p.DeleteTag(clientID, key) {
		http.Error(w, `{"code":404,"msg":"tag not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]string{"client_id": clientID, "key": key}})
}

func (p *TagPlugin) HandleGetTags(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		http.Error(w, `{"code":400,"msg":"client_id is required"}`, http.StatusBadRequest)
		return
	}
	tags := p.GetTags(clientID)
	if tags == nil {
		tags = map[string]string{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"client_id": clientID, "tags": tags}})
}

func (p *TagPlugin) HandleListByTag(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("tag")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": p.ListByTag(filter)})
}

func (p *TagPlugin) GetHTTPHandlers() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"tag.set":    p.HandleSetTag,
		"tag.delete": p.HandleDeleteTag,
		"tag.get":    p.HandleGetTags,
		"tag.list":   p.HandleListByTag,
	}
}
