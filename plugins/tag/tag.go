// Package tag 标签管理插件
//
// 内置组件：直接使用 runtime.Core。
package tag

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"BBgrid/common/persist"
	"strings"
	"sync"
)

func init() {
	// 注册到旧的 plugin registry (过渡用)
}

type TagPlugin struct {
	core   *runtime.Core
	tags   map[string]map[string]string
	mu     sync.RWMutex
	stopCh chan struct{}
}

func New() *TagPlugin {
	return &TagPlugin{
		tags:   make(map[string]map[string]string),
		stopCh: make(chan struct{}),
	}
}

func (p *TagPlugin) Name() string    { return "tag" }
func (p *TagPlugin) Version() string { return "1.0.0" }

func (p *TagPlugin) Init(core *runtime.Core, config map[string]any) error {
	p.core = core

	// 注册持久化 provider
	persist.Register(p)

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

	alog.Info(alog.CatSystem, "tag 插件初始化完成")
	return nil
}

func (p *TagPlugin) Run() error {
	<-p.stopCh
	return nil
}

func (p *TagPlugin) Stop() { close(p.stopCh) }

func (p *TagPlugin) Schema() []runtime.ActionSchema {
	return []runtime.ActionSchema{
		{
			Name:        "tag.set",
			Description: "设置客户端标签",
			Params: []runtime.ParamSchema{
				{Name: "client_id", Type: "string", Required: true},
				{Name: "key", Type: "string", Required: true},
				{Name: "value", Type: "string", Required: true},
			},
		},
		{
			Name:        "tag.delete",
			Description: "删除客户端标签",
			Params: []runtime.ParamSchema{
				{Name: "client_id", Type: "string", Required: true},
				{Name: "key", Type: "string", Required: true},
			},
		},
		{
			Name:        "tag.get",
			Description: "获取客户端的所有标签",
			Params: []runtime.ParamSchema{
				{Name: "client_id", Type: "string", Required: true},
			},
		},
		{
			Name:        "tag.list",
			Description: "按标签筛选客户端",
			Params: []runtime.ParamSchema{
				{Name: "tag", Type: "string", Required: false},
			},
		},
	}
}

func (p *TagPlugin) handleAction(ctx *runtime.ActionContext) (*runtime.ActionResult, error) {
	switch ctx.Action {
	case "tag.set":
		clientID, _ := ctx.Params["client_id"].(string)
		key, _ := ctx.Params["key"].(string)
		value, _ := ctx.Params["value"].(string)
		if clientID == "" || key == "" || value == "" {
			return &runtime.ActionResult{Code: 400, Msg: "client_id, key, value are required"}, nil
		}
		p.SetTag(clientID, key, value)
		return &runtime.ActionResult{Code: 200, Data: map[string]string{"client_id": clientID, "key": key, "value": value}}, nil

	case "tag.delete":
		clientID, _ := ctx.Params["client_id"].(string)
		key, _ := ctx.Params["key"].(string)
		if clientID == "" || key == "" {
			return &runtime.ActionResult{Code: 400, Msg: "client_id and key are required"}, nil
		}
		if !p.DeleteTag(clientID, key) {
			return &runtime.ActionResult{Code: 404, Msg: "tag not found"}, nil
		}
		return &runtime.ActionResult{Code: 200, Data: map[string]string{"client_id": clientID, "key": key}}, nil

	case "tag.get":
		clientID, _ := ctx.Params["client_id"].(string)
		if clientID == "" {
			return &runtime.ActionResult{Code: 400, Msg: "client_id is required"}, nil
		}
		tags := p.GetTags(clientID)
		if tags == nil {
			tags = map[string]string{}
		}
		return &runtime.ActionResult{Code: 200, Data: map[string]any{"client_id": clientID, "tags": tags}}, nil

	case "tag.list":
		filter, _ := ctx.Params["tag"].(string)
		result := p.ListByTag(filter)
		if len(result) == 0 {
			return &runtime.ActionResult{Code: 200, Msg: "没有标签"}, nil
		}
		return &runtime.ActionResult{Code: 200, Data: result}, nil

	default:
		return &runtime.ActionResult{Code: 404, Msg: "unknown action"}, nil
	}
}

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
