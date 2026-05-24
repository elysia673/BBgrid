// Package plugin 提供 Server 端的通用插件 gRPC 服务实现
//
// 所有外部插件通过此服务与 Server 通信。
// Server 是事件中枢，插件通过 gRPC 订阅/发布事件。
package plugin

import (
	"BBgrid/common/plugin/proto/pb"
	"BBgrid/common/proto"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	alog "BBgrid/common/log"
)

// PluginGRPCServer 实现 gRPC PluginService
//
// 职责：
// - 管理外部插件的连接和订阅
// - 将 Server 内部事件转发给外部插件
// - 接收外部插件发布的事件并注入 Server
type PluginGRPCServer struct {
	pb.UnimplementedPluginServiceServer

	mu          sync.RWMutex
	plugins     map[string]*pluginState
	subscribers map[string]chan *pb.EventBatch
	actionReg   ActionRegistry
}

type pluginState struct {
	ID           string
	Version      string
	Capabilities []*pb.Capability
	Actions      []*pb.ActionDeclaration
	ConnectedAt  int64
	executeCh    chan *executeRequest
	resultCh     chan *executeResult
}

type executeRequest struct {
	Action string
	Params map[string]any
	Result chan *executeResult
}

type executeResult struct {
	Data  any
	Error string
}

// ActionRegistry 外部插件 Action 注册接口
type ActionRegistry interface {
	RegisterExternal(pluginID string, actions []Action)
	UnregisterPlugin(pluginID string)
}

// NewPluginGRPCServer 创建 PluginGRPCServer
func NewPluginGRPCServer(actionReg ActionRegistry) *PluginGRPCServer {
	return &PluginGRPCServer{
		plugins:     make(map[string]*pluginState),
		subscribers: make(map[string]chan *pb.EventBatch),
		actionReg:   actionReg,
	}
}

// Subscribe 实现 gRPC streaming，外部插件连接后持续接收事件
func (s *PluginGRPCServer) Subscribe(req *pb.SubscribeRequest, stream pb.PluginService_SubscribeServer) error {
	pluginID := req.PluginId
	if pluginID == "" {
		pluginID = "anonymous"
	}

	alog.Info(alog.CatSystem, "外部插件已订阅", "pluginID", pluginID)

	// 注册 subscriber
	updateCh := make(chan *pb.EventBatch, 64)
	s.mu.Lock()
	if oldCh, exists := s.subscribers[pluginID]; exists {
		close(oldCh)
		alog.Warn(alog.CatSystem, "插件重复连接，关闭旧连接", "pluginID", pluginID)
	}
	s.subscribers[pluginID] = updateCh
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.subscribers[pluginID] == updateCh {
			delete(s.subscribers, pluginID)
		}
		s.mu.Unlock()
		close(updateCh)
		alog.Info(alog.CatSystem, "外部插件已断开", "pluginID", pluginID)
	}()

	// 持续推送事件
	for batch := range updateCh {
		if err := stream.Send(batch); err != nil {
			return err
		}
	}

	return nil
}

// Publish 实现外部插件发布事件到 Server
func (s *PluginGRPCServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	for _, event := range req.Events {
		// 检查是否是 Action 执行结果
		if event.Resource != nil && event.Resource.Type == "action_result" {
			s.handleActionResult(req.PluginId, event)
			continue
		}
		// 其他事件可以注入 Dispatcher
		alog.Info(alog.CatSystem, "收到插件事件",
			"pluginID", req.PluginId,
			"type", event.EventType)
	}

	return &pb.PublishResponse{Success: true}, nil
}

// handleActionResult 处理插件返回的 Action 执行结果
func (s *PluginGRPCServer) handleActionResult(pluginID string, event *pb.PluginEvent) {
	s.mu.RLock()
	p, exists := s.plugins[pluginID]
	s.mu.RUnlock()

	if !exists || p.resultCh == nil {
		return
	}

	var result executeResult
	if event.EventType == "ERROR" {
		result.Error = string(event.Payload)
	} else {
		json.Unmarshal(event.Payload, &result.Data)
	}

	select {
	case p.resultCh <- &result:
	default:
	}
}

// Register 实现外部插件注册能力
func (s *PluginGRPCServer) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	pluginID := req.PluginId
	if pluginID == "" {
		return &pb.RegisterResponse{Success: false, Error: "plugin_id is required"}, nil
	}

	s.mu.Lock()
	s.plugins[pluginID] = &pluginState{
		ID:           pluginID,
		Version:      req.Version,
		Capabilities: req.Capabilities,
		Actions:      req.Actions,
		executeCh:    make(chan *executeRequest, 16),
	}
	s.mu.Unlock()

	// 注册外部插件的 Actions 到统一注册表
	if s.actionReg != nil && len(req.Actions) > 0 {
		actions := make([]Action, 0, len(req.Actions))
		for _, a := range req.Actions {
			actions = append(actions, Action{
				Name:        a.Name,
				Description: a.Description,
			})
		}
		s.actionReg.RegisterExternal(pluginID, actions)
	}

	alog.Info(alog.CatSystem, "外部插件已注册",
		"pluginID", pluginID,
		"version", req.Version,
		"capabilities", len(req.Capabilities),
		"actions", len(req.Actions))

	return &pb.RegisterResponse{Success: true}, nil
}

// Execute 实现 Server 调用外部插件的 Action
func (s *PluginGRPCServer) Execute(ctx context.Context, req *pb.ExecuteRequest) (*pb.ExecuteResponse, error) {
	// TODO: 路由到对应的外部插件执行
	return &pb.ExecuteResponse{Success: false, Error: "not implemented"}, nil
}

// Heartbeat 实现心跳保活
func (s *PluginGRPCServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{ServerTime: 0}, nil
}

// ==================== 内部方法 ====================

// BroadcastEvent 向所有订阅者广播事件
func (s *PluginGRPCServer) BroadcastEvent(event proto.GenericEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	payload, _ := json.Marshal(event.Payload)

	pbEvent := &pb.PluginEvent{
		Id:        event.ID,
		Sequence:  event.Sequence,
		Timestamp: event.Timestamp,
		Resource: &pb.ResourceKey{
			Type:      event.Resource.Type,
			Namespace: event.Resource.Namespace,
			Name:      event.Resource.Name,
		},
		EventType: string(event.EventType),
		Payload:   payload,
	}

	batch := &pb.EventBatch{Events: []*pb.PluginEvent{pbEvent}}

	for pluginID, ch := range s.subscribers {
		select {
		case ch <- batch:
		default:
			alog.Warn(alog.CatSystem, "插件事件队列已满，丢弃", "pluginID", pluginID)
		}
	}
}

// GetPlugins 获取所有已注册的外部插件
func (s *PluginGRPCServer) GetPlugins() map[string]*pluginState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*pluginState)
	for k, v := range s.plugins {
		result[k] = v
	}
	return result
}

// ExecuteAction 在外部插件上执行 Action（通过 gRPC stream 发送请求）
func (s *PluginGRPCServer) ExecuteAction(pluginID, action string, params map[string]any) (any, error) {
	s.mu.RLock()
	ch, exists := s.subscribers[pluginID]
	p, pluginExists := s.plugins[pluginID]
	s.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("plugin not connected: %s", pluginID)
	}
	if !pluginExists {
		return nil, fmt.Errorf("plugin not registered: %s", pluginID)
	}

	// 设置结果通道
	resultCh := make(chan *executeResult, 1)
	s.mu.Lock()
	p.resultCh = resultCh
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		p.resultCh = nil
		s.mu.Unlock()
	}()

	// 序列化参数
	paramsBytes, _ := json.Marshal(params)

	// 通过 stream 发送执行请求
	batch := &pb.EventBatch{
		ExecuteRequests: []*pb.ExecuteRequest{
			{
				Action: action,
				Params: paramsBytes,
			},
		},
	}

	select {
	case ch <- batch:
	default:
		return nil, fmt.Errorf("plugin stream queue full: %s", pluginID)
	}

	// 等待结果（超时 30 秒）
	select {
	case result := <-resultCh:
		if result.Error != "" {
			return nil, fmt.Errorf("%s", result.Error)
		}
		return result.Data, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("execute timeout: %s", action)
	}
}
