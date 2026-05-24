// Package sdk 提供外部插件开发 SDK
//
// 外部插件通过此 SDK 连接 Server，订阅/发布事件，注册能力。
//
// 使用方式：
//
//	plugin := sdk.NewPlugin("my-plugin", "1.0.0", serverAddr)
//
//	// 注册资源 handler
//	plugin.On("proxy", func(event sdk.Event) {
//	    // 处理 proxy 事件
//	})
//
//	// 注册 Action handler
//	plugin.HandleAction("my.action", func(params map[string]any) (any, error) {
//	    return result, nil
//	})
//
//	// 连接 Server 并运行
//	plugin.Run()
package sdk

import (
	"BBgrid/common/plugin/proto/pb"
	"BBgrid/common/proto"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Event 外部插件收到的事件
type Event struct {
	Resource  proto.ResourceKey
	EventType proto.EventType
	Payload   map[string]any
	Sequence  int64
	Timestamp int64
}

// ActionHandler Action 处函数
type ActionHandler func(params map[string]any) (any, error)

// ActionDef Action 定义
type ActionDef struct {
	Name        string
	Description string
	Handler     ActionHandler
}

// Plugin 外部插件客户端
type Plugin struct {
	id      string
	version string
	server  string

	conn   *grpc.ClientConn
	client pb.PluginServiceClient
	stream pb.PluginService_SubscribeClient

	mu           sync.RWMutex
	handlers     map[string][]func(Event) // resourceType -> handlers
	actions      map[string]ActionDef     // actionName -> def
	capabilities []*pb.Capability
	stopCh       chan struct{}
}

// NewPlugin 创建外部插件
func NewPlugin(id, version, serverAddr string) *Plugin {
	return &Plugin{
		id:       id,
		version:  version,
		server:   serverAddr,
		handlers: make(map[string][]func(Event)),
		actions:  make(map[string]ActionDef),
		stopCh:   make(chan struct{}),
	}
}

// HandleAction 注册 Action handler
func (p *Plugin) HandleAction(name, description string, handler ActionHandler) {
	p.mu.Lock()
	p.actions[name] = ActionDef{
		Name:        name,
		Description: description,
		Handler:     handler,
	}
	p.mu.Unlock()
}

// On 注册资源事件 handler
func (p *Plugin) On(resourceType string, handler func(Event)) {
	p.mu.Lock()
	p.handlers[resourceType] = append(p.handlers[resourceType], handler)
	p.mu.Unlock()
}

// AddCapability 声明资源能力
func (p *Plugin) AddCapability(resourceType string, eventTypes ...string) {
	p.capabilities = append(p.capabilities, &pb.Capability{
		ResourceType: resourceType,
		EventTypes:   eventTypes,
	})
}

// Run 连接 Server 并运行
func (p *Plugin) Run() error {
	// 连接 Server
	conn, err := grpc.NewClient(p.server,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("连接 server 失败: %w", err)
	}
	p.conn = conn
	p.client = pb.NewPluginServiceClient(conn)

	// 注册能力
	if err := p.register(); err != nil {
		return fmt.Errorf("注册失败: %w", err)
	}

	// 订阅事件
	if err := p.subscribe(); err != nil {
		return fmt.Errorf("订阅失败: %w", err)
	}

	log.Printf("[%s] 已连接 server %s", p.id, p.server)

	// 主循环：接收事件
	go p.receiveLoop()

	// 心跳保活
	go p.heartbeatLoop()

	<-p.stopCh
	return nil
}

// Stop 停止插件
func (p *Plugin) Stop() {
	close(p.stopCh)
	if p.conn != nil {
		p.conn.Close()
	}
}

// Publish 发布事件到 Server
func (p *Plugin) Publish(event Event) error {
	payload, _ := json.Marshal(event.Payload)

	pbEvent := &pb.PluginEvent{
		Resource: &pb.ResourceKey{
			Type:      event.Resource.Type,
			Namespace: event.Resource.Namespace,
			Name:      event.Resource.Name,
		},
		EventType: string(event.EventType),
		Payload:   payload,
	}

	_, err := p.client.Publish(context.Background(), &pb.PublishRequest{
		PluginId: p.id,
		Events:   []*pb.PluginEvent{pbEvent},
	})
	return err
}

// ==================== 内部方法 ====================

func (p *Plugin) register() error {
	actions := make([]*pb.ActionDeclaration, 0, len(p.actions))
	for _, def := range p.actions {
		actions = append(actions, &pb.ActionDeclaration{
			Name:        def.Name,
			Description: def.Description,
		})
	}

	_, err := p.client.Register(context.Background(), &pb.RegisterRequest{
		PluginId:     p.id,
		Version:      p.version,
		Capabilities: p.capabilities,
		Actions:      actions,
	})
	return err
}

func (p *Plugin) subscribe() error {
	var resourceTypes []string
	for rt := range p.handlers {
		resourceTypes = append(resourceTypes, rt)
	}

	stream, err := p.client.Subscribe(context.Background(), &pb.SubscribeRequest{
		PluginId:      p.id,
		ResourceTypes: resourceTypes,
	})
	if err != nil {
		return err
	}
	p.stream = stream
	return nil
}

func (p *Plugin) receiveLoop() {
	for {
		select {
		case <-p.stopCh:
			return
		default:
		}

		batch, err := p.stream.Recv()
		if err != nil {
			log.Printf("[%s] stream 断开: %v", p.id, err)
			return
		}

		for _, pbEvent := range batch.Events {
			event := pbToEvent(pbEvent)
			p.dispatchEvent(event)
		}

		// 处理执行请求
		for _, execReq := range batch.ExecuteRequests {
			go p.handleExecuteRequest(execReq)
		}
	}
}

func (p *Plugin) handleExecuteRequest(req *pb.ExecuteRequest) {
	p.mu.RLock()
	def, exists := p.actions[req.Action]
	p.mu.RUnlock()

	if !exists {
		log.Printf("[%s] action not found: %s", p.id, req.Action)
		return
	}

	var params map[string]any
	if len(req.Params) > 0 {
		json.Unmarshal(req.Params, &params)
	}

	result, err := def.Handler(params)
	if err != nil {
		log.Printf("[%s] action %s failed: %v", p.id, req.Action, err)
		return
	}

	// 发送结果
	resultBytes, _ := json.Marshal(result)
	p.client.Publish(context.Background(), &pb.PublishRequest{
		PluginId: p.id,
		Events: []*pb.PluginEvent{
			{
				Resource: &pb.ResourceKey{
					Type: "action_result",
					Name: req.Action,
				},
				EventType: "RESULT",
				Payload:   resultBytes,
			},
		},
	})
}

func (p *Plugin) dispatchEvent(event Event) {
	p.mu.RLock()
	handlers := p.handlers[event.Resource.Type]
	allHandlers := p.handlers["*"] // 通配符订阅
	p.mu.RUnlock()

	for _, h := range handlers {
		go h(event)
	}
	for _, h := range allHandlers {
		go h(event)
	}
}

func (p *Plugin) heartbeatLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.client.Heartbeat(context.Background(), &pb.HeartbeatRequest{
				PluginId:  p.id,
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}
}

func pbToEvent(pbEvent *pb.PluginEvent) Event {
	var payload map[string]any
	if len(pbEvent.Payload) > 0 {
		json.Unmarshal(pbEvent.Payload, &payload)
	}

	return Event{
		Resource: proto.ResourceKey{
			Type:      pbEvent.Resource.Type,
			Namespace: pbEvent.Resource.Namespace,
			Name:      pbEvent.Resource.Name,
		},
		EventType: proto.EventType(pbEvent.EventType),
		Payload:   payload,
		Sequence:  pbEvent.Sequence,
		Timestamp: pbEvent.Timestamp,
	}
}
