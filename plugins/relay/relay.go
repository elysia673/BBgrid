// Package relay 中继管理插件
//
// 注册 RelayProvider 到 CapabilityRegistry，
// 管理 relay WS 配对和桥接。
package relay

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/proto"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Plugin struct {
	core     *runtime.Core
	provider *DefaultProvider
	stopCh   chan struct{}
	stopOnce sync.Once
}

func New() *Plugin {
	return &Plugin{
		stopCh: make(chan struct{}),
	}
}

func (p *Plugin) Name() string    { return "relay-provider" }
func (p *Plugin) Version() string { return "1.0.0" }

func (p *Plugin) Init(core *runtime.Core, config map[string]any) error {
	p.core = core

	p.provider = NewDefaultProvider(core, nil)

	// 注册到 CapabilityRegistry
	core.Capability().RegisterRelayProvider("default", p.provider)

	// 订阅 relay 事件
	core.EventBus().Subscribe(proto.ResourceTypeRelay, p.handleRelayEvent)

	alog.Info(alog.CatSystem, "relay-provider 插件初始化完成")
	return nil
}

func (p *Plugin) Run() error {
	<-p.stopCh
	return nil
}

func (p *Plugin) Stop() {
	p.stopOnce.Do(func() { close(p.stopCh) })
}

// SetNotifyFn 设置通知回调（由 session handler 注入）
func (p *Plugin) SetNotifyFn(fn func(clientID string, msg any) error) {
	p.provider.notifyFn = fn
}

// GetProvider 获取 Provider 实例
func (p *Plugin) GetProvider() *DefaultProvider {
	return p.provider
}

// handleRelayEvent 处理 relay 事件
func (p *Plugin) handleRelayEvent(event proto.GenericEvent) {
	switch event.EventType {
	case proto.EventAdded:
		p.handleRelayAdded(event)
	case proto.EventDeleted:
		p.handleRelayDeleted(event)
	}
}

// handleRelayAdded 处理 relay 创建事件：通知客户端
func (p *Plugin) handleRelayAdded(event proto.GenericEvent) {
	session, ok := event.Payload.(runtime.RelaySession)
	if !ok {
		return
	}

	// 通知客户端建立中继（恢复期间跳过）
	if !p.core.IsRestoring() {
		p.notifyRelayClients(session, "relay_signal")
	}

	alog.Info(alog.CatSystem, "relay-provider: 已通知客户端建立中继",
		"session_id", session.ID,
		"source", session.SourceClient,
		"target", session.TargetClient,
	)
}

// handleRelayDeleted 处理 relay 删除事件：通知客户端关闭
func (p *Plugin) handleRelayDeleted(event proto.GenericEvent) {
	sessionID := event.Resource.Name
	// 从 StateStore 获取 relay 信息（用于通知客户端）
	sess, ok := p.core.StateStore().GetRelaySession(sessionID)
	if !ok {
		return
	}
	p.notifyRelayClients(sess, "relay_closed")
	alog.Info(alog.CatSystem, "relay-provider: 已通知客户端关闭中继", "session_id", sessionID)
}

// notifyRelayClients 通知源/目标客户端
func (p *Plugin) notifyRelayClients(session runtime.RelaySession, msgType string) {
	sourceMsg := model.WSMessage{
		Type: msgType,
		Data: map[string]any{
			"session_id":      session.ID,
			"protocol":        session.Protocol,
			"role":            "source",
			"peer_client_id":  session.TargetClient,
			"source_port":     session.SourcePort,
			"target_port":     session.TargetPort,
			"target_local_ip": session.TargetLocalIP,
			"source_local_ip": session.SourceLocalIP,
			"server_host":     p.core.StateStore().GetPublicIP(),
			"token":           session.Token,
		},
	}
	if p.provider.notifyFn != nil {
		if err := p.provider.notifyFn(session.SourceClient, sourceMsg); err != nil {
			alog.Warn(alog.CatSystem, "relay-provider: 通知源客户端失败", "session_id", session.ID, "error", err)
		}
	}

	targetMsg := model.WSMessage{
		Type: msgType,
		Data: map[string]any{
			"session_id":      session.ID,
			"protocol":        session.Protocol,
			"role":            "target",
			"peer_client_id":  session.SourceClient,
			"source_port":     session.SourcePort,
			"target_port":     session.TargetPort,
			"target_local_ip": session.TargetLocalIP,
			"source_local_ip": session.SourceLocalIP,
			"server_host":     p.core.StateStore().GetPublicIP(),
			"token":           session.Token,
		},
	}
	if p.provider.notifyFn != nil {
		if err := p.provider.notifyFn(session.TargetClient, targetMsg); err != nil {
			alog.Warn(alog.CatSystem, "relay-provider: 通知目标客户端失败", "session_id", session.ID, "error", err)
		}
	}
}

// DefaultProvider 默认中继提供者
type DefaultProvider struct {
	core     *runtime.Core
	notifyFn func(clientID string, msg any) error

	pendingMu    sync.Mutex
	pendingConns map[string]chan *websocket.Conn
}

func NewDefaultProvider(core *runtime.Core, notifyFn func(clientID string, msg any) error) *DefaultProvider {
	return &DefaultProvider{
		core:         core,
		notifyFn:     notifyFn,
		pendingConns: make(map[string]chan *websocket.Conn),
	}
}

func (p *DefaultProvider) Name() string { return "default" }

func (p *DefaultProvider) Create(req runtime.RelayCreateRequest) (string, string, error) {
	sessionID := proto.GenerateID()
	token := proto.GenerateID()

	p.core.Publish(proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      sessionID,
		},
		proto.EventAdded,
		runtime.RelaySession{
			ID:            sessionID,
			SourceClient:  req.SourceClient,
			TargetClient:  req.TargetClient,
			Protocol:      req.Protocol,
			SourcePort:    req.SourcePort,
			TargetPort:    req.TargetPort,
			TargetLocalIP: req.TargetLocalIP,
			SourceLocalIP: req.SourceLocalIP,
			Token:         token,
		},
	))

	return sessionID, token, nil
}

func (p *DefaultProvider) Delete(sessionID string) error {
	if _, exists := p.core.StateStore().GetRelaySession(sessionID); !exists {
		return fmt.Errorf("relay session not found: %s", sessionID)
	}

	p.core.Publish(proto.NewGenericEvent(
		proto.ResourceKey{
			Type:      proto.ResourceTypeRelay,
			Namespace: proto.NamespaceDefault,
			Name:      sessionID,
		},
		proto.EventDeleted,
		nil,
	))
	return nil
}

func (p *DefaultProvider) List() []runtime.RelaySession {
	return p.core.StateStore().ListRelaySessions()
}

// AcceptRelay 接受 relay WS 连接并配对
func (p *DefaultProvider) AcceptRelay(sessionID string, role string, conn *websocket.Conn) {
	p.pendingMu.Lock()
	ch, exists := p.pendingConns[sessionID]
	if !exists {
		ch = make(chan *websocket.Conn, 1)
		p.pendingConns[sessionID] = ch
		p.pendingMu.Unlock()

		select {
		case peer := <-ch:
			if peer == nil {
				conn.Close()
				return
			}
			alog.Info(alog.CatWS, "Relay 配对成功 开始转发", "session", sessionID)
			relayWSPipe(conn, peer)
			alog.Info(alog.CatWS, "Relay 转发结束", "session", sessionID)
		case <-time.After(60 * time.Second):
			p.pendingMu.Lock()
			delete(p.pendingConns, sessionID)
			p.pendingMu.Unlock()
			conn.Close()
		}
		return
	}
	delete(p.pendingConns, sessionID)
	p.pendingMu.Unlock()

	ch <- conn
}

// relayWSPipe 双向转发 WebSocket 消息
func relayWSPipe(a, b *websocket.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			msgType, data, err := a.ReadMessage()
			if err != nil {
				return
			}
			if err := b.WriteMessage(msgType, data); err != nil {
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			msgType, data, err := b.ReadMessage()
			if err != nil {
				return
			}
			if err := a.WriteMessage(msgType, data); err != nil {
				return
			}
		}
	}()
	wg.Wait()
	a.Close()
	b.Close()
}

// AcceptTunnel 接受隧道连接（proxy 隧道配对，由 session handler 处理）
func (p *DefaultProvider) AcceptTunnel(token string, conn net.Conn) {
	// 由 session handler 的 pendingMap 处理
}
