// Package proxy 代理管理插件
//
// 注册 TCPProxyProvider 到 CapabilityRegistry，
// 通过 EventBus 监听 proxy 事件管理 TCP listener 生命周期。
package proxy

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"BBgrid/common/proto"
	"fmt"
	"net"
	"sync"
)

type Plugin struct {
	core      *runtime.Core
	provider  *TCPProvider
	stopCh    chan struct{}
	stopOnce  sync.Once
	tunnelPort int
}

func New() *Plugin {
	return &Plugin{
		stopCh: make(chan struct{}),
	}
}

func (p *Plugin) Name() string    { return "proxy-provider" }
func (p *Plugin) Version() string { return "1.0.0" }

func (p *Plugin) Init(core *runtime.Core, config map[string]any) error {
	p.core = core

	// 从 config 读取 tunnelPort（可选）
	if v, ok := config["tunnel_port"].(float64); ok {
		p.tunnelPort = int(v)
	}

	// 创建 Provider（notifyFn 由 session handler 设置）
	p.provider = NewTCPProvider(core, nil)

	// 注册到 CapabilityRegistry
	core.Capability().RegisterProxyProvider("tcp", p.provider)

	// 订阅 proxy 事件，管理 listener 生命周期
	core.EventBus().Subscribe(proto.ResourceTypeProxy, p.handleProxyEvent)

	alog.Info(alog.CatSystem, "proxy-provider 插件初始化完成")
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
func (p *Plugin) GetProvider() *TCPProvider {
	return p.provider
}

// handleProxyEvent 处理 proxy 事件
func (p *Plugin) handleProxyEvent(event proto.GenericEvent) {
	switch event.EventType {
	case proto.EventAdded:
		p.handleProxyAdded(event)
	case proto.EventDeleted:
		p.handleProxyDeleted(event)
	}
}

// handleProxyAdded 处理 proxy 创建事件：启动 TCP listener
func (p *Plugin) handleProxyAdded(event proto.GenericEvent) {
	proxy, ok := event.Payload.(runtime.ProxyState)
	if !ok {
		if m, ok := event.Payload.(map[string]any); ok {
			proxy = parseProxyFromMap(m)
		} else {
			return
		}
	}

	if proxy.ClientID == "" || proxy.RemotePort == 0 {
		return
	}

	req := runtime.ProxyCreateRequest{
		ClientID:   proxy.ClientID,
		RemotePort: proxy.RemotePort,
		LocalPort:  proxy.LocalPort,
		LocalIP:    proxy.LocalIP,
		Protocol:   proxy.Protocol,
		BindAddr:   proxy.BindAddr,
	}

	if err := p.provider.Create(req); err != nil {
		alog.Error(alog.CatSystem, "proxy-provider: 启动 listener 失败",
			"client_id", proxy.ClientID, "port", proxy.RemotePort, "error", err)
		return
	}

	alog.Info(alog.CatSystem, "proxy-provider: listener 已启动",
		"client_id", proxy.ClientID, "port", proxy.RemotePort)
}

// handleProxyDeleted 处理 proxy 删除事件：关闭 TCP listener
func (p *Plugin) handleProxyDeleted(event proto.GenericEvent) {
	key := event.Resource.Name
	clientID, port, ok := parseProxyResourceKey(key)
	if !ok {
		return
	}

	if err := p.provider.Delete(clientID, port); err != nil {
		alog.Error(alog.CatSystem, "proxy-provider: 关闭 listener 失败",
			"client_id", clientID, "port", port, "error", err)
	}
}

// parseProxyFromMap 从 map 解析 ProxyState
func parseProxyFromMap(m map[string]any) runtime.ProxyState {
	var proxy runtime.ProxyState
	if v, ok := m["client_id"].(string); ok {
		proxy.ClientID = v
	}
	if v, ok := m["remote_port"].(float64); ok {
		proxy.RemotePort = int(v)
	}
	if v, ok := m["local_port"].(float64); ok {
		proxy.LocalPort = int(v)
	}
	if v, ok := m["local_ip"].(string); ok {
		proxy.LocalIP = v
	}
	if v, ok := m["protocol"].(string); ok {
		proxy.Protocol = v
	}
	if v, ok := m["bind_addr"].(string); ok {
		proxy.BindAddr = v
	}
	return proxy
}

// parseProxyResourceKey 解析 "clientID:port" 格式的 key
func parseProxyResourceKey(key string) (string, int, bool) {
	clientID, portText, ok := cutString(key, ":")
	if !ok || clientID == "" || portText == "" {
		return "", 0, false
	}
	port := 0
	fmt.Sscanf(portText, "%d", &port)
	if port == 0 {
		return "", 0, false
	}
	return clientID, port, true
}

func cutString(s, sep string) (before, after string, found bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == sep[0] {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// TCPProvider 代理生命周期管理
type TCPProvider struct {
	core      *runtime.Core
	notifyFn  func(clientID string, msg any) error
	listeners map[string]net.Listener
	lnMu      sync.Mutex
}

func NewTCPProvider(core *runtime.Core, notifyFn func(clientID string, msg any) error) *TCPProvider {
	return &TCPProvider{
		core:      core,
		notifyFn:  notifyFn,
		listeners: make(map[string]net.Listener),
	}
}

func (p *TCPProvider) Name() string { return "tcp" }

func (p *TCPProvider) Create(req runtime.ProxyCreateRequest) error {
	bindAddr := req.BindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	addr := fmt.Sprintf("%s:%d", bindAddr, req.RemotePort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	key := fmt.Sprintf("%s:%d", req.ClientID, req.RemotePort)
	p.lnMu.Lock()
	p.listeners[key] = ln
	p.lnMu.Unlock()

	alog.Info(alog.CatSystem, "TCP listener 启动", "addr", addr, "client_id", req.ClientID)
	return nil
}

func (p *TCPProvider) Delete(clientID string, port int) error {
	key := fmt.Sprintf("%s:%d", clientID, port)
	p.lnMu.Lock()
	ln, ok := p.listeners[key]
	if ok {
		delete(p.listeners, key)
	}
	p.lnMu.Unlock()

	if ok && ln != nil {
		ln.Close()
		alog.Info(alog.CatSystem, "TCP listener 关闭", "client_id", clientID, "port", port)
	}
	return nil
}

func (p *TCPProvider) List() []runtime.ProxyInfo {
	return p.core.StateStore().ListProxies()
}

func (p *TCPProvider) GetListener(clientID string, port int) (net.Listener, bool) {
	key := fmt.Sprintf("%s:%d", clientID, port)
	p.lnMu.Lock()
	defer p.lnMu.Unlock()
	ln, ok := p.listeners[key]
	return ln, ok
}
