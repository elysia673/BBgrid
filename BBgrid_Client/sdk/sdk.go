// Package sdk 提供无感调用 SDK，简化客户端使用。
package sdk

import (
	"BBgrid/BBgrid_Client/api"
	"BBgrid/BBgrid_Client/client"
	"BBgrid/BBgrid_Client/relay"
	"BBgrid/BBgrid_Client/tunnel"
	"BBgrid/common/model"
	"BBgrid/common/proto"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// SDK 无感调用 SDK
type SDK struct {
	mu       sync.RWMutex
	client   client.Client
	api      *api.Client
	tunnels  *tunnel.Manager
	relays   *relay.Manager
	config   Config
	stopCh   chan struct{}

	// 代理信息缓存
	proxyInfo      map[string]*model.CommandData
	proxyInfoMu    sync.RWMutex
	tunnelContexts map[string]context.Context
	tunnelCancel   map[string]context.CancelFunc
}

// Config SDK 配置
type Config struct {
	ServerURL      string        `json:"server_url"`
	ClientID       string        `json:"client_id"`
	ClientToken    string        `json:"client_token"`
	APIKey         string        `json:"api_key"`
	PrivateKeyPath string        `json:"private_key_path"`
	PublicKeyPath  string        `json:"public_key_path"`
	CertPath       string        `json:"cert_path"`
	DataDir        string        `json:"data_dir"`
	UseHTTP        bool          `json:"use_http"`
	Insecure       bool          `json:"insecure"`
	TLSSNI         string        `json:"tls_sni"`
	Origin         string        `json:"origin"`
	ReconnectDelay time.Duration `json:"reconnect_delay"`
}

// DefaultConfig 默认配置
func DefaultConfig() Config {
	return Config{
		ReconnectDelay: 5 * time.Second,
	}
}

// New 创建 SDK
func New(config Config) *SDK {
	return &SDK{
		config:         config,
		tunnels:        tunnel.NewManager(),
		relays:         relay.NewManager(),
		stopCh:         make(chan struct{}),
		proxyInfo:      make(map[string]*model.CommandData),
		tunnelContexts: make(map[string]context.Context),
		tunnelCancel:   make(map[string]context.CancelFunc),
	}
}

// Start 启动 SDK
func (s *SDK) Start(ctx context.Context) error {
	// 创建客户端
	s.client = client.New(client.Config{
		ServerURL:      s.config.ServerURL,
		ClientID:       s.config.ClientID,
		ClientToken:    s.config.ClientToken,
		PrivateKeyPath: s.config.PrivateKeyPath,
		PublicKeyPath:  s.config.PublicKeyPath,
		CertPath:       s.config.CertPath,
		DataDir:        s.config.DataDir,
		UseHTTP:        s.config.UseHTTP,
		Insecure:       s.config.Insecure,
		TLSSNI:         s.config.TLSSNI,
		Origin:         s.config.Origin,
		ReconnectDelay: s.config.ReconnectDelay,
	})

	// 创建 API 客户端
	s.api = api.New(api.Config{
		ServerURL: s.config.ServerURL,
		Token:     s.config.APIKey,
		Insecure:  s.config.Insecure,
	})

	// 注册消息处理器
	s.client.OnMessage(s.handleMessage)

	// 注册事件处理器
	s.client.On(client.EventConnect, func(event client.Event, data any) {
		fmt.Println("[SDK] Connected to server")
	})

	s.client.On(client.EventDisconnect, func(event client.Event, data any) {
		fmt.Println("[SDK] Disconnected from server, cleaning up...")
		s.cleanupAllTunnels()
		s.relays.Stop()
	})

	s.client.On(client.EventError, func(event client.Event, data any) {
		if err, ok := data.(error); ok {
			fmt.Printf("[SDK] Error: %v\n", err)
		}
	})

	// 启动客户端
	go s.client.Run(ctx)

	return nil
}

// Stop 停止 SDK
func (s *SDK) Stop() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}

	s.cleanupAllTunnels()
	s.tunnels.Stop()
	s.relays.Stop()
	s.client.Stop()
}

// cleanupAllTunnels 清理所有隧道
func (s *SDK) cleanupAllTunnels() {
	s.proxyInfoMu.Lock()
	for key, cancel := range s.tunnelCancel {
		cancel()
		delete(s.tunnelCancel, key)
		delete(s.tunnelContexts, key)
		delete(s.proxyInfo, key)
	}
	s.proxyInfoMu.Unlock()
}

// handleMessage 处理来自服务器的消息
func (s *SDK) handleMessage(msg *model.WSMessage) {
	switch msg.Type {
	case "proxy":
		s.handleProxy(msg.Data)
	case "proxy_closed":
		s.handleProxyClosed(msg.Data)
	case "proxy_outbound":
		s.handleProxyOutbound(msg.Data)
	case "tunnel_request":
		s.handleTunnelRequest(msg.Data)
	case "relay_signal":
		s.handleRelaySignal(msg.Data)
	case "relay_closed":
		s.handleRelayClosed(msg.Data)
	case "ping":
		s.handlePing(msg.Data)
	case "update_start":
		s.handleUpdateStart(msg.Data)
	case "update_chunk":
		s.handleUpdateChunk(msg.Data)
	case "update_end":
		s.handleUpdateEnd()
	default:
		fmt.Printf("[SDK] Unknown message type: %s\n", msg.Type)
	}
}

// handleProxy 处理代理消息
func (s *SDK) handleProxy(data any) {
	cmd, err := unmarshalData[model.CommandData](data)
	if err != nil {
		fmt.Printf("[SDK] proxy unmarshal error: %v\n", err)
		return
	}

	if cmd.LocalIP == "" {
		cmd.LocalIP = "127.0.0.1"
	}

	fmt.Printf("[SDK] Proxy command: server=%s remote=%d local=%d:%s\n",
		cmd.ServerHost, cmd.RemotePort, cmd.LocalPort, cmd.LocalIP)

	key := fmt.Sprintf("%s-%d", s.config.ClientID, cmd.RemotePort)

	// 保存代理信息
	s.proxyInfoMu.Lock()
	s.proxyInfo[key] = cmd
	ctx, cancel := context.WithCancel(context.Background())
	s.tunnelContexts[key] = ctx
	s.tunnelCancel[key] = cancel
	s.proxyInfoMu.Unlock()

	fmt.Printf("[SDK] Proxy registered: %s\n", key)
}

// handleProxyClosed 处理代理关闭消息
func (s *SDK) handleProxyClosed(data any) {
	closed, err := unmarshalData[model.ProxyClosedData](data)
	if err != nil {
		fmt.Printf("[SDK] proxy_closed unmarshal error: %v\n", err)
		return
	}

	fmt.Printf("[SDK] Proxy closed: %s\n", closed.Key)

	// 清理隧道
	s.proxyInfoMu.Lock()
	if cancel, ok := s.tunnelCancel[closed.Key]; ok {
		cancel()
		delete(s.tunnelCancel, closed.Key)
		delete(s.tunnelContexts, closed.Key)
		delete(s.proxyInfo, closed.Key)
	}
	s.proxyInfoMu.Unlock()
}

// handleProxyOutbound 处理出站代理消息
func (s *SDK) handleProxyOutbound(data any) {
	cmd, err := unmarshalData[model.ProxyOutboundData](data)
	if err != nil {
		fmt.Printf("[SDK] proxy_outbound unmarshal error: %v\n", err)
		return
	}

	fmt.Printf("[SDK] Proxy outbound: server=%s tunnel=%d local=%d\n",
		cmd.ServerHost, cmd.TunnelPort, cmd.LocalPort)

	key := fmt.Sprintf("outbound-%d", cmd.LocalPort)

	// 保存代理信息
	info := &model.CommandData{
		ServerHost: cmd.ServerHost,
		TunnelPort: cmd.TunnelPort,
		LocalPort:  cmd.LocalPort,
		LocalIP:    "127.0.0.1",
		Token:      cmd.Token,
	}

	s.proxyInfoMu.Lock()
	s.proxyInfo[key] = info
	ctx, cancel := context.WithCancel(context.Background())
	s.tunnelContexts[key] = ctx
	s.tunnelCancel[key] = cancel
	s.proxyInfoMu.Unlock()

	// 立即建立连接
	go s.connectAndPipe(ctx, cmd.ServerHost, cmd.TunnelPort, "127.0.0.1", cmd.LocalPort, cmd.Token)
}

// handleTunnelRequest 处理隧道请求消息
func (s *SDK) handleTunnelRequest(data any) {
	req, err := unmarshalData[model.TunnelRequestData](data)
	if err != nil {
		fmt.Printf("[SDK] tunnel_request unmarshal error: %v\n", err)
		return
	}

	fmt.Printf("[SDK] Tunnel request: key=%s\n", req.Key)

	// 获取代理信息
	s.proxyInfoMu.RLock()
	info := s.proxyInfo[req.Key]
	ctx := s.tunnelContexts[req.Key]
	s.proxyInfoMu.RUnlock()

	if info == nil {
		fmt.Printf("[SDK] No proxy info for key: %s\n", req.Key)
		return
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// 建立连接并转发
	go s.connectAndPipe(ctx, info.ServerHost, info.TunnelPort, info.LocalIP, info.LocalPort, req.Token)
}

// connectAndPipe 连接并转发数据
func (s *SDK) connectAndPipe(ctx context.Context, serverHost string, tunnelPort int, localIP string, localPort int, token string) {
	tunnelAddr := net.JoinHostPort(serverHost, fmt.Sprintf("%d", tunnelPort))
	localAddr := net.JoinHostPort(localIP, fmt.Sprintf("%d", localPort))

	fmt.Printf("[SDK] Connecting tunnel: %s -> %s\n", tunnelAddr, localAddr)

	// 连接到服务器隧道端口
	tunnelConn, err := net.DialTimeout("tcp", tunnelAddr, 10*time.Second)
	if err != nil {
		fmt.Printf("[SDK] Tunnel dial failed: %v\n", err)
		return
	}
	defer tunnelConn.Close()

	// 发送认证
	if err := proto.WriteTunnelAuth(tunnelConn, token); err != nil {
		fmt.Printf("[SDK] Tunnel auth failed: %v\n", err)
		return
	}

	// 等待确认
	ack := make([]byte, 1)
	tunnelConn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(tunnelConn, ack); err != nil || ack[0] != 0x01 {
		fmt.Printf("[SDK] Tunnel ack failed: %v\n", err)
		return
	}
	tunnelConn.SetReadDeadline(time.Time{})

	// 连接本地服务
	localConn, err := net.DialTimeout("tcp", localAddr, 10*time.Second)
	if err != nil {
		fmt.Printf("[SDK] Local dial failed: %v\n", err)
		return
	}
	defer localConn.Close()

	fmt.Printf("[SDK] Tunnel paired: %s <-> %s\n", tunnelAddr, localAddr)

	// context 取消时关闭连接
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tunnelConn.Close()
			localConn.Close()
		case <-done:
		}
	}()

	// 双向转发
	pipeTCP(tunnelConn, localConn)
	close(done)
}

// pipeTCP 双向转发 TCP 连接
func pipeTCP(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(a, b)
		if tc, ok := a.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	go func() {
		defer wg.Done()
		io.Copy(b, a)
		if tc, ok := b.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	wg.Wait()
	a.Close()
	b.Close()
}

// handleRelaySignal 处理中继信号消息
func (s *SDK) handleRelaySignal(data any) {
	sig, err := unmarshalData[model.RelaySignalData](data)
	if err != nil {
		fmt.Printf("[SDK] relay_signal unmarshal error: %v\n", err)
		return
	}

	fmt.Printf("[SDK] Relay signal: session=%s role=%s\n", sig.SessionID, sig.Role)

	config := relay.Config{
		SessionID:     sig.SessionID,
		Role:          relay.RelayRole(sig.Role),
		Protocol:      sig.Protocol,
		ServerHost:    sig.ServerHost,
		SourcePort:    sig.SourcePort,
		TargetPort:    sig.TargetPort,
		SourceLocalIP: sig.SourceLocalIP,
		TargetLocalIP: sig.TargetLocalIP,
		Token:         sig.Token,
		ClientID:      s.config.ClientID,
		UseHTTP:       s.config.UseHTTP,
		Insecure:      s.config.Insecure,
		SNI:           s.config.TLSSNI,
		Origin:        s.config.Origin,
	}

	r, err := s.relays.Create(config)
	if err != nil {
		fmt.Printf("[SDK] Failed to create relay: %v\n", err)
		return
	}

	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		fmt.Printf("[SDK] Failed to start relay: %v\n", err)
		return
	}

	fmt.Printf("[SDK] Relay started: %s (%s)\n", sig.SessionID, sig.Role)
}

// handleRelayClosed 处理中继关闭消息
func (s *SDK) handleRelayClosed(data any) {
	status, err := unmarshalData[model.RelayStatusData](data)
	if err != nil {
		fmt.Printf("[SDK] relay_closed unmarshal error: %v\n", err)
		return
	}

	fmt.Printf("[SDK] Relay closed: %s\n", status.SessionID)
	s.relays.Delete(status.SessionID)
}

// handlePing 处理心跳消息
func (s *SDK) handlePing(data any) {
	// 回复 pong
	s.client.Send(model.WSMessage{
		Type: "pong",
		Data: data,
	})
}

// IsConnected 是否已连接
func (s *SDK) IsConnected() bool {
	return s.client.IsConnected()
}

// GetClient 获取客户端
func (s *SDK) GetClient() client.Client {
	return s.client
}

// GetAPI 获取 API 客户端
func (s *SDK) GetAPI() *api.Client {
	return s.api
}

// CreateTunnel 创建隧道
func (s *SDK) CreateTunnel(config tunnel.Config) (tunnel.Tunnel, error) {
	t, err := s.tunnels.Create(config)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := t.Start(ctx); err != nil {
		return nil, err
	}

	return t, nil
}

// DeleteTunnel 删除隧道
func (s *SDK) DeleteTunnel(key string) error {
	return s.tunnels.Delete(key)
}

// GetTunnel 获取隧道
func (s *SDK) GetTunnel(key string) (tunnel.Tunnel, bool) {
	return s.tunnels.Get(key)
}

// ListTunnels 列出所有隧道
func (s *SDK) ListTunnels() []tunnel.Tunnel {
	return s.tunnels.List()
}

// CreateRelay 创建中继
func (s *SDK) CreateRelay(config relay.Config) (relay.Relay, error) {
	r, err := s.relays.Create(config)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

// DeleteRelay 删除中继
func (s *SDK) DeleteRelay(sessionID string) error {
	return s.relays.Delete(sessionID)
}

// GetRelay 获取中继
func (s *SDK) GetRelay(sessionID string) (relay.Relay, bool) {
	return s.relays.Get(sessionID)
}

// ListRelays 列出所有中继
func (s *SDK) ListRelays() []relay.Relay {
	return s.relays.List()
}

// ProxyCreate 创建代理
func (s *SDK) ProxyCreate(clientID string, remotePort, localPort int, localIP, protocol, bindAddr string) error {
	return s.api.ProxyCreate(clientID, remotePort, localPort, localIP, protocol, bindAddr)
}

// ProxyDelete 删除代理
func (s *SDK) ProxyDelete(port int) error {
	return s.api.ProxyDelete(port)
}

// Proxies 获取所有代理
func (s *SDK) Proxies() ([]map[string]any, error) {
	return s.api.Proxies()
}

// RelayCreate 创建中继
func (s *SDK) RelayCreate(sourceClient, targetClient, protocol string, sourcePort, targetPort int, sourceLocalIP, targetLocalIP string) (string, error) {
	return s.api.RelayCreate(sourceClient, targetClient, protocol, sourcePort, targetPort, sourceLocalIP, targetLocalIP)
}

// RelayDelete 删除中继
func (s *SDK) RelayDelete(sessionID string) error {
	return s.api.RelayDelete(sessionID)
}

// Relays 获取所有中继
func (s *SDK) Relays() ([]map[string]any, error) {
	return s.api.Relays()
}

// Nodes 获取所有节点
func (s *SDK) Nodes() ([]map[string]any, error) {
	return s.api.Nodes()
}

// NodeView 获取节点详情
func (s *SDK) NodeView(clientID string) (map[string]any, error) {
	return s.api.NodeView(clientID)
}

// Status 获取服务器状态
func (s *SDK) Status() (map[string]any, error) {
	return s.api.Status()
}

// Ping 健康检查
func (s *SDK) Ping() error {
	return s.api.Ping()
}

// Call 执行插件操作
func (s *SDK) Call(action string, params map[string]any) (*api.Result, error) {
	return s.api.Call(action, params)
}

// Query 查询状态
func (s *SDK) Query(resourceType, name string) (map[string]any, error) {
	return s.api.Query(resourceType, name)
}

// Capabilities 获取插件能力列表
func (s *SDK) Capabilities() ([]map[string]any, error) {
	return s.api.Capabilities()
}

// Namespaces 获取命名空间列表
func (s *SDK) Namespaces() ([]map[string]any, error) {
	return s.api.Namespaces()
}

// NamespaceInfo 获取命名空间详情
func (s *SDK) NamespaceInfo(name string) (map[string]any, error) {
	return s.api.NamespaceInfo(name)
}

// NamespaceClients 获取命名空间客户端
func (s *SDK) NamespaceClients(name string) ([]map[string]any, error) {
	return s.api.NamespaceClients(name)
}

// NamespaceAssign 分配命名空间
func (s *SDK) NamespaceAssign(clientID, namespace, role string) error {
	return s.api.NamespaceAssign(clientID, namespace, role)
}

// RegisterApply 提交注册申请
func (s *SDK) RegisterApply(clientID, publicKey, token string) error {
	return s.api.RegisterApply(clientID, publicKey, token)
}

// RegisterList 获取已注册客户端列表
func (s *SDK) RegisterList() ([]map[string]any, error) {
	return s.api.RegisterList()
}

// RegisterApprove 审核签发证书
func (s *SDK) RegisterApprove(clientID, namespace, role string) error {
	return s.api.RegisterApprove(clientID, namespace, role)
}

// RegisterRevoke 吊销证书
func (s *SDK) RegisterRevoke(clientID string) error {
	return s.api.RegisterRevoke(clientID)
}

// RegisterPending 获取待审核列表
func (s *SDK) RegisterPending() ([]map[string]any, error) {
	return s.api.RegisterPending()
}

// unmarshalData 反序列化数据
func unmarshalData[T any](data any) (*T, error) {
	switch v := data.(type) {
	case string:
		var result T
		if err := json.Unmarshal([]byte(v), &result); err != nil {
			return nil, fmt.Errorf("unmarshal string data: %w", err)
		}
		return &result, nil
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal data: %w", err)
		}
		var result T
		if err := json.Unmarshal(b, &result); err != nil {
			return nil, fmt.Errorf("unmarshal data: %w", err)
		}
		return &result, nil
	}
}

// getTunnelType 获取隧道类型
func getTunnelType(protocol string) tunnel.TunnelType {
	switch protocol {
	case "udp":
		return tunnel.TunnelTypeUDP
	case "websocket":
		return tunnel.TunnelTypeWS
	default:
		return tunnel.TunnelTypeTCP
	}
}

// SDK 更新相关字段
var (
	updateMu      sync.Mutex
	updateData    []byte
	updateMD5     string
	updateSize    int
	updateChunksN int
)

// handleUpdateStart 处理更新开始消息
func (s *SDK) handleUpdateStart(data any) {
	m, ok := data.(map[string]any)
	if !ok {
		fmt.Println("[SDK] update_start: invalid data")
		return
	}

	updateMu.Lock()
	defer updateMu.Unlock()

	md5Val, _ := m["md5"].(string)
	sizeVal, _ := m["size"].(float64)
	chunksVal, _ := m["chunks"].(float64)

	updateMD5 = md5Val
	updateSize = int(sizeVal)
	updateChunksN = int(chunksVal)
	updateData = make([]byte, 0, updateSize)

	fmt.Printf("[SDK] Update start: md5=%s size=%d chunks=%d\n", updateMD5, updateSize, updateChunksN)
}

// handleUpdateChunk 处理更新数据块消息
func (s *SDK) handleUpdateChunk(data any) {
	m, ok := data.(map[string]any)
	if !ok {
		fmt.Println("[SDK] update_chunk: invalid data")
		return
	}

	dataStr, _ := m["data"].(string)
	chunk, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		fmt.Printf("[SDK] update_chunk: decode error: %v\n", err)
		return
	}

	updateMu.Lock()
	defer updateMu.Unlock()

	if updateSize == 0 {
		fmt.Println("[SDK] update_chunk: update_start not received")
		return
	}

	if len(updateData)+len(chunk) > updateSize {
		fmt.Println("[SDK] update_chunk: data exceeds expected size")
		return
	}

	updateData = append(updateData, chunk...)
}

// handleUpdateEnd 处理更新结束消息
func (s *SDK) handleUpdateEnd() {
	updateMu.Lock()

	if len(updateData) == 0 {
		updateMu.Unlock()
		fmt.Println("[SDK] update_end: no data received")
		return
	}

	data := make([]byte, len(updateData))
	copy(data, updateData)
	updateData = nil
	updateMu.Unlock()

	// 校验 MD5
	hash := md5.Sum(data)
	actualMD5 := hex.EncodeToString(hash[:])
	if updateMD5 != "" && actualMD5 != updateMD5 {
		fmt.Printf("[SDK] Update failed: MD5 mismatch (expected=%s actual=%s)\n", updateMD5, actualMD5)
		return
	}

	fmt.Printf("[SDK] Update data received: md5=%s size=%d\n", actualMD5, len(data))

	// 获取当前可执行文件路径
	execPath, err := os.Executable()
	if err != nil {
		fmt.Printf("[SDK] Update failed: get executable path: %v\n", err)
		return
	}

	// 写入临时文件
	tmpPath := execPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0755); err != nil {
		fmt.Printf("[SDK] Update failed: write temp file: %v\n", err)
		return
	}

	fmt.Println("[SDK] Update successful, restarting...")
	os.Exit(0)
}
