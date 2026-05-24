package workers

import (
	alog "BBgrid/common/log"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// DataConfig 数据面配置
type DataConfig struct {
	TunnelPort int
}

// DataWorker 数据面 Worker
//
// 只负责转发，零锁、零分配：
// - TCP 代理监听
// - UDP 代理监听
// - 隧道配对
// - 字节流 Pipe
type DataWorker struct {
	config DataConfig

	// 隧道配对 (唯一需要锁的地方)
	pendingMap map[string]*pendingEntry
	pendingMu  sync.Mutex

	// 活跃的 listener 管理
	listeners map[string]net.Listener
	lnMu      sync.Mutex

	// 生命周期
	stopCh chan struct{}

	// 引用 StateStore 获取客户端连接
	state StateStore
}

// NewDataWorker 创建数据面 Worker
func NewDataWorker(config DataConfig, state StateStore) *DataWorker {
	return &DataWorker{
		config:     config,
		pendingMap: make(map[string]*pendingEntry),
		listeners:  make(map[string]net.Listener),
		stopCh:     make(chan struct{}),
		state:      state,
	}
}

// Name 返回 Worker 名称
func (w *DataWorker) Name() string {
	return "data"
}

// Run 启动数据面 Worker
func (w *DataWorker) Run() error {
	alog.Info(alog.CatSystem, "DataWorker 启动")

	// 启动 pending 清理协程
	go w.cleanupPendingLoop()

	// 等待停止信号
	<-w.stopCh
	alog.Info(alog.CatSystem, "DataWorker 停止")
	return nil
}

// Stop 停止数据面 Worker
func (w *DataWorker) Stop() {
	close(w.stopCh)
}

// ==================== TCP 代理 ====================

// StartTCPProxy 启动 TCP 代理监听
func (w *DataWorker) StartTCPProxy(port int, bindAddr string, clientID string, token string) {
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		alog.Error(alog.CatProxy, "listen failed", "error", err, "port", port)
		return
	}

	key := fmt.Sprintf("%s-%d", clientID, port)

	// 注册 listener
	w.lnMu.Lock()
	w.listeners[key] = ln
	w.lnMu.Unlock()

	defer func() {
		ln.Close()
		w.lnMu.Lock()
		delete(w.listeners, key)
		w.lnMu.Unlock()
	}()

	alog.Info(alog.CatProxy, "TCP proxy listening", "port", port, "clientID", clientID)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		go w.handlePublicConn(conn, key, clientID)
	}
}

// CloseListener 关闭指定 key 的 listener
func (w *DataWorker) CloseListener(key string) {
	w.lnMu.Lock()
	ln, ok := w.listeners[key]
	w.lnMu.Unlock()

	if ok && ln != nil {
		ln.Close()
		alog.Info(alog.CatProxy, "closed stale listener", "key", key)
	}
}

// handlePublicConn 处理公网连接
func (w *DataWorker) handlePublicConn(conn net.Conn, key string, clientID string) {
	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatProxy, "public connection, requesting tunnel", "remote", remoteAddr, "key", key)

	tunnelToken := generateRandomToken(16)
	pendingCh := w.registerPending(tunnelToken)
	defer w.removePending(tunnelToken)
	defer conn.Close()

	// 通过 StateStore 发送隧道请求
	reqData := fmt.Sprintf(`{"key":"%s","token":"%s"}`, key, tunnelToken)
	notify := map[string]any{
		"type": "tunnel_request",
		"data": reqData,
	}
	if err := w.state.SendCommand(clientID, notify); err != nil {
		alog.Error(alog.CatProxy, "send tunnel_request failed", "remote", remoteAddr, "error", err)
		return
	}

	select {
	case tunnelConn := <-pendingCh:
		if _, err := tunnelConn.Write([]byte{0x01}); err != nil {
			alog.Error(alog.CatProxy, "send tunnel ack failed", "error", err)
			tunnelConn.Close()
			return
		}
		alog.Info(alog.CatProxy, "tunnel paired", "remote", remoteAddr, "key", key)
		pipeBidir(conn, tunnelConn)
	case <-time.After(60 * time.Second):
		alog.Warn(alog.CatProxy, "tunnel pairing timeout", "remote", remoteAddr, "key", key)
	}
}

// AcceptTunnel 接受客户端隧道连接并配对公网连接
func (w *DataWorker) AcceptTunnel(tunnelConn net.Conn, token string) {
	ch := w.takePending(token)
	if ch == nil {
		alog.Warn(alog.CatProxy, "no pending public connection for tunnel token")
		tunnelConn.Close()
		return
	}

	select {
	case ch <- tunnelConn:
	case <-time.After(5 * time.Second):
		alog.Warn(alog.CatProxy, "public connection already gone, dropping tunnel")
		tunnelConn.Close()
	}
}

// ==================== Pending 管理 ====================

// registerPending 注册待配对的隧道连接
func (w *DataWorker) registerPending(token string) chan net.Conn {
	ch := make(chan net.Conn, 1)
	w.pendingMu.Lock()
	w.pendingMap[token] = &pendingEntry{ch: ch, createdAt: time.Now()}
	w.pendingMu.Unlock()
	return ch
}

// takePending 获取待配对的隧道连接
func (w *DataWorker) takePending(token string) chan net.Conn {
	w.pendingMu.Lock()
	entry := w.pendingMap[token]
	if entry != nil {
		delete(w.pendingMap, token)
	}
	w.pendingMu.Unlock()
	if entry != nil {
		return entry.ch
	}
	return nil
}

// removePending 移除待配对的隧道连接
func (w *DataWorker) removePending(token string) {
	w.pendingMu.Lock()
	delete(w.pendingMap, token)
	w.pendingMu.Unlock()
}

// cleanupPendingLoop 定期清理过期的 pending 连接
func (w *DataWorker) cleanupPendingLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.pendingMu.Lock()
			now := time.Now()
			for token, entry := range w.pendingMap {
				if now.Sub(entry.createdAt) > 60*time.Second {
					delete(w.pendingMap, token)
				}
			}
			w.pendingMu.Unlock()
		}
	}
}

// ==================== UDP/KCP 代理 ====================

// UDPConfig UDP 隧道配置
type UDPConfig struct {
	NoDelay  int
	Interval int
	Resend   int
	NC       int
	Crypt    string
	Key      string
}

// DefaultUDPConfig 默认 UDP 配置
func DefaultUDPConfig(key string) *UDPConfig {
	return &UDPConfig{
		NoDelay:  1,
		Interval: 10,
		Resend:   2,
		NC:       1,
		Crypt:    "aes",
		Key:      key,
	}
}

// StartKCPProxy 启动 KCP 代理监听
func (w *DataWorker) StartKCPProxy(port int, clientID string, config *UDPConfig) {
	if config == nil {
		config = DefaultUDPConfig("")
	}

	// 创建加密块
	var block kcp.BlockCrypt
	if config.Crypt != "" && config.Key != "" {
		var err error
		block, err = kcp.NewAESBlockCrypt([]byte(config.Key))
		if err != nil {
			alog.Error(alog.CatProxy, "创建 KCP 加密块失败", "error", err)
			return
		}
	}

	// 创建 KCP 监听器
	listener, err := kcp.ListenWithOptions(fmt.Sprintf(":%d", port), block, 0, 0)
	if err != nil {
		alog.Error(alog.CatProxy, "KCP 监听失败", "error", err, "port", port)
		return
	}
	defer listener.Close()

	alog.Info(alog.CatProxy, "KCP proxy listening", "port", port, "clientID", clientID)

	for {
		conn, err := listener.AcceptKCP()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			alog.Error(alog.CatProxy, "KCP 接受连接失败", "error", err)
			continue
		}

		// 设置 KCP 参数
		conn.SetNoDelay(config.NoDelay, config.Interval, config.Resend, config.NC)

		go w.handleKCPTunnelConn(conn)
	}
}

// handleKCPTunnelConn 处理 KCP 隧道连接
func (w *DataWorker) handleKCPTunnelConn(conn *kcp.UDPSession) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatTunnel, "handleKCPTunnelConn panic", "error", r)
		}
	}()

	remoteAddr := conn.RemoteAddr().String()
	alog.Info(alog.CatTunnel, "新的 KCP 隧道连接", "remote", remoteAddr)

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// 读取认证数据 [4B magic "TUNL"][2B tokenLen][token]
	header := make([]byte, 6)
	if _, err := io.ReadFull(conn, header); err != nil {
		alog.Error(alog.CatAuth, "KCP 隧道认证读取失败", "error", err)
		conn.Close()
		return
	}

	// 验证魔数
	if string(header[:4]) != "TUNL" {
		alog.Error(alog.CatAuth, "KCP 隧道魔数错误", "magic", string(header[:4]))
		conn.Close()
		return
	}

	// 读取 token
	tokenLen := binary.BigEndian.Uint16(header[4:6])
	tokenBuf := make([]byte, tokenLen)
	if _, err := io.ReadFull(conn, tokenBuf); err != nil {
		alog.Error(alog.CatAuth, "KCP 隧道 token 读取失败", "error", err)
		conn.Close()
		return
	}

	conn.SetReadDeadline(time.Time{})

	// 接受隧道连接
	w.AcceptTunnel(conn, string(tokenBuf))
}

// StartUDPProxy 启动 UDP 代理监听
func (w *DataWorker) StartUDPProxy(port int, bindAddr string, clientID string) {
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}

	udpAddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(bindAddr, fmt.Sprintf("%d", port)))
	if err != nil {
		alog.Error(alog.CatProxy, "UDP 地址解析失败", "error", err)
		return
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		alog.Error(alog.CatProxy, "UDP 监听失败", "error", err)
		return
	}
	defer conn.Close()

	alog.Info(alog.CatProxy, "UDP proxy listening", "port", port, "clientID", clientID)

	// UDP 会话管理
	var (
		sessions      = make(map[string]*udpSession)
		sessionByPort = make(map[int]*udpSession)
		clientAddr    *net.UDPAddr
		mu            sync.RWMutex
	)

	// 清理过期会话
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			mu.Lock()
			now := time.Now()
			for addr, sess := range sessions {
				if now.Sub(sess.lastActive) > 60*time.Second {
					delete(sessions, addr)
				}
			}
			mu.Unlock()
		}
	}()

	buf := make([]byte, 65535)
	for {
		n, srcAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		if n == 0 {
			continue
		}

		// 隧道包：[0xAE][publicPort][data]
		if n >= 3 && buf[0] == 0xAE {
			publicPort := int(buf[1])<<8 | int(buf[2])

			// 注册包：publicPort == 0
			if publicPort == 0 {
				mu.Lock()
				clientAddr = srcAddr
				mu.Unlock()
				alog.Info(alog.CatProxy, "UDP 隧道客户端已注册", "addr", srcAddr)
				continue
			}

			// 回包：转发给公网用户
			data := buf[3:n]
			mu.RLock()
			sess, ok := sessionByPort[publicPort]
			if ok {
				conn.WriteToUDP(data, sess.publicAddr)
			}
			mu.RUnlock()
			continue
		}

		// 公网用户包
		mu.Lock()
		sess := &udpSession{
			publicAddr: srcAddr,
			lastActive: time.Now(),
		}
		sessions[srcAddr.String()] = sess
		sessionByPort[srcAddr.Port] = sess
		ca := clientAddr
		mu.Unlock()

		if ca == nil {
			continue
		}

		// 封装转发：[0xAE][publicPort][data]
		packet := make([]byte, 3+n)
		packet[0] = 0xAE
		packet[1] = byte(srcAddr.Port >> 8)
		packet[2] = byte(srcAddr.Port)
		copy(packet[3:], buf[:n])
		conn.WriteToUDP(packet, ca)
	}
}

// udpSession UDP 会话
type udpSession struct {
	publicAddr *net.UDPAddr
	lastActive time.Time
}

// ==================== 辅助函数 ====================

// pendingEntry 待配对的隧道连接
type pendingEntry struct {
	ch        chan net.Conn
	createdAt time.Time
}

// pipeBidir 双向转发 (零拷贝)
func pipeBidir(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer a.Close()
		io.Copy(a, b)
	}()

	go func() {
		defer wg.Done()
		defer b.Close()
		io.Copy(b, a)
	}()

	wg.Wait()
}
