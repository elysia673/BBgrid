package handler

import (
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/mux"
	"BBgrid/common/wsconn"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// relaySession 表示一个中继会话。
type relaySession struct {
	id            string
	protocol      string
	role          string
	targetPort    int
	targetLocalIP string
	sourceLocalIP string
	sourcePort    int
	token         string
	serverHost    string
	cancel        context.CancelFunc
	mx            *mux.Multiplexer
	ln            net.Listener
	closeOnce     sync.Once
}

// relayManager 管理客户端中继会话。
type relayManager struct {
	mu       sync.Mutex
	sessions map[string]*relaySession
	sender   MessageSender
	cfg      Config
	baseCtx  context.Context
}

// newRelayManager 创建新的中继管理器。
func newRelayManager(cfg Config, baseCtx context.Context) *relayManager {
	return &relayManager{
		sessions: make(map[string]*relaySession),
		cfg:      cfg,
		baseCtx:  baseCtx,
	}
}

// SetSender 设置消息发送器。
func (rm *relayManager) SetSender(sender MessageSender) {
	rm.sender = sender
}

// handleRelaySignal 处理中继信令消息。
func (rm *relayManager) handleRelaySignal(data interface{}) {
	sig, err := unmarshalData[model.RelaySignalData](data)
	if err != nil {
		alog.Error(alog.CatRelay, "中继信令解析错误", "error", err)
		return
	}

	alog.Info(alog.CatRelay, "中继信令",
		"session", sig.SessionID, "role", sig.Role, "protocol", sig.Protocol,
		"sourcePort", sig.SourcePort, "targetPort", sig.TargetPort, "server", sig.ServerHost)

	ctx, cancel := context.WithCancel(rm.baseCtx)

	sess := &relaySession{
		id:            sig.SessionID,
		protocol:      sig.Protocol,
		role:          sig.Role,
		targetPort:    sig.TargetPort,
		targetLocalIP: sig.TargetLocalIP,
		sourceLocalIP: sig.SourceLocalIP,
		sourcePort:    sig.SourcePort,
		token:         sig.Token,
		serverHost:    sig.ServerHost,
		cancel:        cancel,
	}

	rm.mu.Lock()
	rm.sessions[sig.SessionID] = sess
	rm.mu.Unlock()

	if sig.Role == "source" {
		go rm.runSource(ctx, sess)
	} else {
		go rm.runTarget(ctx, sess)
	}
}

// handleRelayClosed 处理中继会话关闭消息。
func (rm *relayManager) handleRelayClosed(data interface{}) {
	status, err := unmarshalData[model.RelayStatusData](data)
	if err != nil {
		return
	}

	rm.mu.Lock()
	sess, ok := rm.sessions[status.SessionID]
	if ok {
		delete(rm.sessions, status.SessionID)
	}
	rm.mu.Unlock()

	if ok {
		sess.closeOnce.Do(func() {
			sess.cancel()
		})
	}
}

// stop 停止所有中继会话。
func (rm *relayManager) stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for _, s := range rm.sessions {
		s.closeOnce.Do(func() {
			s.cancel()
		})
	}
	rm.sessions = make(map[string]*relaySession)
}

// Close 关闭所有中继会话。
func (rm *relayManager) Close() {
	rm.stop()
}

// sendEstablished 发送中继建立状态。
func (rm *relayManager) sendEstablished(sessionID string, connected bool, msg string) {
	if rm.sender == nil {
		return
	}
	status := "connected"
	if !connected {
		status = "failed"
	}
	sendData, _ := json.Marshal(model.RelayStatusData{
		SessionID: sessionID, Status: status, Message: msg,
	})
	rm.sender.WriteJSON(&model.WSMessage{Type: "relay_established", Data: string(sendData)})
}

// sendClosed 发送中继关闭状态。
func (rm *relayManager) sendClosed(sessionID string) {
	if rm.sender == nil {
		return
	}
	sendData, _ := json.Marshal(model.RelayStatusData{SessionID: sessionID, Status: "closed"})
	rm.sender.WriteJSON(&model.WSMessage{Type: "relay_closed", Data: string(sendData)})
}

// cleanupSession 清理中继会话。
func (rm *relayManager) cleanupSession(sessionID string) {
	rm.mu.Lock()
	sess, ok := rm.sessions[sessionID]
	if ok {
		delete(rm.sessions, sessionID)
	}
	rm.mu.Unlock()
	if ok {
		sess.closeOnce.Do(func() {
			sess.cancel()
			if sess.ln != nil {
				sess.ln.Close()
			}
			if sess.mx != nil {
				sess.mx.Close()
			}
		})
	}
}

// runSource 运行源端中继。
func (rm *relayManager) runSource(ctx context.Context, sess *relaySession) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatRelay, "中继源端 panic", "session", sess.id, "error", r)
		}
		rm.cleanupSession(sess.id)
		rm.sendClosed(sess.id)
	}()

	scheme := "wss"
	if rm.cfg.UseHTTP {
		scheme = "ws"
	}
	relayURL := fmt.Sprintf("%s://%s:9909/relay?session=%s&token=%s&role=source&client_id=%s",
		scheme, sess.serverHost, sess.id, sess.token, rm.cfg.ClientID)

	alog.Info(alog.CatRelay, "中继源端: 正在连接中继", "session", sess.id, "role", "source", "server", sess.serverHost)

	conn, err := rm.connectRelay(relayURL)
	if err != nil {
		alog.Error(alog.CatRelay, "中继源端: 中继连接失败", "error", err)
		rm.sendEstablished(sess.id, false, err.Error())
		return
	}

	mx := mux.New(conn)
	sess.mx = mx

	bindAddr := net.JoinHostPort(sess.sourceLocalIP, fmt.Sprintf("%d", sess.sourcePort))
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		alog.Error(alog.CatRelay, "中继源端: 监听失败", "addr", bindAddr, "error", err)
		conn.Close()
		rm.sendEstablished(sess.id, false, err.Error())
		return
	}
	sess.ln = ln
	defer ln.Close()

	rm.sendEstablished(sess.id, true, "")
	alog.Info(alog.CatRelay, "中继源端: 正在监听", "addr", bindAddr)

	go func() {
		<-ctx.Done()
		ln.Close()
		mx.Close()
	}()

	for {
		localConn, err := ln.Accept()
		if err != nil {
			return
		}
		channel, err := mx.OpenChannel(uint16(sess.sourcePort))
		if err != nil {
			localConn.Close()
			return
		}
		alog.Info(alog.CatRelay, "中继源端: 新连接", "channelPort", channel.Port)
		go bridgeChannel(localConn, channel, sess.id)
	}
}

// runTarget 运行目标端中继。
func (rm *relayManager) runTarget(ctx context.Context, sess *relaySession) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatRelay, "中继目标端 panic", "session", sess.id, "error", r)
		}
		rm.cleanupSession(sess.id)
		rm.sendClosed(sess.id)
	}()

	scheme := "wss"
	if rm.cfg.UseHTTP {
		scheme = "ws"
	}
	relayURL := fmt.Sprintf("%s://%s:9909/relay?session=%s&token=%s&role=target&client_id=%s",
		scheme, sess.serverHost, sess.id, sess.token, rm.cfg.ClientID)

	alog.Info(alog.CatRelay, "中继目标端: 正在连接中继", "session", sess.id, "role", "target", "server", sess.serverHost)

	conn, err := rm.connectRelay(relayURL)
	if err != nil {
		alog.Error(alog.CatRelay, "中继目标端: 中继连接失败", "error", err)
		rm.sendEstablished(sess.id, false, err.Error())
		return
	}

	localTarget := net.JoinHostPort(sess.targetLocalIP, fmt.Sprintf("%d", sess.targetPort))
	mx := mux.New(conn)
	mx.LocalTarget = localTarget
	mx.OnNewChannel = mx.HandleChannel
	sess.mx = mx

	rm.sendEstablished(sess.id, true, "")
	alog.Info(alog.CatRelay, "中继目标端: 中继就绪", "localTarget", localTarget)

	go func() {
		<-ctx.Done()
		mx.Close()
	}()

	<-mx.Done()
	alog.Info(alog.CatRelay, "中继目标端: 会话已关闭", "session", sess.id)
}

// connectRelay 连接到中继服务器。
func (rm *relayManager) connectRelay(relayURL string) (net.Conn, error) {
	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	u, _ := url.Parse(relayURL)
	hostname := ""
	if u != nil {
		hostname = u.Hostname()
	}

	if !rm.cfg.UseHTTP {
		dialer.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: rm.cfg.Insecure}
		if rm.cfg.SNIOverride != "" {
			dialer.TLSClientConfig.ServerName = rm.cfg.SNIOverride
		} else if hostname != "" {
			dialer.TLSClientConfig.ServerName = hostname
		}
	}

	header := http.Header{}
	if !rm.cfg.UseHTTP {
		origin := rm.cfg.OriginOverride
		if origin == "" && hostname != "" {
			origin = "https://" + hostname
		}
		if origin != "" {
			header.Set("Origin", origin)
		}
	}

	ws, _, err := dialer.Dial(relayURL, header)
	if err != nil {
		return nil, fmt.Errorf("中继拨号: %w", err)
	}
	return wsconn.New(ws), nil
}

// bridgeChannel 在本地连接和多路复用通道之间桥接数据。
func bridgeChannel(localConn net.Conn, channel *mux.Channel, sessionID string) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatRelay, "bridgeChannel panic", "session", sessionID, "channelPort", channel.Port, "error", r)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// 本地 → 隧道
	go func() {
		defer wg.Done()
		defer localConn.Close()
		buf := make([]byte, mux.MaxFrameSize)
		for {
			n, err := localConn.Read(buf)
			if err != nil {
				break
			}
			if err := channel.Mux.Send(channel.Port, buf[:n]); err != nil {
				break
			}
		}
		channel.Mux.CloseChannel(channel.Port)
	}()

	// 隧道 → 本地
	go func() {
		defer wg.Done()
		defer localConn.Close()
		for {
			data, ok := channel.ReceiveBlocking()
			if !ok {
				break
			}
			if _, err := localConn.Write(data); err != nil {
				break
			}
		}
	}()

	wg.Wait()
}
