package handler

import (
	"BBgrid/BBgrid_Server/manager"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"BBgrid/common/wsconn"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// RelayHandler 处理中继连接请求。
type RelayHandler struct {
	clientMgr  *manager.ClientManager
	serverHost string

	mu       sync.Mutex
	sessions map[string]*relaySessionState
}

// relaySessionState 中继会话状态。
type relaySessionState struct {
	ID            string
	SourceClient  string
	TargetClient  string
	Protocol      string
	SourcePort    int
	TargetPort    int
	TargetLocalIP string
	SourceLocalIP string
	Token         string
	CreatedAt     time.Time
	SourceReady   bool
	TargetReady   bool
	Error         string
	sourceConn    *websocket.Conn
	targetConn    *websocket.Conn
	done          chan struct{}
	once          sync.Once
}

// NewRelayHandler 创建中继处理器。
func NewRelayHandler(clientMgr *manager.ClientManager, serverHost string) *RelayHandler {
	return &RelayHandler{
		clientMgr:  clientMgr,
		serverHost: serverHost,
		sessions:   make(map[string]*relaySessionState),
	}
}

// relayConnectRequest 中继连接请求。
type relayConnectRequest struct {
	SourceClientID string `json:"source_client_id" binding:"required"`
	TargetClientID string `json:"target_client_id" binding:"required"`
	TargetPort     int    `json:"target_port" binding:"required,min=1,max=65535"`
	SourcePort     int    `json:"source_port" binding:"required,min=1,max=65535"`
	Protocol       string `json:"protocol" binding:"required,oneof=udp tcp websocket"`
	TargetLocalIP  string `json:"target_local_ip"`
	SourceLocalIP  string `json:"source_local_ip"`
}

// CreateRelay 创建中继连接。
func (h *RelayHandler) CreateRelay(c *gin.Context) {
	var req relayConnectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(400, "invalid request: "+err.Error()))
		return
	}

	if req.SourceClientID == req.TargetClientID {
		c.JSON(http.StatusBadRequest, model.Error(400, "source and target clients must be different"))
		return
	}

	_, ok := h.clientMgr.Get(req.SourceClientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "source client not found"))
		return
	}

	_, ok = h.clientMgr.Get(req.TargetClientID)
	if !ok {
		c.JSON(http.StatusNotFound, model.Error(404, "target client not found"))
		return
	}

	if req.TargetLocalIP == "" {
		req.TargetLocalIP = "127.0.0.1"
	}
	if req.SourceLocalIP == "" {
		req.SourceLocalIP = "0.0.0.0"
	}

	sessionID := generateSessionID()
	token := generateToken()

	serverHost := h.serverHost
	if serverHost == "" {
		serverHost = h.clientMgr.GetPublicIP()
	}

	session := &relaySessionState{
		ID:            sessionID,
		SourceClient:  req.SourceClientID,
		TargetClient:  req.TargetClientID,
		Protocol:      req.Protocol,
		SourcePort:    req.SourcePort,
		TargetPort:    req.TargetPort,
		TargetLocalIP: req.TargetLocalIP,
		SourceLocalIP: req.SourceLocalIP,
		Token:         token,
		CreatedAt:     time.Now(),
		done:          make(chan struct{}),
	}

	h.mu.Lock()
	h.sessions[sessionID] = session
	h.mu.Unlock()

	signal := model.WSMessage{
		Type: "relay_signal",
		Data: model.RelaySignalData{
			SessionID:     sessionID,
			Protocol:      req.Protocol,
			Role:          "",
			PeerClientID:  "",
			SourcePort:    req.SourcePort,
			TargetPort:    req.TargetPort,
			TargetLocalIP: req.TargetLocalIP,
			SourceLocalIP: req.SourceLocalIP,
			ServerHost:    serverHost,
			Token:         token,
		},
	}

	sourceSignal := signal
	sourceSignalData := sourceSignal.Data.(model.RelaySignalData)
	sourceSignalData.Role = "source"
	sourceSignalData.PeerClientID = req.TargetClientID
	sourceSignal.Data = sourceSignalData

	targetSignal := signal
	targetSignalData := targetSignal.Data.(model.RelaySignalData)
	targetSignalData.Role = "target"
	targetSignalData.PeerClientID = req.SourceClientID
	targetSignal.Data = targetSignalData

	if err := h.clientMgr.SendCommand(req.SourceClientID, sourceSignal); err != nil {
		h.cleanupSession(sessionID)
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send signal to source: "+err.Error()))
		return
	}
	if err := h.clientMgr.SendCommand(req.TargetClientID, targetSignal); err != nil {
		h.cleanupSession(sessionID)
		c.JSON(http.StatusInternalServerError, model.Error(500, "failed to send signal to target: "+err.Error()))
		return
	}

	go h.monitorSession(session)

	alog.Info(alog.CatRelay, "relay session created",
		"session_id", sessionID,
		"source", req.SourceClientID,
		"target", req.TargetClientID,
		"protocol", req.Protocol)

	c.JSON(http.StatusOK, model.Success(map[string]interface{}{
		"session_id":    sessionID,
		"source_client": req.SourceClientID,
		"target_client": req.TargetClientID,
		"protocol":      req.Protocol,
	}))
}

// HandleRelayWS 处理中继 WebSocket 连接。
func (h *RelayHandler) HandleRelayWS(c *gin.Context) {
	sessionID := c.Query("session")
	token := c.Query("token")
	role := c.Query("role")
	clientID := c.Query("client_id")

	if sessionID == "" || token == "" || role == "" || clientID == "" {
		http.Error(c.Writer, "missing params", http.StatusBadRequest)
		return
	}

	h.mu.Lock()
	session, ok := h.sessions[sessionID]
	h.mu.Unlock()

	if !ok {
		http.Error(c.Writer, "session not found", http.StatusNotFound)
		return
	}
	if session.Token != token {
		http.Error(c.Writer, "invalid token", http.StatusForbidden)
		return
	}
	if role == "source" && clientID != session.SourceClient {
		http.Error(c.Writer, "client not authorized for this session", http.StatusForbidden)
		return
	}
	if role == "target" && clientID != session.TargetClient {
		http.Error(c.Writer, "client not authorized for this session", http.StatusForbidden)
		return
	}

	ws, err := newUpgrader(h.serverHost).Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		alog.Error(alog.CatRelay, "relay websocket upgrade error", "error", err)
		return
	}

	if role == "source" {
		session.sourceConn = ws
	} else {
		session.targetConn = ws
	}

	alog.Info(alog.CatRelay, "relay connected", "role", role, "session_id", sessionID)

	if session.sourceConn != nil && session.targetConn != nil {
		go h.bridgeRelay(session)
	}
}

// bridgeRelay 桥接两个客户端的中继连接。
func (h *RelayHandler) bridgeRelay(session *relaySessionState) {
	defer h.cleanupSession(session.ID)

	src := wsconn.New(session.sourceConn)
	dst := wsconn.New(session.targetConn)
	defer src.Close()
	defer dst.Close()

	alog.Info(alog.CatRelay, "bridging relay", "session_id", session.ID)

	bufSize := 256 * 1024
	done := make(chan struct{}, 2)
	go func() {
		buf := make([]byte, bufSize)
		io.CopyBuffer(dst, src, buf)
		done <- struct{}{}
	}()
	go func() {
		buf := make([]byte, bufSize)
		io.CopyBuffer(src, dst, buf)
		done <- struct{}{}
	}()

	<-done
	alog.Info(alog.CatRelay, "relay session closed", "session_id", session.ID)
}

// monitorSession 监控中继会话超时。
func (h *RelayHandler) monitorSession(session *relaySessionState) {
	select {
	case <-session.done:
	case <-time.After(120 * time.Second):
		if session.sourceConn == nil || session.targetConn == nil {
			alog.Warn(alog.CatRelay, "relay connection timeout", "session_id", session.ID)
			h.cleanupSession(session.ID)
		}
	}
}

// HandleClientStatus 处理客户端状态消息。
func (h *RelayHandler) HandleClientStatus(msg *model.WSMessage, clientID string) {
	switch msg.Type {
	case "relay_established":
		var status model.RelayStatusData
		data, _ := json.Marshal(msg.Data)
		json.Unmarshal(data, &status)

		h.mu.Lock()
		session, ok := h.sessions[status.SessionID]
		h.mu.Unlock()
		if !ok {
			return
		}

		if status.Status == "failed" {
			alog.Error(alog.CatRelay, "relay client reported failure",
				"session_id", status.SessionID,
				"client_id", clientID,
				"message", status.Message)
			session.Error = clientID + ": " + status.Message
			session.done <- struct{}{}
			go func() {
				time.Sleep(30 * time.Second)
				h.cleanupSession(status.SessionID)
			}()
			return
		}

		if clientID == session.SourceClient {
			session.SourceReady = true
		} else if clientID == session.TargetClient {
			session.TargetReady = true
		}

	case "relay_closed":
		var status model.RelayStatusData
		data, _ := json.Marshal(msg.Data)
		json.Unmarshal(data, &status)
		h.cleanupSession(status.SessionID)
	}
}

// cleanupSession 清理中继会话。
func (h *RelayHandler) cleanupSession(sessionID string) {
	h.mu.Lock()
	session, ok := h.sessions[sessionID]
	if ok {
		delete(h.sessions, sessionID)
	}
	h.mu.Unlock()

	if ok {
		session.once.Do(func() {
			close(session.done)
			if session.sourceConn != nil {
				session.sourceConn.Close()
			}
			if session.targetConn != nil {
				session.targetConn.Close()
			}
		})
		alog.Info(alog.CatRelay, "relay session cleaned up", "session_id", sessionID)
	}
}

// ListSessions 列出所有中继会话。
func (h *RelayHandler) ListSessions(c *gin.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sessions := make([]map[string]interface{}, 0)
	for _, s := range h.sessions {
		status := "connecting"
		if s.Error != "" {
			status = "failed"
		} else if s.sourceConn != nil && s.targetConn != nil {
			status = "connected"
		}
		entry := map[string]interface{}{
			"session_id":    s.ID,
			"source_client": s.SourceClient,
			"target_client": s.TargetClient,
			"protocol":      s.Protocol,
			"source_port":   s.SourcePort,
			"target_port":   s.TargetPort,
			"status":        status,
			"created_at":    s.CreatedAt.Unix(),
		}
		if s.Error != "" {
			entry["error"] = s.Error
		}
		sessions = append(sessions, entry)
	}
	c.JSON(http.StatusOK, model.Success(map[string]interface{}{"sessions": sessions}))
}

// CloseSession 关闭中继会话。
func (h *RelayHandler) CloseSession(c *gin.Context) {
	sessionID := c.Param("id")
	h.cleanupSession(sessionID)
	c.JSON(http.StatusOK, model.Success(map[string]string{"session_id": sessionID}))
}

func generateSessionID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		alog.Error(alog.CatSystem, "crypto/rand failed, using fallback", "error", err)
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i * 8))
		}
	}
	return hex.EncodeToString(b)
}
