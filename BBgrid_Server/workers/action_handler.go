package workers

import (
	"BBgrid/common/plugin"
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// ActionHandler Action 处理器
//
// 负责将 Action 映射到具体的 HTTP 实现。
type ActionHandler struct {
	mu           sync.RWMutex
	handlers     map[string]gin.HandlerFunc
	actions      []plugin.Action
	capabilities []plugin.Capability
}

// NewActionHandler 创建 Action 处理器
func NewActionHandler() *ActionHandler {
	return &ActionHandler{
		handlers: make(map[string]gin.HandlerFunc),
	}
}

// Register 注册 Action 实现
func (h *ActionHandler) Register(action plugin.Action, handler gin.HandlerFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.actions = append(h.actions, action)
	h.handlers[action.Name] = handler
}

// GetSyncResponse 获取同步响应（包含 Actions 和 Capabilities）
func (h *ActionHandler) GetSyncResponse() plugin.SyncResponse {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return plugin.SyncResponse{
		Actions:      h.actions,
		Capabilities: h.capabilities,
	}
}

// HandleRun 处理 run 命令
//
// CLI 调用此端点执行 Action。
func (h *ActionHandler) HandleRun(c *gin.Context) {
	var req struct {
		Action string         `json:"action"`
		Params map[string]any `json:"params"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code": 400,
			"msg":  "invalid request",
		})
		return
	}

	h.mu.RLock()
	handler, ok := h.handlers[req.Action]
	h.mu.RUnlock()

	if !ok {
		c.JSON(http.StatusNotFound, gin.H{
			"code": 404,
			"msg":  fmt.Sprintf("action not found: %s", req.Action),
		})
		return
	}

	// 将参数设置到 query string（正确的方式）
	q := c.Request.URL.Query()
	for k, v := range req.Params {
		q.Set(k, fmt.Sprintf("%v", v))
	}
	c.Request.URL.RawQuery = q.Encode()

	handler(c)
}
