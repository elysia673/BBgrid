package workers

import (
	"BBgrid/common/plugin"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ==================== Action 注册表 ====================

// ActionSource Action 来源
type ActionSource string

const (
	ActionSourceInternal ActionSource = "internal" // 内置插件 (HTTP handler)
	ActionSourceExternal ActionSource = "external" // 外部插件 (gRPC Execute)
)

// RegisteredAction 已注册的 Action
type RegisteredAction struct {
	Action   plugin.Action
	Source   ActionSource
	Handler  gin.HandlerFunc // 内置插件的 HTTP handler
	PluginID string          // 外部插件的 ID
}

// ActionRegistry 统一 Action 注册表
//
// 管理所有插件的 Actions（内置 + 外部）。
type ActionRegistry struct {
	mu      sync.RWMutex
	actions map[string]*RegisteredAction
}

// NewActionRegistry 创建 ActionRegistry
func NewActionRegistry() *ActionRegistry {
	return &ActionRegistry{
		actions: make(map[string]*RegisteredAction),
	}
}

// RegisterInternal 注册内置插件的 Action
func (r *ActionRegistry) RegisterInternal(pluginID string, action plugin.Action, handler gin.HandlerFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions[action.Name] = &RegisteredAction{
		Action:   action,
		Source:   ActionSourceInternal,
		Handler:  handler,
		PluginID: pluginID,
	}
}

// RegisterExternal 注册外部插件的 Action
func (r *ActionRegistry) RegisterExternal(pluginID string, actions []plugin.Action) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, action := range actions {
		r.actions[action.Name] = &RegisteredAction{
			Action:   action,
			Source:   ActionSourceExternal,
			PluginID: pluginID,
		}
	}
}

// UnregisterPlugin 注销插件的所有 Actions
func (r *ActionRegistry) UnregisterPlugin(pluginID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, action := range r.actions {
		if action.PluginID == pluginID {
			delete(r.actions, name)
		}
	}
}

// Get 获取 Action
func (r *ActionRegistry) Get(name string) (*RegisteredAction, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	action, ok := r.actions[name]
	return action, ok
}

// ListAll 列出所有 Actions
func (r *ActionRegistry) ListAll() []plugin.Action {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]plugin.Action, 0, len(r.actions))
	for _, a := range r.actions {
		result = append(result, a.Action)
	}
	return result
}

// PluginActions 插件的 Actions 分组
type PluginActions struct {
	PluginID string          `json:"plugin_id"`
	Actions  []plugin.Action `json:"actions"`
}

// ListGrouped 按插件分组列出 Actions
func (r *ActionRegistry) ListGrouped() []PluginActions {
	r.mu.RLock()
	defer r.mu.RUnlock()

	groups := make(map[string][]plugin.Action)
	for _, a := range r.actions {
		pid := a.PluginID
		if pid == "" {
			pid = "server"
		}
		groups[pid] = append(groups[pid], a.Action)
	}

	result := make([]PluginActions, 0, len(groups))
	for pid, actions := range groups {
		result = append(result, PluginActions{PluginID: pid, Actions: actions})
	}
	return result
}

// ==================== Task 管理 ====================

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
)

// Task 异步任务
type Task struct {
	ID        string          `json:"id"`
	Action    string          `json:"action"`
	Status    TaskStatus      `json:"status"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     string          `json:"error,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// TaskManager 异步任务管理器
type TaskManager struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

// NewTaskManager 创建 TaskManager
func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*Task),
	}
}

// Create 创建异步任务
func (m *TaskManager) Create(action string) *Task {
	id := generateTaskID()
	task := &Task{
		ID:        id,
		Action:    action,
		Status:    TaskStatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	m.mu.Lock()
	m.tasks[id] = task
	m.mu.Unlock()
	return task
}

// Run 异步执行任务
func (m *TaskManager) Run(task *Task, fn func() (any, error)) {
	m.updateStatus(task.ID, TaskStatusRunning, nil, "")

	go func() {
		result, err := fn()
		if err != nil {
			m.updateStatus(task.ID, TaskStatusFailed, nil, err.Error())
			return
		}
		data, _ := json.Marshal(result)
		m.updateStatus(task.ID, TaskStatusCompleted, data, "")
	}()
}

// Get 获取任务状态
func (m *TaskManager) Get(id string) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.tasks[id]
	return task, ok
}

func (m *TaskManager) updateStatus(id string, status TaskStatus, result json.RawMessage, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if task, ok := m.tasks[id]; ok {
		task.Status = status
		task.Result = result
		task.Error = errMsg
		task.UpdatedAt = time.Now()
	}
}

func generateTaskID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
