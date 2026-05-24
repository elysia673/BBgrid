package workers

import (
	alog "BBgrid/common/log"
	"fmt"
	"sync"
	"time"
)

// ==================== ReconcileWorker ====================

// ReconcileConfig Reconcile 配置
type ReconcileConfig struct {
	// Interval 周期性 Reconcile 间隔
	Interval time.Duration

	// InitialDelay 首次 Reconcile 延迟（等待系统初始化）
	InitialDelay time.Duration
}

// DefaultReconcileConfig 默认配置
func DefaultReconcileConfig() ReconcileConfig {
	return ReconcileConfig{
		Interval:     30 * time.Second,
		InitialDelay: 5 * time.Second,
	}
}

// ReconcileResult 单次 Reconcile 结果
type ReconcileResult struct {
	Timestamp     time.Time
	ProxiesTotal  int
	ProxiesFixed  int
	ProxiesFailed int
	RelaysTotal   int
	RelaysFixed   int
	RelaysFailed  int
	Duration      time.Duration
}

// ReconcileWorker 状态协调 Worker
//
// 周期性检查期望状态与运行时状态的差异，自动修复。
// 实现 Worker 接口，由 Supervisor 管理。
type ReconcileWorker struct {
	config  ReconcileConfig
	state   *StateWorker
	control *ControlWorker
	data    *DataWorker

	// 手动触发通道
	triggerCh chan struct{}

	// 指标
	mu      sync.RWMutex
	last    *ReconcileResult
	history []ReconcileResult
	stopCh  chan struct{}
}

// NewReconcileWorker 创建 Reconcile Worker
func NewReconcileWorker(config ReconcileConfig, state *StateWorker, control *ControlWorker, data *DataWorker) *ReconcileWorker {
	return &ReconcileWorker{
		config:    config,
		state:     state,
		control:   control,
		data:      data,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
}

// Name 返回 Worker 名称
func (w *ReconcileWorker) Name() string {
	return "reconcile"
}

// Run 启动 Reconcile Worker
func (w *ReconcileWorker) Run() error {
	alog.Info(alog.CatSystem, "ReconcileWorker 启动",
		"interval", w.config.Interval)

	// 等待初始延迟（让其他 Worker 先就绪）
	select {
	case <-w.stopCh:
		return nil
	case <-time.After(w.config.InitialDelay):
	}

	// 首次启动执行一次
	w.doReconcile()

	ticker := time.NewTicker(w.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			alog.Info(alog.CatSystem, "ReconcileWorker 停止")
			return nil
		case <-ticker.C:
			w.doReconcile()
		case <-w.triggerCh:
			w.doReconcile()
		}
	}
}

// Stop 停止 Reconcile Worker
func (w *ReconcileWorker) Stop() {
	close(w.stopCh)
}

// Trigger 手动触发一次 Reconcile（非阻塞）
func (w *ReconcileWorker) Trigger() {
	select {
	case w.triggerCh <- struct{}{}:
	default:
		// 已有 pending 触发，跳过
	}
}

// GetLastResult 获取最近一次 Reconcile 结果
func (w *ReconcileWorker) GetLastResult() *ReconcileResult {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.last
}

// GetHistory 获取历史记录（最近 100 次）
func (w *ReconcileWorker) GetHistory() []ReconcileResult {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make([]ReconcileResult, len(w.history))
	copy(result, w.history)
	return result
}

// doReconcile 执行一次完整的 Reconcile
func (w *ReconcileWorker) doReconcile() {
	start := time.Now()

	result := ReconcileResult{
		Timestamp: start,
	}

	// Reconcile Proxies
	result.ProxiesTotal, result.ProxiesFixed, result.ProxiesFailed = w.reconcileProxies()

	// Reconcile Relays
	result.RelaysTotal, result.RelaysFixed, result.RelaysFailed = w.reconcileRelays()

	result.Duration = time.Since(start)

	// 保存结果
	w.mu.Lock()
	w.last = &result
	w.history = append(w.history, result)
	if len(w.history) > 100 {
		w.history = w.history[1:]
	}
	w.mu.Unlock()

	// 只在有修复或失败时打印日志
	if result.ProxiesFixed > 0 || result.ProxiesFailed > 0 ||
		result.RelaysFixed > 0 || result.RelaysFailed > 0 {
		alog.Info(alog.CatSystem, "Reconcile 完成",
			"proxies", result.ProxiesFixed,
			"relays", result.RelaysFixed,
			"failed", result.ProxiesFailed+result.RelaysFailed,
			"duration", result.Duration)
	}
}

// reconcileProxies 协调代理状态
func (w *ReconcileWorker) reconcileProxies() (total, fixed, failed int) {
	// 收集需要恢复的 proxy
	w.state.proxyMu.RLock()
	var toRestore []ProxyDesiredState
	for _, d := range w.state.desiredProxies {
		if _, ok := w.state.GetProxy(d.ClientID, d.RemotePort); ok {
			continue
		}
		toRestore = append(toRestore, d)
	}
	w.state.proxyMu.RUnlock()

	total = len(toRestore)

	for _, d := range toRestore {
		// 检查客户端是否在线
		if _, ok := w.state.GetClient(d.ClientID); !ok {
			continue
		}

		// 先关闭可能残留的旧 listener
		key := fmt.Sprintf("%s-%d", d.ClientID, d.RemotePort)
		w.data.CloseListener(key)

		_, err := w.control.CreateProxy(CreateProxyRequest{
			ClientID:   d.ClientID,
			RemotePort: d.RemotePort,
			LocalPort:  d.LocalPort,
			LocalIP:    d.LocalIP,
			Protocol:   d.Protocol,
			BindAddr:   d.BindAddr,
		})
		if err != nil {
			failed++
			alog.Warn(alog.CatProxy, "Reconcile: 恢复 proxy 失败",
				"clientID", d.ClientID,
				"port", d.RemotePort,
				"error", err)
		} else {
			fixed++
			alog.Info(alog.CatProxy, "Reconcile: proxy 已恢复",
				"clientID", d.ClientID,
				"port", d.RemotePort)
		}
	}

	return
}

// reconcileRelays 协调中继状态
func (w *ReconcileWorker) reconcileRelays() (total, fixed, failed int) {
	// 收集需要恢复的 relay
	w.state.relayMu.RLock()
	var toRestore []RelayDesiredState
	for _, d := range w.state.desiredRelays {
		if _, exists := w.state.relaySessions[d.ID]; exists {
			continue
		}
		toRestore = append(toRestore, d)
	}
	w.state.relayMu.RUnlock()

	total = len(toRestore)

	for _, d := range toRestore {
		// 检查双端客户端是否在线
		if _, ok := w.state.GetClient(d.SourceClient); !ok {
			continue
		}
		if _, ok := w.state.GetClient(d.TargetClient); !ok {
			continue
		}

		_, err := w.control.CreateRelay(CreateRelayRequest{
			SourceClientID: d.SourceClient,
			TargetClientID: d.TargetClient,
			SourcePort:     d.SourcePort,
			TargetPort:     d.TargetPort,
			Protocol:       d.Protocol,
			TargetLocalIP:  d.TargetLocalIP,
			SourceLocalIP:  d.SourceLocalIP,
		})
		if err != nil {
			failed++
			alog.Warn(alog.CatRelay, "Reconcile: 恢复 relay 失败",
				"source", d.SourceClient,
				"target", d.TargetClient,
				"error", err)
		} else {
			fixed++
			alog.Info(alog.CatRelay, "Reconcile: relay 已恢复",
				"source", d.SourceClient,
				"target", d.TargetClient)
		}
	}

	return
}
