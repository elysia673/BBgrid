package runtime

import (
	alog "BBgrid/common/log"
	"context"
	"sync"
	"time"
)

// ReconcileConfig 协调器配置
type ReconcileConfig struct {
	Interval     time.Duration // 协调间隔
	InitialDelay time.Duration // 初始延迟
}

// DefaultReconcileConfig 默认配置
func DefaultReconcileConfig() ReconcileConfig {
	return ReconcileConfig{
		Interval:     30 * time.Second,
		InitialDelay: 5 * time.Second,
	}
}

// ReconcileEngine 协调引擎 (Runtime Core 内置)
//
// 负责保证 desired state == actual state。
// 不是 plugin，是 Runtime Core 的核心机制。
type ReconcileEngine struct {
	config   ReconcileConfig
	state    StateStore
	provider ReconcileProvider

	triggerCh chan struct{}
	mu        sync.RWMutex
	last      *ReconcileResult
	history   []ReconcileResult
	stopCh    chan struct{}
	stopped   bool
}

// NewReconcileEngine 创建协调引擎
func NewReconcileEngine(config ReconcileConfig, state StateStore, provider ReconcileProvider) *ReconcileEngine {
	return &ReconcileEngine{
		config:    config,
		state:     state,
		provider:  provider,
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
		history:   make([]ReconcileResult, 0, 100),
	}
}

// Trigger 手动触发协调
func (e *ReconcileEngine) Trigger() {
	select {
	case e.triggerCh <- struct{}{}:
	default:
		// 已经有 pending trigger，忽略
	}
}

// Run 启动协调循环 (阻塞)
func (e *ReconcileEngine) Run() error {
	alog.Info(alog.CatSystem, "ReconcileEngine 启动")

	// 初始延迟
	select {
	case <-time.After(e.config.InitialDelay):
	case <-e.stopCh:
		return nil
	}

	// 首次协调
	e.doReconcile()

	// 定期协调 + 触发协调
	ticker := time.NewTicker(e.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.doReconcile()
		case <-e.triggerCh:
			e.doReconcile()
		case <-e.stopCh:
			alog.Info(alog.CatSystem, "ReconcileEngine 停止")
			return nil
		}
	}
}

// Stop 停止协调器
func (e *ReconcileEngine) Stop() {
	e.mu.Lock()
	if !e.stopped {
		e.stopped = true
		close(e.stopCh)
	}
	e.mu.Unlock()
}

// GetLastResult 获取最近一次协调结果
func (e *ReconcileEngine) GetLastResult() *ReconcileResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.last
}

// GetHistory 获取协调历史
func (e *ReconcileEngine) GetHistory() []ReconcileResult {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]ReconcileResult, len(e.history))
	copy(result, e.history)
	return result
}

// doReconcile 执行协调
func (e *ReconcileEngine) doReconcile() {
	start := time.Now()

	result := ReconcileResult{
		Timestamp: start,
	}

	// 协调代理
	proxiesTotal, proxiesFixed, proxiesFailed := e.reconcileProxies()
	result.ProxiesTotal = proxiesTotal
	result.ProxiesFixed = proxiesFixed
	result.ProxiesFailed = proxiesFailed

	// 协调中继
	relaysTotal, relaysFixed, relaysFailed := e.reconcileRelays()
	result.RelaysTotal = relaysTotal
	result.RelaysFixed = relaysFixed
	result.RelaysFailed = relaysFailed

	result.Duration = time.Since(start)

	// 保存结果
	e.mu.Lock()
	e.last = &result
	e.history = append(e.history, result)
	if len(e.history) > 100 {
		e.history = e.history[1:]
	}
	e.mu.Unlock()

	if proxiesFixed > 0 || relaysFixed > 0 {
		alog.Info(alog.CatSystem, "Reconcile 完成",
			"proxies_fixed", proxiesFixed,
			"relays_fixed", relaysFixed,
			"duration", result.Duration,
		)
	}
}

// reconcileProxies 协调代理状态
func (e *ReconcileEngine) reconcileProxies() (total, fixed, failed int) {
	desired := e.state.GetDesiredProxies()
	total = len(desired)

	for _, d := range desired {
		if _, exists := e.state.GetProxy(d.ClientID, d.RemotePort); exists {
			continue
		}

		proxy := ProxyState{
			ClientID:   d.ClientID,
			RemotePort: d.RemotePort,
			LocalPort:  d.LocalPort,
			LocalIP:    d.LocalIP,
			Protocol:   d.Protocol,
			BindAddr:   d.BindAddr,
		}

		if err := e.provider.CreateProxy(d.ClientID, proxy); err != nil {
			alog.Error(alog.CatSystem, "Reconcile 创建代理失败",
				"client_id", d.ClientID,
				"port", d.RemotePort,
				"error", err,
			)
			failed++
		} else {
			fixed++
		}
	}

	return
}

// reconcileRelays 协调中继状态
func (e *ReconcileEngine) reconcileRelays() (total, fixed, failed int) {
	desired := e.state.GetDesiredRelays()
	total = len(desired)

	for _, d := range desired {
		if _, exists := e.state.GetRelaySession(d.ID); exists {
			continue
		}
		stillDesired := false
		for _, cd := range e.state.GetDesiredRelays() {
			if cd.ID == d.ID {
				stillDesired = true
				break
			}
		}
		if !stillDesired {
			continue
		}

		// 期望存在但实际不存在，需要创建
		session := RelaySession{
			ID:            d.ID,
			SourceClient:  d.SourceClient,
			TargetClient:  d.TargetClient,
			Protocol:      d.Protocol,
			SourcePort:    d.SourcePort,
			TargetPort:    d.TargetPort,
			TargetLocalIP: d.TargetLocalIP,
			SourceLocalIP: d.SourceLocalIP,
			Token:         d.Token,
		}

		if err := e.provider.CreateRelay(session); err != nil {
			alog.Error(alog.CatSystem, "Reconcile 创建中继失败",
				"session_id", d.ID,
				"error", err,
			)
			failed++
		} else {
			fixed++
		}
	}

	return
}

// ReconcileEngineFromContext 从 context 创建可取消的协调引擎
func ReconcileEngineFromContext(ctx context.Context, config ReconcileConfig, state StateStore, provider ReconcileProvider) *ReconcileEngine {
	engine := NewReconcileEngine(config, state, provider)
	go func() {
		<-ctx.Done()
		engine.Stop()
	}()
	return engine
}
