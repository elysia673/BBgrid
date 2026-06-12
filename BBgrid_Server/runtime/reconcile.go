package runtime

import (
	alog "BBgrid/common/log"
	"context"
	"fmt"
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
// 支持：定时协调 + 事件驱动触发 + 失败退避。
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

	// 失败退避：记录连续失败次数和上次失败时间
	proxyFailCount map[string]int       // key: "clientID:port"
	proxyFailTime  map[string]time.Time // key: "clientID:port"
	relayFailCount map[string]int       // key: sessionID
	relayFailTime  map[string]time.Time // key: sessionID
	backoffMu      sync.Mutex
}

// NewReconcileEngine 创建协调引擎
func NewReconcileEngine(config ReconcileConfig, state StateStore, provider ReconcileProvider) *ReconcileEngine {
	return &ReconcileEngine{
		config:         config,
		state:          state,
		provider:       provider,
		triggerCh:      make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		history:        make([]ReconcileResult, 0, 100),
		proxyFailCount: make(map[string]int),
		proxyFailTime:  make(map[string]time.Time),
		relayFailCount: make(map[string]int),
		relayFailTime:  make(map[string]time.Time),
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
		// <-ch 从 channel 读取但丢弃值，常用于信号通知
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

// reconcileProxies 协调代理状态：创建缺失的，删除多余的
func (e *ReconcileEngine) reconcileProxies() (total, fixed, failed int) {
	desired := e.state.GetDesiredProxies()
	actual := e.state.ListProxies()

	desiredSet := make(map[string]bool, len(desired))
	for _, d := range desired {
		key := fmt.Sprintf("%s:%d", d.ClientID, d.RemotePort)
		desiredSet[key] = true
	}

	actualSet := make(map[string]bool, len(actual))
	for _, a := range actual {
		key := fmt.Sprintf("%s:%d", a.ClientID, a.RemotePort)
		actualSet[key] = true
	}

	// 创建：desired 有但 actual 没有
	for _, d := range desired {
		key := fmt.Sprintf("%s:%d", d.ClientID, d.RemotePort)
		if actualSet[key] {
			e.backoffMu.Lock()
			delete(e.proxyFailCount, key)
			delete(e.proxyFailTime, key)
			e.backoffMu.Unlock()
			continue
		}
		total++

		// 退避检查：失败后指数退避，最长 10 分钟
		e.backoffMu.Lock()
		failCount := e.proxyFailCount[key]
		failTime := e.proxyFailTime[key]
		e.backoffMu.Unlock()
		if failCount > 0 {
			backoff := time.Duration(1<<uint(failCount-1)) * 30 * time.Second
			if backoff > 10*time.Minute {
				backoff = 10 * time.Minute
			}
			if time.Since(failTime) < backoff {
				continue // 还在退避期，跳过
			}
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
				"client_id", d.ClientID, "port", d.RemotePort, "error", err)
			e.backoffMu.Lock()
			e.proxyFailCount[key]++
			e.proxyFailTime[key] = time.Now()
			e.backoffMu.Unlock()
			failed++
		} else {
			e.backoffMu.Lock()
			delete(e.proxyFailCount, key)
			delete(e.proxyFailTime, key)
			e.backoffMu.Unlock()
			fixed++
		}
	}

	// 删除：actual 有但 desired 没有
	for _, a := range actual {
		key := fmt.Sprintf("%s:%d", a.ClientID, a.RemotePort)
		if desiredSet[key] {
			continue
		}
		total++

		if err := e.provider.DeleteProxy(a.ClientID, a.RemotePort); err != nil {
			alog.Error(alog.CatSystem, "Reconcile 删除代理失败",
				"client_id", a.ClientID, "port", a.RemotePort, "error", err)
			failed++
		} else {
			fixed++
		}
	}

	return
}

// reconcileRelays 协调中继状态：创建缺失的，删除多余的
func (e *ReconcileEngine) reconcileRelays() (total, fixed, failed int) {
	desired := e.state.GetDesiredRelays()
	actual := e.state.ListRelaySessions()

	desiredSet := make(map[string]bool, len(desired))
	for _, d := range desired {
		desiredSet[d.ID] = true
	}

	actualSet := make(map[string]bool, len(actual))
	for _, a := range actual {
		actualSet[a.ID] = true
	}

	// 创建：desired 有但 actual 没有
	for _, d := range desired {
		if actualSet[d.ID] {
			e.backoffMu.Lock()
			delete(e.relayFailCount, d.ID)
			delete(e.relayFailTime, d.ID)
			e.backoffMu.Unlock()
			continue
		}
		total++

		// 退避检查
		e.backoffMu.Lock()
		failCount := e.relayFailCount[d.ID]
		failTime := e.relayFailTime[d.ID]
		e.backoffMu.Unlock()
		if failCount > 0 {
			backoff := time.Duration(1<<uint(failCount-1)) * 30 * time.Second
			if backoff > 10*time.Minute {
				backoff = 10 * time.Minute
			}
			if time.Since(failTime) < backoff {
				continue
			}
		}

		session := RelaySession{
			ID:            d.ID,
			SourceClient:  d.SourceClient,
			TargetClient:  d.TargetClient,
			Protocol:      d.Protocol,
			SourcePort:    d.SourcePort,
			TargetPort:    d.TargetPort,
			TargetLocalIP: d.TargetLocalIP,
			SourceLocalIP: d.SourceLocalIP,
		}

		if err := e.provider.CreateRelay(session); err != nil {
			alog.Error(alog.CatSystem, "Reconcile 创建中继失败",
				"session_id", d.ID, "error", err)
			e.backoffMu.Lock()
			e.relayFailCount[d.ID]++
			e.relayFailTime[d.ID] = time.Now()
			e.backoffMu.Unlock()
			failed++
		} else {
			e.backoffMu.Lock()
			delete(e.relayFailCount, d.ID)
			delete(e.relayFailTime, d.ID)
			e.backoffMu.Unlock()
			fixed++
		}
	}

	// 删除：actual 有但 desired 没有
	for _, a := range actual {
		if desiredSet[a.ID] {
			continue
		}
		total++

		if err := e.provider.DeleteRelay(a.ID); err != nil {
			alog.Error(alog.CatSystem, "Reconcile 删除中继失败",
				"session_id", a.ID, "error", err)
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
