package main

import (
	alog "BBgrid/common/log"
	"sync"
	"time"
)

// Worker 工作单元接口
//
// 每个 Worker 代表一个独立的功能模块，
// 运行在自己的 goroutine 中，由 Supervisor 监控。
type Worker interface {
	// Name 返回 Worker 名称
	Name() string

	// Run 启动 Worker，阻塞直到退出
	// 返回 error 表示异常退出，nil 表示正常退出
	Run() error

	// Stop 通知 Worker 停止
	Stop()
}

// Supervisor 进程监控器
//
// 负责启动、监控和重启 Worker。
// 如果 Worker 异常退出，Supervisor 会自动重启它。
type Supervisor struct {
	workers  map[string]Worker
	restarts map[string]int
	maxRetry int
	mu       sync.Mutex
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewSupervisor 创建 Supervisor
func NewSupervisor() *Supervisor {
	return &Supervisor{
		workers:  make(map[string]Worker),
		restarts: make(map[string]int),
		maxRetry: 5,
		stopChan: make(chan struct{}),
	}
}

// SetMaxRetry 设置最大重试次数
func (s *Supervisor) SetMaxRetry(n int) {
	s.maxRetry = n
}

// Add 添加 Worker
func (s *Supervisor) Add(w Worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[w.Name()] = w
}

// Start 启动所有 Worker
func (s *Supervisor) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, w := range s.workers {
		alog.Info(alog.CatSystem, "启动 Worker", "name", name)
		s.wg.Add(1)
		go s.watch(w)
	}
}

// watch 监控 Worker，异常退出时自动重启
func (s *Supervisor) watch(w Worker) {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopChan:
			return
		default:
		}

		err := w.Run()
		if err == nil {
			// 正常退出
			alog.Info(alog.CatSystem, "Worker 正常退出", "name", w.Name())
			return
		}

		// 异常退出，检查重试次数
		s.mu.Lock()
		s.restarts[w.Name()]++
		restarts := s.restarts[w.Name()]
		s.mu.Unlock()

		if restarts > s.maxRetry {
			alog.Error(alog.CatSystem, "Worker 重启次数超限，停止重启",
				"name", w.Name(),
				"restarts", restarts,
				"maxRetry", s.maxRetry,
				"error", err)
			return
		}

		alog.Warn(alog.CatSystem, "Worker 异常退出，准备重启",
			"name", w.Name(),
			"restarts", restarts,
			"maxRetry", s.maxRetry,
			"error", err)

		// 等待一段时间再重启，避免快速重启循环
		select {
		case <-s.stopChan:
			return
		case <-time.After(time.Duration(restarts) * time.Second):
		}
	}
}

// Stop 优雅停止所有 Worker
func (s *Supervisor) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	alog.Info(alog.CatSystem, "正在停止 Supervisor")

	// 通知所有 watch 退出
	close(s.stopChan)

	// 停止所有 Worker
	for name, w := range s.workers {
		alog.Info(alog.CatSystem, "正在停止 Worker", "name", name)
		w.Stop()
	}

	// 等待所有 watch 退出
	s.wg.Wait()

	alog.Info(alog.CatSystem, "Supervisor 已停止")
}

// Stats 返回运行统计
func (s *Supervisor) Stats() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := make(map[string]int)
	for name, count := range s.restarts {
		stats[name] = count
	}
	return stats
}
