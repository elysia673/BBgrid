package main

import (
	alog "BBgrid/common/log"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogCollector 日志收集器
type LogCollector struct {
	clientID      string
	dataDir       string
	batchSizeKB   int
	batchInterval int
	stopCh        chan struct{}
	logs          []LogEntry
	logsMu        sync.Mutex
	flushMu       sync.Mutex
	lastFlush     time.Time
}

// LogEntry 日志条目
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Data      string    `json:"data,omitempty"`
}

// NewLogCollector 创建日志收集器
func NewLogCollector(clientID, dataDir string, batchSizeKB, batchIntervalSec int) *LogCollector {
	return &LogCollector{
		clientID:      clientID,
		dataDir:       dataDir,
		batchSizeKB:   batchSizeKB,
		batchInterval: batchIntervalSec,
		stopCh:        make(chan struct{}),
		logs:          make([]LogEntry, 0),
		lastFlush:     time.Now(),
	}
}

// Start 启动日志收集器
func (c *LogCollector) Start() {
	go c.flushLoop()
	alog.Info(alog.CatSystem, "Log collector started",
		"batch_size_kb", c.batchSizeKB,
		"batch_interval_sec", c.batchInterval)
}

// Stop 停止日志收集器
func (c *LogCollector) Stop() {
	close(c.stopCh)
	c.flush()
}

// AddLog 添加日志
func (c *LogCollector) AddLog(level, message, data string) {
	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Data:      data,
	}

	c.logsMu.Lock()
	c.logs = append(c.logs, entry)
	shouldFlush := c.shouldFlush()
	c.logsMu.Unlock()

	if shouldFlush {
		go c.flush()
	}
}

// SendImmediate 立即推送
func (c *LogCollector) SendImmediate(level, message, data string) {
	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Data:      data,
	}

	c.logsMu.Lock()
	c.logs = append(c.logs, entry)
	c.logsMu.Unlock()

	go c.flush()
}

// shouldFlush 检查是否应该刷新
func (c *LogCollector) shouldFlush() bool {
	// 检查大小
	size := c.calculateSize()
	if size >= c.batchSizeKB*1024 {
		return true
	}

	// 检查时间
	if time.Since(c.lastFlush).Seconds() >= float64(c.batchInterval) {
		return true
	}

	return false
}

// calculateSize 计算当前日志大小
func (c *LogCollector) calculateSize() int {
	data, _ := json.Marshal(c.logs)
	return len(data)
}

// flushLoop 定时刷新循环
func (c *LogCollector) flushLoop() {
	ticker := time.NewTicker(time.Duration(c.batchInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.flush()
		}
	}
}

// flush 刷新日志到文件
func (c *LogCollector) flush() {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.logsMu.Lock()
	if len(c.logs) == 0 {
		c.logsMu.Unlock()
		return
	}

	logs := c.logs
	c.logs = make([]LogEntry, 0)
	lastFlush := c.lastFlush
	c.lastFlush = time.Now()
	c.logsMu.Unlock()

	// 打包成文件
	filename := fmt.Sprintf("%s_%s.log", c.clientID, lastFlush.Format("20060102_150405"))
	filePath := filepath.Join(c.dataDir, "tmp", "logs", filename)

	// 确保目录存在
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		alog.Error(alog.CatSystem, "Failed to create log directory", "error", err)
		return
	}

	// 写入文件
	data, _ := json.MarshalIndent(logs, "", "  ")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		alog.Error(alog.CatSystem, "Failed to write log file", "error", err)
		return
	}

	alog.Info(alog.CatSystem, "Log file saved", "filename", filename, "size", len(data))
}
