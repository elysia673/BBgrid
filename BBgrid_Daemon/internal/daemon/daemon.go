// Package daemon 提供守护进程核心功能。
package daemon

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Config daemon 配置
type Config struct {
	Version    string
	SocketPath string
	CtlPath    string
	UpdateURL  string
	Server     ServiceConfig
	Client     ServiceConfig
}

// ServiceConfig 服务配置
type ServiceConfig struct {
	Enabled    bool
	BinPath    string
	ConfigPath string
}

// Daemon 守护进程
type Daemon struct {
	config     Config
	listener   net.Listener
	services   map[string]*Service
	servicesMu sync.RWMutex
	stopCh     chan struct{}
}

// Service 服务信息
type Service struct {
	Name       string
	Config     ServiceConfig
	Cmd        *exec.Cmd
	PID        int
	Running    bool
	Registered bool
	LastBeat   time.Time
	Version    string
	mu         sync.Mutex
}

// Message 消息协议
type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// RegisterPayload 注册消息
type RegisterPayload struct {
	Name    string `json:"name"`
	PID     int    `json:"pid"`
	Version string `json:"version"`
}

// HeartbeatPayload 心跳消息
type HeartbeatPayload struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// CommandPayload 命令消息
type CommandPayload struct {
	Command string   `json:"command"`
	Target  string   `json:"target"`
	Args    []string `json:"args,omitempty"`
}

// New 创建 daemon
func New(config Config) *Daemon {
	return &Daemon{
		config:   config,
		services: make(map[string]*Service),
		stopCh:   make(chan struct{}),
	}
}

// Start 启动 daemon
func (d *Daemon) Start() error {
	// 清理旧 socket
	os.Remove(d.config.SocketPath)

	// 创建 socket 目录
	os.MkdirAll(filepath.Dir(d.config.SocketPath), 0755)

	// 监听 Unix Socket
	listener, err := net.Listen("unix", d.config.SocketPath)
	if err != nil {
		return fmt.Errorf("listen socket: %w", err)
	}
	d.listener = listener

	// 设置 socket 权限
	os.Chmod(d.config.SocketPath, 0660)

	// 启动健康检查
	go d.healthCheckLoop()

	// 接受连接
	go d.acceptLoop()

	return nil
}

// StartServicesViaCtl 通过 ctl 拉起服务
func (d *Daemon) StartServicesViaCtl() {
	// 等待 socket 就绪
	time.Sleep(500 * time.Millisecond)

	if d.config.Server.Enabled {
		go d.execCtl("start", "server")
	}
	if d.config.Client.Enabled {
		go d.execCtl("start", "client")
	}
}

// Stop 停止 daemon
func (d *Daemon) Stop() {
	close(d.stopCh)

	// 通过 ctl 停止服务
	if d.config.Server.Enabled {
		d.execCtl("stop", "server")
	}
	if d.config.Client.Enabled {
		d.execCtl("stop", "client")
	}

	// 关闭 socket
	if d.listener != nil {
		d.listener.Close()
	}
	os.Remove(d.config.SocketPath)
}

// execCtl 执行 ctl 命令
func (d *Daemon) execCtl(args ...string) {
	ctlPath := d.config.CtlPath
	if _, err := os.Stat(ctlPath); os.IsNotExist(err) {
		log.Printf("Ctl not found: %s", ctlPath)
		return
	}

	// 构建参数: -socket <path> <command> <target>
	ctlArgs := append([]string{"-socket", d.config.SocketPath}, args...)
	cmd := exec.Command(ctlPath, ctlArgs...)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setSysProcAttr(cmd.SysProcAttr)

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Ctl %v failed: %v, output: %s", args, err, string(output))
	} else {
		log.Printf("Ctl %v success: %s", args, string(output))
	}
}

// healthCheckLoop 健康检查循环
func (d *Daemon) healthCheckLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.checkAndRestart()
		}
	}
}

// checkAndRestart 检查并重启死掉的服务
func (d *Daemon) checkAndRestart() {
	d.servicesMu.Lock()
	defer d.servicesMu.Unlock()

	for name, svc := range d.services {
		svc.mu.Lock()

		// 检查应该运行但实际没有运行的服务
		if !svc.Running {
			// 检查是否应该运行（配置中启用）
			shouldRun := false
			switch name {
			case "server":
				shouldRun = d.config.Server.Enabled
			case "client":
				shouldRun = d.config.Client.Enabled
			}

			if shouldRun {
				log.Printf("Service %s is not running, restarting...", name)
				svc.mu.Unlock()
				go d.execCtl("start", name)
				continue
			}
			svc.mu.Unlock()
			continue
		}

		// 检查进程是否还在
		if svc.Cmd != nil && svc.Cmd.Process != nil {
			err := svc.Cmd.Process.Signal(syscall.Signal(0))
			if err != nil {
				// 进程已死，重新拉起
				log.Printf("Service %s died, restarting...", name)
				svc.Running = false
				svc.Registered = false
				svc.PID = 0
				svc.Cmd = nil
				svc.mu.Unlock()

				// 通过 ctl 重新拉起
				go d.execCtl("start", name)
				continue
			}
		}

		svc.mu.Unlock()
	}
}

// acceptLoop 接受连接循环
func (d *Daemon) acceptLoop() {
	for {
		select {
		case <-d.stopCh:
			return
		default:
		}

		conn, err := d.listener.Accept()
		if err != nil {
			select {
			case <-d.stopCh:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}

		go d.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (d *Daemon) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			return
		}

		switch msg.Type {
		case "register":
			var payload RegisterPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				log.Printf("Invalid register payload: %v", err)
				continue
			}
			d.handleRegister(payload)
			encoder.Encode(Message{Type: "ok"})

		case "heartbeat":
			var payload HeartbeatPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				log.Printf("Invalid heartbeat payload: %v", err)
				continue
			}
			d.handleHeartbeat(payload)

		case "command":
			var payload CommandPayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				log.Printf("Invalid command payload: %v", err)
				encoder.Encode(Message{Type: "error", Payload: mustMarshal("invalid payload")})
				continue
			}
			result := d.handleCommand(payload)
			encoder.Encode(result)

		case "status":
			result := d.handleStatus()
			encoder.Encode(result)

		case "quit":
			return
		}
	}
}

// handleRegister 处理注册
func (d *Daemon) handleRegister(payload RegisterPayload) {
	d.servicesMu.Lock()
	defer d.servicesMu.Unlock()

	svc, exists := d.services[payload.Name]
	if !exists {
		svc = &Service{
			Name: payload.Name,
		}
		d.services[payload.Name] = svc
	}

	svc.mu.Lock()
	svc.PID = payload.PID
	svc.Running = true
	svc.Registered = true
	svc.LastBeat = time.Now()
	svc.Version = payload.Version
	svc.mu.Unlock()

	log.Printf("Service registered: %s (PID: %d, Version: %s)", payload.Name, payload.PID, payload.Version)
}

// handleHeartbeat 处理心跳
func (d *Daemon) handleHeartbeat(payload HeartbeatPayload) {
	d.servicesMu.RLock()
	svc, exists := d.services[payload.Name]
	d.servicesMu.RUnlock()

	if exists {
		svc.mu.Lock()
		svc.LastBeat = time.Now()
		svc.mu.Unlock()
	}
}

// handleCommand 处理命令
func (d *Daemon) handleCommand(payload CommandPayload) Message {
	switch payload.Command {
	case "start":
		err := d.startService(payload.Target)
		if err != nil {
			return Message{Type: "error", Payload: mustMarshal(err.Error())}
		}
		return Message{Type: "ok"}

	case "stop":
		err := d.stopService(payload.Target)
		if err != nil {
			return Message{Type: "error", Payload: mustMarshal(err.Error())}
		}
		return Message{Type: "ok"}

	case "restart":
		d.stopService(payload.Target)
		time.Sleep(500 * time.Millisecond)
		err := d.startService(payload.Target)
		if err != nil {
			return Message{Type: "error", Payload: mustMarshal(err.Error())}
		}
		return Message{Type: "ok"}

	default:
		return Message{Type: "error", Payload: mustMarshal("unknown command")}
	}
}

// handleStatus 处理状态查询
func (d *Daemon) handleStatus() Message {
	d.servicesMu.RLock()
	defer d.servicesMu.RUnlock()

	status := make(map[string]any)
	for name, svc := range d.services {
		svc.mu.Lock()
		status[name] = map[string]any{
			"running":    svc.Running,
			"pid":        svc.PID,
			"version":    svc.Version,
			"registered": svc.Registered,
		}
		svc.mu.Unlock()
	}

	return Message{Type: "status", Payload: mustMarshal(status)}
}

// startService 启动服务
func (d *Daemon) startService(name string) error {
	d.servicesMu.Lock()
	defer d.servicesMu.Unlock()

	svc, exists := d.services[name]
	if !exists {
		svc = &Service{
			Name: name,
		}
		d.services[name] = svc
	}

	svc.mu.Lock()
	defer svc.mu.Unlock()

	if svc.Running {
		return fmt.Errorf("%s is already running", name)
	}

	// 获取配置
	var binPath, configPath string
	switch name {
	case "server":
		binPath = d.config.Server.BinPath
		configPath = d.config.Server.ConfigPath
	case "client":
		binPath = d.config.Client.BinPath
		configPath = d.config.Client.ConfigPath
	default:
		return fmt.Errorf("unknown service: %s", name)
	}

	// 检查二进制是否存在
	if _, err := os.Stat(binPath); os.IsNotExist(err) {
		return fmt.Errorf("binary not found: %s", binPath)
	}

	// 检查配置文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("config not found: %s", configPath)
	}

	log.Printf("Starting %s: %s -config %s", name, binPath, configPath)

	// 构建命令
	cmd := exec.Command(binPath, "-config", configPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setSysProcAttr(cmd.SysProcAttr)

	// 启动
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", name, err)
	}

	svc.Cmd = cmd
	svc.PID = cmd.Process.Pid
	svc.Running = true

	log.Printf("Service started: %s (PID: %d)", name, svc.PID)

	// 监控进程
	go func() {
		err := cmd.Wait()
		d.servicesMu.Lock()
		svc.mu.Lock()
		svc.Running = false
		svc.Registered = false
		svc.PID = 0
		svc.Cmd = nil
		svc.mu.Unlock()
		d.servicesMu.Unlock()

		if err != nil {
			log.Printf("Service exited: %s, error: %v", name, err)
		} else {
			log.Printf("Service exited: %s", name)
		}
	}()

	return nil
}

// stopService 停止服务
func (d *Daemon) stopService(name string) error {
	d.servicesMu.Lock()
	svc, exists := d.services[name]
	d.servicesMu.Unlock()

	if !exists {
		return nil
	}

	svc.mu.Lock()
	defer svc.mu.Unlock()

	if !svc.Running || svc.Cmd == nil {
		return nil
	}

	// 发送 SIGTERM
	if err := svc.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
		svc.Cmd.Process.Signal(syscall.SIGKILL)
	}

	// 等待退出，最多 5 秒
	done := make(chan error, 1)
	go func() {
		done <- svc.Cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		svc.Cmd.Process.Signal(syscall.SIGKILL)
	}

	svc.Running = false
	svc.Registered = false
	svc.PID = 0
	svc.Cmd = nil

	log.Printf("Service stopped: %s", name)
	return nil
}

// getBaseDir 获取安装目录
func getBaseDir() string {
	if dir := os.Getenv("BBGRID_HOME"); dir != "" {
		return dir
	}
	execPath, err := os.Executable()
	if err != nil {
		return "/usr/local/bbgrid"
	}
	return filepath.Dir(filepath.Dir(execPath))
}

// mustMarshal 序列化 JSON
func mustMarshal(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
