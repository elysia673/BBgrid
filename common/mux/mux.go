// Package mux 提供 TCP 连接多路复用
//
// SSH 风格包处理：长度前缀帧 + 窗口流控 + 通道状态机。
// 协议格式：[2B magic 0xAE01] [2B port] [2B len] [2B type] [data]
//
// 包类型（type 字段）：
//
//	0x0000 = DATA          数据帧
//	0x0001 = OPEN          打开通道
//	0x0002 = CLOSE         关闭通道
//	0x0003 = WINDOW_UPDATE  窗口更新（接收方消费后通知）
package mux

import (
	alog "BBgrid/common/log"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Magic           = 0xAE01
	FrameHeaderSize = 8 // 2B magic + 2B port + 2B len + 2B type
	MaxFrameSize    = 65535
	workerCount     = 4
	windowSize      = 1024 * 1024 // 1MB 流控窗口
	sendQueueSize   = 64
	ctrlQueueSize   = 32
	windowThreshold = 16 * 1024             // 累积 16KB 才发 WINDOW_UPDATE
	windowInterval  = 10 * time.Millisecond // 最长 10ms 发一次
)

const (
	PktData         uint16 = 0x0000
	PktOpen         uint16 = 0x0001
	PktClose        uint16 = 0x0002
	PktWindowUpdate uint16 = 0x0003
)

type frameOut struct {
	port  uint16
	data  []byte
	srcFd int
	n     int
	ctrl  bool
}

type frameIn struct {
	port  uint16
	ptype uint16
	data  []byte
}

// ctrlFrame 控制帧（不走 sendQueue，直接写）
type ctrlFrame struct {
	port  uint16
	ptype uint16
}

// Channel 虚拟通道
type Channel struct {
	Port      uint16
	DataChan  chan []byte
	CloseChan chan struct{}
	Mux       *Multiplexer
	closeOnce sync.Once

	sendQueue  chan frameOut
	sendWindow int32
	closing    bool // CloseChannel 标记，等 PktClose 发完再清理

	// WINDOW_UPDATE 累积
	pendingConsumed int32      // 累积待发送的消费量
	lastWUTime      time.Time  // 上次发送 WINDOW_UPDATE 的时间
	wuMu            sync.Mutex // 保护 pendingConsumed 和 lastWUTime

	// io.Copy 读缓冲
	reader     []byte // 上次 Read 剩余数据
	readerMu   sync.Mutex
}

// Multiplexer 多路复用器
type Multiplexer struct {
	conn         net.Conn
	connFdFile   *os.File
	channels     map[uint16]*Channel
	lock         sync.RWMutex
	writeLock    sync.Mutex
	workChan     chan frameIn
	ctrlChan     chan ctrlFrame // 控制帧队列
	closed       atomic.Bool
	closeOnce    sync.Once
	closeChan    chan struct{}
	writeNotify  chan struct{}
	spliceAvail  bool
	LocalTarget  string
	OnNewChannel func(ch *Channel)
}

func New(conn net.Conn) *Multiplexer {
	m := &Multiplexer{
		conn:        conn,
		channels:    make(map[uint16]*Channel),
		workChan:    make(chan frameIn, 64),
		ctrlChan:    make(chan ctrlFrame, ctrlQueueSize),
		closeChan:   make(chan struct{}),
		writeNotify: make(chan struct{}, 1),
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
		tcpConn.SetReadBuffer(256 * 1024)
		tcpConn.SetWriteBuffer(256 * 1024)

		if f, err := tcpConn.File(); err == nil {
			m.connFdFile = f
		}
	}

	m.spliceAvail = checkSpliceAvail(conn)

	go m.readLoop()
	go m.writeLoop()
	for i := 0; i < workerCount; i++ {
		go m.worker()
	}
	return m
}

func (m *Multiplexer) notifyWrite() {
	select {
	case m.writeNotify <- struct{}{}:
	default:
	}
}

// writeLoop 写入 TCP：优先处理控制帧，然后轮询数据帧
func (m *Multiplexer) writeLoop() {
	idleTimer := time.NewTimer(time.Second)
	defer idleTimer.Stop()

	for {
		if m.closed.Load() {
			return
		}

		// 1. 优先处理控制帧
		madeProgress := false
		select {
		case cf := <-m.ctrlChan:
			m.writeLock.Lock()
			err := m.writeCtrl(cf.port, cf.ptype)
			m.writeLock.Unlock()
			if err != nil {
				if !m.closed.Load() {
					alog.Error(alog.CatMux, "ctrl write error", "error", err)
				}
				m.Close()
				return
			}
			// PktClose 发完后清理 channel
			if cf.ptype == PktClose {
				m.closeChannelLocal(cf.port)
			}
			madeProgress = true
		default:
		}

		// 2. 处理数据帧
		m.lock.RLock()
		channels := make([]*Channel, 0, len(m.channels))
		for _, ch := range m.channels {
			channels = append(channels, ch)
		}
		m.lock.RUnlock()

		if len(channels) == 0 && !madeProgress {
			select {
			case <-m.writeNotify:
				continue
			case cf := <-m.ctrlChan:
				m.writeLock.Lock()
				err := m.writeCtrl(cf.port, cf.ptype)
				m.writeLock.Unlock()
				if err != nil {
					if !m.closed.Load() {
						alog.Error(alog.CatMux, "ctrl write error", "error", err)
					}
					m.Close()
					return
				}
				continue
			case <-m.closeChan:
				return
			case <-idleTimer.C:
				idleTimer.Reset(time.Second)
				continue
			}
		}

		for _, ch := range channels {
			select {
			case frame := <-ch.sendQueue:
				m.writeLock.Lock()
				var err error
				if frame.srcFd >= 0 {
					err = m.writeFrameSplice(frame.port, frame.srcFd, frame.n)
				} else {
					err = m.writeFrame(frame.port, frame.data, frame.ctrl)
				}
				m.writeLock.Unlock()
				if err != nil {
					if !m.closed.Load() {
						alog.Error(alog.CatMux, "mux write error", "error", err)
					}
					m.Close()
					return
				}
				madeProgress = true
			default:
			}
		}

		if !madeProgress {
			select {
			case <-m.writeNotify:
			case cf := <-m.ctrlChan:
				m.writeLock.Lock()
				err := m.writeCtrl(cf.port, cf.ptype)
				m.writeLock.Unlock()
				if err != nil {
					if !m.closed.Load() {
						alog.Error(alog.CatMux, "ctrl write error", "error", err)
					}
					m.Close()
					return
				}
			case <-m.closeChan:
				return
			}
		}
	}
}

// writeFrame 写入数据帧
func (m *Multiplexer) writeFrame(port uint16, data []byte, ctrl bool) error {
	dataLen := len(data)
	if dataLen > MaxFrameSize {
		return fmt.Errorf("data too large: %d", dataLen)
	}

	ptype := PktData
	if ctrl {
		ptype = PktOpen
	}

	buf := make([]byte, FrameHeaderSize+dataLen)
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	binary.BigEndian.PutUint16(buf[2:4], port)
	binary.BigEndian.PutUint16(buf[4:6], uint16(dataLen))
	binary.BigEndian.PutUint16(buf[6:8], ptype)
	copy(buf[FrameHeaderSize:], data)

	for written := 0; written < len(buf); {
		n, err := m.conn.Write(buf[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("zero write")
		}
		written += n
	}
	return nil
}

// writeCtrl 写入控制帧（已持有 writeLock）
func (m *Multiplexer) writeCtrl(port uint16, ptype uint16) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}

	frame := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint16(frame[0:2], Magic)
	binary.BigEndian.PutUint16(frame[2:4], port)
	binary.BigEndian.PutUint16(frame[4:6], 0)
	binary.BigEndian.PutUint16(frame[6:8], ptype)

	for written := 0; written < len(frame); {
		n, err := m.conn.Write(frame[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("zero write")
		}
		written += n
	}
	return nil
}

// writeFrameSplice splice 零拷贝写入（已持有 writeLock）
func (m *Multiplexer) writeFrameSplice(port uint16, srcFd int, n int) error {
	if n > MaxFrameSize {
		n = MaxFrameSize
	}

	pr, pw, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("create pipe: %w", err)
	}
	defer pr.Close()

	spliced, err := spliceFd(srcFd, int(pw.Fd()), n)
	if err != nil {
		pw.Close()
		return fmt.Errorf("splice src→pipe: %w", err)
	}
	if spliced == 0 {
		pw.Close()
		return fmt.Errorf("splice: EOF")
	}
	pw.Close()

	header := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint16(header[0:2], Magic)
	binary.BigEndian.PutUint16(header[2:4], port)
	binary.BigEndian.PutUint16(header[4:6], uint16(spliced))
	binary.BigEndian.PutUint16(header[6:8], PktData)

	if _, err := m.conn.Write(header); err != nil {
		return err
	}

	_, err = spliceFd(int(pr.Fd()), m.connFd(), int(spliced))
	return err
}

func (m *Multiplexer) connFd() int {
	if m.connFdFile != nil {
		return int(m.connFdFile.Fd())
	}
	return -1
}

func (m *Multiplexer) SpliceAvailable() bool {
	return m.spliceAvail
}

func (m *Multiplexer) readLoop() {
	defer func() {
		m.lock.Lock()
		for _, ch := range m.channels {
			ch.closeOnce.Do(func() { close(ch.CloseChan) })
		}
		m.channels = make(map[uint16]*Channel)
		m.lock.Unlock()
		m.Close()
	}()

	header := make([]byte, FrameHeaderSize)
	for {
		if _, err := io.ReadFull(m.conn, header); err != nil {
			if !m.closed.Load() {
				alog.Error(alog.CatMux, "read header error", "error", err)
			}
			return
		}

		magic := binary.BigEndian.Uint16(header[0:2])
		if magic != Magic {
			alog.Error(alog.CatMux, "invalid magic", "magic", fmt.Sprintf("0x%04x", magic))
			return
		}

		port := binary.BigEndian.Uint16(header[2:4])
		dataLen := binary.BigEndian.Uint16(header[4:6])
		ptype := binary.BigEndian.Uint16(header[6:8])

		if dataLen > MaxFrameSize {
			alog.Error(alog.CatMux, "frame too large", "size", dataLen)
			return
		}

		var data []byte
		if dataLen > 0 {
			data = make([]byte, dataLen)
			if _, err := io.ReadFull(m.conn, data); err != nil {
				alog.Error(alog.CatMux, "read data error", "error", err)
				return
			}
		}

		select {
		case m.workChan <- frameIn{port: port, ptype: ptype, data: data}:
		case <-m.closeChan:
			return
		}
	}
}

func (m *Multiplexer) worker() {
	for {
		select {
		case frame, ok := <-m.workChan:
			if !ok {
				return
			}
			m.dispatch(frame.port, frame.ptype, frame.data)
		case <-m.closeChan:
			return
		}
	}
}

func (m *Multiplexer) dispatch(port uint16, ptype uint16, data []byte) {
	switch ptype {
	case PktData:
		m.dispatchData(port, data)
	case PktClose:
		m.closeChannelLocal(port)
	case PktWindowUpdate:
		m.handleWindowUpdate(port, data)
	case PktOpen:
		// OPEN 帧由服务端处理
	default:
		alog.Warn(alog.CatMux, "unknown packet type", "type", fmt.Sprintf("0x%04x", ptype))
	}
}

func (m *Multiplexer) dispatchData(port uint16, data []byte) {
	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if ok {
		select {
		case ch.DataChan <- data:
		case <-m.closeChan:
		}
		return
	}

	// channel 不存在，尝试创建新 channel（relay 模式由 OnNewChannel 处理）
	if m.OnNewChannel == nil {
		alog.Warn(alog.CatMux, "dispatchData: no channel and no OnNewChannel", "port", port, "dataLen", len(data))
		return
	}

	m.lock.Lock()
	if _, exists := m.channels[port]; exists {
		m.lock.Unlock()
		m.dispatchData(port, data)
		return
	}
	ch = &Channel{
		Port:       port,
		DataChan:   make(chan []byte, 32),
		CloseChan:  make(chan struct{}),
		Mux:        m,
		sendQueue:  make(chan frameOut, sendQueueSize),
		sendWindow: windowSize,
		lastWUTime: time.Now(),
	}
	m.channels[port] = ch
	m.lock.Unlock()

	go m.OnNewChannel(ch)

	select {
	case ch.DataChan <- data:
	case <-ch.CloseChan:
	case <-m.closeChan:
	}
}

func (m *Multiplexer) handleWindowUpdate(port uint16, data []byte) {
	if len(data) < 4 {
		return
	}
	addWindow := int32(binary.BigEndian.Uint32(data[:4]))

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if ok {
		atomic.AddInt32(&ch.sendWindow, addWindow)
	}
}

func (m *Multiplexer) enqueueFrame(ch *Channel, frame frameOut) error {
	select {
	case ch.sendQueue <- frame:
		m.notifyWrite()
		return nil
	case <-ch.CloseChan:
		return fmt.Errorf("channel closed")
	case <-m.closeChan:
		return fmt.Errorf("multiplexer closed")
	}
}

// Send 发送数据到指定端口（带流控）
func (m *Multiplexer) Send(port uint16, data []byte) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if !ok {
		return fmt.Errorf("channel %d not found", port)
	}

	// 流控：等待窗口
	dataLen := int32(len(data))
	for atomic.LoadInt32(&ch.sendWindow) < dataLen {
		select {
		case <-ch.CloseChan:
			return fmt.Errorf("channel closed")
		case <-m.closeChan:
			return fmt.Errorf("multiplexer closed")
		default:
			time.Sleep(100 * time.Microsecond)
		}
	}

	atomic.AddInt32(&ch.sendWindow, -dataLen)

	buf := make([]byte, len(data))
	copy(buf, data)

	return m.enqueueFrame(ch, frameOut{port: port, data: buf, srcFd: -1})
}

// SpliceSend 零拷贝发送
func (m *Multiplexer) SpliceSend(port uint16, srcFd int, n int) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}
	if !m.spliceAvail {
		return fmt.Errorf("splice not available")
	}
	if n > MaxFrameSize {
		n = MaxFrameSize
	}

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if !ok {
		return fmt.Errorf("channel %d not found", port)
	}

	n32 := int32(n)
	for atomic.LoadInt32(&ch.sendWindow) < n32 {
		select {
		case <-ch.CloseChan:
			return fmt.Errorf("channel closed")
		case <-m.closeChan:
			return fmt.Errorf("multiplexer closed")
		default:
			time.Sleep(100 * time.Microsecond)
		}
	}

	atomic.AddInt32(&ch.sendWindow, -n32)

	return m.enqueueFrame(ch, frameOut{port: port, data: nil, srcFd: srcFd, n: n})
}

// enqueueCtrl 入队控制帧（阻塞等待，确保不丢帧）
func (m *Multiplexer) enqueueCtrl(port uint16, ptype uint16) {
	if m.closed.Load() {
		return
	}
	select {
	case m.ctrlChan <- ctrlFrame{port: port, ptype: ptype}:
		m.notifyWrite()
	case <-m.closeChan:
	}
}

// OpenChannel 打开通道并发送 OPEN 帧
func (m *Multiplexer) OpenChannel(port uint16) (*Channel, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.closed.Load() {
		return nil, fmt.Errorf("multiplexer closed")
	}
	if ch, exists := m.channels[port]; exists {
		return ch, nil
	}
	ch := &Channel{
		Port:       port,
		DataChan:   make(chan []byte, 32),
		CloseChan:  make(chan struct{}),
		Mux:        m,
		sendQueue:  make(chan frameOut, sendQueueSize),
		sendWindow: windowSize,
		lastWUTime: time.Now(),
	}
	m.channels[port] = ch

	go m.enqueueCtrl(port, PktOpen)

	return ch, nil
}

// CloseChannel 关闭通道：只发 PktClose，不删 map，writeLoop 发完后再清理
func (m *Multiplexer) CloseChannel(port uint16) {
	if m.closed.Load() {
		return
	}
	m.enqueueCtrl(port, PktClose)
}

func (m *Multiplexer) closeChannelLocal(port uint16) {
	m.lock.Lock()
	ch, ok := m.channels[port]
	if ok {
		delete(m.channels, port)
	}
	m.lock.Unlock()

	if ok {
		ch.closeOnce.Do(func() {
			// 排空 sendQueue，避免丢失已入队的帧
			for {
				select {
				case <-ch.sendQueue:
				default:
					goto done
				}
			}
		done:
			close(ch.CloseChan)
		})
	}
}

// SendWindowUpdate 累积式窗口更新：超过阈值或超时才发送
func (m *Multiplexer) SendWindowUpdate(port uint16, consumed int) error {
	if m.closed.Load() {
		return fmt.Errorf("multiplexer closed")
	}

	m.lock.RLock()
	ch, ok := m.channels[port]
	m.lock.RUnlock()

	if !ok {
		return nil
	}

	ch.wuMu.Lock()
	ch.pendingConsumed += int32(consumed)
	shouldSend := ch.pendingConsumed >= windowThreshold ||
		time.Since(ch.lastWUTime) >= windowInterval
	if shouldSend {
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(ch.pendingConsumed))
		ch.pendingConsumed = 0
		ch.lastWUTime = time.Now()
		ch.wuMu.Unlock()

		// 通过 ctrlChan 发送
		if !m.closed.Load() {
			// WINDOW_UPDATE 带数据，需要特殊处理
			m.writeLock.Lock()
			err := m.writeWindowUpdate(port, data)
			m.writeLock.Unlock()
			return err
		}
		return nil
	}
	ch.wuMu.Unlock()
	return nil
}

// writeWindowUpdate 写入 WINDOW_UPDATE 帧（已持有 writeLock）
func (m *Multiplexer) writeWindowUpdate(port uint16, data []byte) error {
	buf := make([]byte, FrameHeaderSize+4)
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	binary.BigEndian.PutUint16(buf[2:4], port)
	binary.BigEndian.PutUint16(buf[4:6], 4)
	binary.BigEndian.PutUint16(buf[6:8], PktWindowUpdate)
	copy(buf[FrameHeaderSize:], data)

	for written := 0; written < len(buf); {
		n, err := m.conn.Write(buf[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("zero write")
		}
		written += n
	}
	return nil
}

func (m *Multiplexer) Close() {
	m.closeOnce.Do(func() {
		t0 := time.Now()
		m.closed.Store(true)
		close(m.closeChan)
		t1 := time.Now()
		m.conn.Close()
		if m.connFdFile != nil {
			m.connFdFile.Close()
		}
		alog.Info(alog.CatMux, "mux.Close done", "total", time.Since(t0), "elapsedSinceCloseChan", time.Since(t1))
	})
}

func (m *Multiplexer) Done() <-chan struct{} {
	return m.closeChan
}

func (m *Multiplexer) GetConn() net.Conn {
	return m.conn
}

func (ch *Channel) Receive() ([]byte, bool) {
	select {
	case data := <-ch.DataChan:
		return data, true
	case <-ch.CloseChan:
		return nil, false
	default:
		return nil, false
	}
}

func (ch *Channel) ReceiveBlocking() ([]byte, bool) {
	select {
	case data := <-ch.DataChan:
		return data, true
	case <-ch.CloseChan:
		for {
			select {
			case data := <-ch.DataChan:
				return data, true
			default:
				return nil, false
			}
		}
	}
}

// Read 实现 io.Reader，供 io.Copy 使用
func (ch *Channel) Read(p []byte) (int, error) {
	ch.readerMu.Lock()
	// 有上次剩余数据，先消费
	if len(ch.reader) > 0 {
		n := copy(p, ch.reader)
		ch.reader = ch.reader[n:]
		if len(ch.reader) == 0 {
			ch.reader = nil
		}
		ch.readerMu.Unlock()
		return n, nil
	}
	ch.readerMu.Unlock()

	data, ok := ch.ReceiveBlocking()
	if !ok {
		return 0, io.EOF
	}

	n := copy(p, data)
	if n < len(data) {
		ch.readerMu.Lock()
		ch.reader = data[n:]
		ch.readerMu.Unlock()
	} else {
		// 整条消息已消费完毕，通知 sender 有空间
		// 只在消息完整消费后才发 window update，避免 sender 过快灌数据
		ch.Mux.SendWindowUpdate(ch.Port, len(data))
	}
	return n, nil
}

// Write 实现 io.Writer，供 io.Copy 使用
func (ch *Channel) Write(p []byte) (int, error) {
	data := make([]byte, len(p))
	copy(data, p)
	if err := ch.Mux.Send(ch.Port, data); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (m *Multiplexer) HandleChannel(ch *Channel) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatTunnel, "PANIC in HandleChannel", "port", ch.Port, "error", r)
		}
		m.CloseChannel(ch.Port)
	}()

	localConn, err := net.Dial("tcp", m.LocalTarget)
	if err != nil {
		alog.Error(alog.CatTunnel, "failed to connect to local", "port", ch.Port, "target", m.LocalTarget, "error", err)
		return
	}

	alog.Info(alog.CatTunnel, "connected to local", "port", ch.Port, "target", m.LocalTarget)

	var wg sync.WaitGroup
	wg.Add(2)

	// 隧道 → 本地
	go func() {
		defer wg.Done()
		io.Copy(localConn, ch)
		if tc, ok := localConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	// 本地 → 隧道
	go func() {
		defer wg.Done()
		io.Copy(ch, localConn)
		m.CloseChannel(ch.Port)
	}()

	wg.Wait()
	localConn.Close()
}
