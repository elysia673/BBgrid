// Package log 提供结构化日志，自动脱敏敏感字段。
//
// 日志格式：datetime [LEVEL] pid file:line message key=value ...
// 示例：  2026-05-17 15:38:19 [INFO] 2952130 server.go:123 client connected client_id=abc123 remote=1.2.3.4:5678
//
// JSON 流格式：{"time":"2026-05-17T15:38:19Z","level":"INFO","pid":2952130,"file":"server.go","line":123,"msg":"client connected","fields":{...}}
//
// 脱敏规则：
//   - token, api_key, secret, password 等字段值替换为 "***"
//   - URL 中的 token 参数替换为 token=***
package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Level 日志级别
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

var levelNames = map[Level]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
	FATAL: "FATAL",
}

// ANSI 颜色
var levelColors = map[Level]string{
	DEBUG: "\033[36m", // cyan
	INFO:  "\033[32m", // green
	WARN:  "\033[33m", // yellow
	ERROR: "\033[31m", // red
	FATAL: "\033[35m", // magenta
}

const resetColor = "\033[0m"

// Category 日志分类
type Category string

const (
	CatAuth   Category = "AUTH"
	CatProxy  Category = "PROXY"
	CatTunnel Category = "TUNNEL"
	CatRelay  Category = "RELAY"
	CatMux    Category = "MUX"
	CatClient Category = "CLIENT"
	CatServer Category = "SERVER"
	CatConfig Category = "CONFIG"
	CatUpdate Category = "UPDATE"
	CatSystem Category = "SYSTEM"
	CatWS     Category = "WS"
)

// Format 日志输出格式
type Format int

const (
	FormatText Format = iota
	FormatJSON
)

// sensitiveKeys 需要脱敏的字段名（小写匹配）
var sensitiveKeys = []string{
	"token", "api_key", "apikey", "secret", "password",
	"passwd", "credential", "auth", "private_key", "privatekey",
}

var sensitiveURLRe = regexp.MustCompile(`(token|api_key|apikey|secret)=([^&\s]+)`)

// jsonEntry JSON 流日志条目
type jsonEntry struct {
	Time   string            `json:"time"`
	Level  string            `json:"level"`
	PID    int               `json:"pid"`
	File   string            `json:"file"`
	Line   int               `json:"line"`
	Msg    string            `json:"msg"`
	Fields map[string]string `json:"fields,omitempty"`
}

// Logger 结构化日志器
type Logger struct {
	mu      sync.Mutex
	level   Level
	format  Format
	output  io.Writer
	file    *os.File
	buf     *bufio.Writer
	flushMu sync.Mutex
	pid     int
	flushStop chan struct{} // 停止 flush goroutine
}

var defaultLogger = New(os.Stderr, INFO, FormatText)

// New 创建日志器
func New(out io.Writer, level Level, format Format) *Logger {
	return &Logger{
		level:  level,
		format: format,
		output: out,
		pid:    os.Getpid(),
	}
}

// SetLevel 设置日志级别
func SetLevel(level Level) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.level = level
}

// SetFormat 设置日志格式
func SetFormat(format Format) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.format = format
}

// SetOutput 设置输出
func SetOutput(w io.Writer) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.output = w
}

// SetFile 设置日志文件，同时输出到 stderr 和文件
func SetFile(path string) error {
	return defaultLogger.SetFile(path)
}

// Flush 将缓冲区写入文件
func Flush() {
	defaultLogger.Flush()
}

func (l *Logger) SetFile(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 停止旧的 flush goroutine
	if l.flushStop != nil {
		close(l.flushStop)
		l.flushStop = nil
	}

	if l.file != nil {
		if l.buf != nil {
			l.buf.Flush()
		}
		l.file.Close()
		l.file = nil
		l.buf = nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create log dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}

	l.file = f
	l.buf = bufio.NewWriterSize(f, 64*1024)

	mw := io.MultiWriter(l.output, l.buf)
	l.output = mw

	// 启动新的 flush goroutine，带 stop 信号
	l.flushStop = make(chan struct{})
	stopCh := l.flushStop
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				l.flushMu.Lock()
				if l.buf != nil {
					l.buf.Flush()
				}
				l.flushMu.Unlock()
			case <-stopCh:
				return
			}
		}
	}()

	return nil
}

func (l *Logger) Flush() {
	l.flushMu.Lock()
	defer l.flushMu.Unlock()
	if l.buf != nil {
		l.buf.Flush()
	}
}

// With 返回带上下文的日志条目
func With(fields ...interface{}) *Entry {
	return &Entry{
		logger: defaultLogger,
		fields: parseFields(fields),
	}
}

// Debug 调试日志
func Debug(cat Category, msg string, fields ...interface{}) {
	defaultLogger.log(DEBUG, cat, msg, fields)
}

// Info 信息日志
func Info(cat Category, msg string, fields ...interface{}) {
	defaultLogger.log(INFO, cat, msg, fields)
}

// Warn 警告日志
func Warn(cat Category, msg string, fields ...interface{}) {
	defaultLogger.log(WARN, cat, msg, fields)
}

// Error 错误日志
func Error(cat Category, msg string, fields ...interface{}) {
	defaultLogger.log(ERROR, cat, msg, fields)
}

// Fatal 致命错误日志并退出
func Fatal(cat Category, msg string, fields ...interface{}) {
	defaultLogger.log(FATAL, cat, msg, fields)
	os.Exit(1)
}

// Debugf 格式化调试日志
func Debugf(cat Category, format string, args ...interface{}) {
	defaultLogger.log(DEBUG, cat, fmt.Sprintf(format, args...), nil)
}

// Infof 格式化信息日志
func Infof(cat Category, format string, args ...interface{}) {
	defaultLogger.log(INFO, cat, fmt.Sprintf(format, args...), nil)
}

// Warnf 格式化警告日志
func Warnf(cat Category, format string, args ...interface{}) {
	defaultLogger.log(WARN, cat, fmt.Sprintf(format, args...), nil)
}

// Errorf 格式化错误日志
func Errorf(cat Category, format string, args ...interface{}) {
	defaultLogger.log(ERROR, cat, fmt.Sprintf(format, args...), nil)
}

// Fatalf 格式化致命日志
func Fatalf(cat Category, format string, args ...interface{}) {
	defaultLogger.log(FATAL, cat, fmt.Sprintf(format, args...), nil)
	os.Exit(1)
}

func (l *Logger) log(level Level, cat Category, msg string, fields []interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if level < l.level {
		return
	}

	pairs := parseFields(fields)
	sanitized := sanitize(pairs)

	// 获取调用者信息
	file, line := caller(3)

	switch l.format {
	case FormatJSON:
		l.logJSON(level, cat, msg, file, line, sanitized)
	default:
		l.logText(level, cat, msg, file, line, sanitized)
	}
}

func (l *Logger) logText(level Level, cat Category, msg string, file string, line int, sanitized map[string]string) {
	now := time.Now().Format("2006-01-02 15:04:05")
	color := levelColors[level]
	name := levelNames[level]

	var sb strings.Builder
	sb.WriteString(now)
	sb.WriteString(" ")
	sb.WriteString(color)
	sb.WriteString("[")
	sb.WriteString(name)
	sb.WriteString("]")
	sb.WriteString(resetColor)
	sb.WriteString(" ")
	sb.WriteString(fmt.Sprintf("%d", l.pid))
	sb.WriteString(" ")
	sb.WriteString(file)
	sb.WriteString(":")
	sb.WriteString(fmt.Sprintf("%d", line))
	sb.WriteString(" ")
	sb.WriteString(msg)

	if len(sanitized) > 0 {
		keys := make([]string, 0, len(sanitized))
		for k := range sanitized {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			sb.WriteString(" ")
			sb.WriteString(k)
			sb.WriteString("=")
			sb.WriteString(sanitized[k])
		}
	}

	l.output.Write([]byte(sb.String() + "\n"))
}

func (l *Logger) logJSON(level Level, cat Category, msg string, file string, line int, sanitized map[string]string) {
	entry := jsonEntry{
		Time:  time.Now().Format(time.RFC3339Nano),
		Level: levelNames[level],
		PID:   l.pid,
		File:  file,
		Line:  line,
		Msg:   msg,
	}
	if len(sanitized) > 0 {
		entry.Fields = sanitized
	}
	data, err := json.Marshal(entry)
	if err != nil {
		l.output.Write([]byte(fmt.Sprintf(`{"level":"ERROR","msg":"json marshal failed: %s"}`+"\n", err.Error())))
		return
	}
	l.output.Write(append(data, '\n'))
}

// caller 获取调用者文件名和行号
func caller(skip int) (string, int) {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "???", 0
	}
	return filepath.Base(file), line
}

// Entry 带上下文的日志条目
type Entry struct {
	logger *Logger
	fields map[string]string
}

func (e *Entry) With(fields ...interface{}) *Entry {
	newFields := make(map[string]string)
	for k, v := range e.fields {
		newFields[k] = v
	}
	for k, v := range parseFields(fields) {
		newFields[k] = v
	}
	return &Entry{logger: e.logger, fields: newFields}
}

func (e *Entry) Debug(cat Category, msg string, fields ...interface{}) {
	e.logger.log(DEBUG, cat, msg, mergeFields(e.fields, fields))
}

func (e *Entry) Info(cat Category, msg string, fields ...interface{}) {
	e.logger.log(INFO, cat, msg, mergeFields(e.fields, fields))
}

func (e *Entry) Warn(cat Category, msg string, fields ...interface{}) {
	e.logger.log(WARN, cat, msg, mergeFields(e.fields, fields))
}

func (e *Entry) Error(cat Category, msg string, fields ...interface{}) {
	e.logger.log(ERROR, cat, msg, mergeFields(e.fields, fields))
}

func (e *Entry) Fatal(cat Category, msg string, fields ...interface{}) {
	e.logger.log(FATAL, cat, msg, mergeFields(e.fields, fields))
	os.Exit(1)
}

func mergeFields(base map[string]string, extra []interface{}) []interface{} {
	result := make([]interface{}, 0, len(base)*2+len(extra))
	for k, v := range base {
		result = append(result, k, v)
	}
	result = append(result, extra...)
	return result
}

func parseFields(fields []interface{}) map[string]string {
	m := make(map[string]string, len(fields)/2)
	for i := 0; i < len(fields)-1; i += 2 {
		key := fmt.Sprintf("%v", fields[i])
		val := fmt.Sprintf("%v", fields[i+1])
		m[key] = val
	}
	if len(fields)%2 == 1 {
		m["_extra"] = fmt.Sprintf("%v", fields[len(fields)-1])
	}
	return m
}

func sanitize(pairs map[string]string) map[string]string {
	result := make(map[string]string, len(pairs))
	for k, v := range pairs {
		lower := strings.ToLower(k)
		isSensitive := false
		for _, sk := range sensitiveKeys {
			if strings.Contains(lower, sk) {
				isSensitive = true
				break
			}
		}
		if isSensitive {
			result[k] = "***"
		} else {
			result[k] = sanitizeURL(v)
		}
	}
	return result
}

func sanitizeURL(s string) string {
	if !strings.Contains(s, "://") {
		return s
	}
	return sensitiveURLRe.ReplaceAllString(s, "${1}=***")
}

func Mask(s string) string {
	if len(s) <= 3 {
		return "***"
	}
	return s[:3] + "***"
}

func MaskToken(s string) string {
	if len(s) <= 10 {
		return "***"
	}
	return s[:6] + "***"
}
