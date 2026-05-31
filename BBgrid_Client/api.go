package main

import (
	alog "BBgrid/common/log"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"encoding/json"
)

// FileAPIServer 文件 API 服务器
type FileAPIServer struct {
	port     int
	server   *http.Server
	stopCh   chan struct{}
	clientID string
	dataDir  string

	serverURL  string
	apiKey     string
	httpClient *http.Client
}

func NewFileAPIServer(port int, serverURL, clientID, dataDir, apiKey string) *FileAPIServer {
	return &FileAPIServer{
		port:       port,
		stopCh:     make(chan struct{}),
		clientID:   clientID,
		dataDir:    dataDir,
		serverURL:  serverURL,
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 5 * time.Minute},
	}
}

func isSafeFilename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return true
}

func (s *FileAPIServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/files", s.handleFiles)
	mux.HandleFunc("/api/v1/files/pull", s.handlePullFromServer)

	s.server = &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", s.port),
		Handler: mux,
	}

	alog.Info(alog.CatSystem, "File API server starting", "addr", s.server.Addr)

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			alog.Error(alog.CatSystem, "File API server error", "error", err)
		}
	}()

	return nil
}

func (s *FileAPIServer) Stop() {
	close(s.stopCh)
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(ctx)
	}
}

func (s *FileAPIServer) handleFiles(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.uploadFile(w, r)
	case http.MethodGet:
		s.listLocalFiles(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// uploadFile 接收外部服务的文件，转发到 Server
func (s *FileAPIServer) uploadFile(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(256 << 20); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "File required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileType := r.FormValue("type")
	if fileType == "" {
		fileType = "file"
	}
	if fileType != "file" && fileType != "log" {
		http.Error(w, "Invalid type, must be 'file' or 'log'", http.StatusBadRequest)
		return
	}

	filename := header.Filename
	if !isSafeFilename(filename) {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	// 转发到 Server
	if err := s.pushToServer(filename, fileType, file); err != nil {
		alog.Error(alog.CatSystem, "Push to server failed", "error", err)
		http.Error(w, "Server unavailable: "+err.Error(), http.StatusBadGateway)
		return
	}

	alog.Info(alog.CatSystem, "File pushed", "name", filename, "type", fileType)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"name": filename,
		"type": fileType,
	})
}

// pushToServer 将文件转发到 Server 的 /runtime/call
func (s *FileAPIServer) pushToServer(filename, fileType string, file io.Reader) error {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// 添加文件
	part, err := writer.CreateFormFile("file", filename)
	if err != nil {
		return fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return fmt.Errorf("copy file: %w", err)
	}
	writer.Close()

	// 发送到 Server /runtime/call
	apiURL := fmt.Sprintf("%s/api/v1/runtime/call?action=file.push&client_id=%s&type=%s",
		s.serverURL, s.clientID, fileType)

	req, err := http.NewRequest("POST", apiURL, &body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if s.apiKey != "" {
		req.Header.Set("X-CLIENT-TOKEN", s.apiKey)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// handlePullFromServer 从 Server 拉取文件
func (s *FileAPIServer) handlePullFromServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	filename := r.URL.Query().Get("filename")
	fileType := r.URL.Query().Get("type")
	if fileType == "" {
		fileType = "file"
	}

	if filename == "" {
		http.Error(w, "filename required", http.StatusBadRequest)
		return
	}

	// 从 Server 下载（使用流式下载端点）
	apiURL := fmt.Sprintf("%s/api/v1/runtime/download?client_id=%s&filename=%s&type=%s",
		s.serverURL, s.clientID, filename, fileType)

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		http.Error(w, "Server unavailable: "+err.Error(), http.StatusBadGateway)
		return
	}
	if s.apiKey != "" {
		req.Header.Set("X-CLIENT-TOKEN", s.apiKey)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		http.Error(w, "Server unavailable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		http.Error(w, fmt.Sprintf("Server returned %d: %s", resp.StatusCode, string(respBody)), resp.StatusCode)
		return
	}

	// 保存到本地
	saveDir := filepath.Join(s.dataDir, "received")
	if err := os.MkdirAll(saveDir, 0755); err != nil {
		http.Error(w, "Failed to create directory", http.StatusInternalServerError)
		return
	}

	filePath := filepath.Join(saveDir, filename)
	dst, err := os.Create(filePath)
	if err != nil {
		http.Error(w, "Failed to create file", http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	written, _ := io.Copy(dst, resp.Body)
	alog.Info(alog.CatSystem, "File pulled from server", "name", filename, "size", written)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"name": filename,
		"type": fileType,
		"size": written,
		"path": filePath,
	})
}

// listLocalFiles 列出本地文件
func (s *FileAPIServer) listLocalFiles(w http.ResponseWriter, r *http.Request) {
	fileType := r.URL.Query().Get("type")
	if fileType == "" {
		fileType = "file"
	}

	var dir string
	if fileType == "log" {
		dir = filepath.Join(s.dataDir, "logs")
	} else {
		dir = filepath.Join(s.dataDir, "received")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": []string{}})
		return
	}

	var files []map[string]any
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, map[string]any{
			"name":    entry.Name(),
			"size":    info.Size(),
			"modTime": info.ModTime().Unix(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": files})
}
