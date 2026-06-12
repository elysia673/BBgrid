// Package file 文件传输插件
//
// 通过 Runtime 接口传输文件：
// - file.push: multipart/form-data 上传
// - file.pull: HTTP 下载
// - file.list: 列出文件
package file

import (
	"BBgrid/BBgrid_Server/runtime"
	alog "BBgrid/common/log"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	// 注册到旧的 plugin registry (过渡用)
}

type FilePlugin struct {
	core    *runtime.Core
	config  map[string]any
	dataDir string
	stopCh  chan struct{}
}

func New() *FilePlugin {
	return &FilePlugin{
		stopCh: make(chan struct{}),
	}
}

func (p *FilePlugin) Name() string    { return "file" }
func (p *FilePlugin) Version() string { return "6.0.0" }

func (p *FilePlugin) Init(core *runtime.Core, config map[string]any) error {
	p.core = core
	p.config = config

	if dir, ok := config["data_dir"].(string); ok && dir != "" {
		p.dataDir = dir
	} else {
		p.dataDir = "data"
	}

	// 注册 Action Schema
	schema := p.Schema()
	for _, s := range schema {
		core.Capability().Register(runtime.Capability{
			Name:        s.Name,
			Description: s.Description,
			Source:      runtime.SourceInternal,
			Schema:      s,
		}, p.handleAction)
	}

	alog.Info(alog.CatSystem, "file 插件初始化完成", "data_dir", p.dataDir)
	return nil
}

func (p *FilePlugin) Run() error {
	<-p.stopCh
	return nil
}

func (p *FilePlugin) Stop() { close(p.stopCh) }

func (p *FilePlugin) Schema() []runtime.ActionSchema {
	return []runtime.ActionSchema{
		{
			Name:        "file.push",
			Description: "上传文件",
			Params: []runtime.ParamSchema{
				{Name: "path", Type: "string", Required: true},
			},
		},
		{
			Name:        "file.pull",
			Description: "下载文件",
			Params: []runtime.ParamSchema{
				{Name: "path", Type: "string", Required: true},
			},
		},
		{
			Name:        "file.list",
			Description: "列出文件",
			Params: []runtime.ParamSchema{
				{Name: "path", Type: "string", Required: false},
			},
		},
	}
}

func (p *FilePlugin) handleAction(ctx *runtime.ActionContext) (*runtime.ActionResult, error) {
	switch ctx.Action {
	case "file.push":
		return p.handlePush(ctx)
	case "file.pull":
		return p.handlePull(ctx)
	case "file.list":
		dir := p.dataDir
		if sub, ok := ctx.Params["path"].(string); ok && sub != "" {
			dir = filepath.Join(dir, sub)
		}
		return p.listFiles(dir)

	default:
		return &runtime.ActionResult{Code: 404, Msg: "unknown action"}, nil
	}
}

func (p *FilePlugin) handlePush(ctx *runtime.ActionContext) (*runtime.ActionResult, error) {
	// 获取文件名
	filename, _ := ctx.Params["filename"].(string)
	if filename == "" {
		return &runtime.ActionResult{Code: 400, Msg: "missing filename"}, nil
	}

	// 获取客户端ID和文件类型
	clientID, _ := ctx.Params["client_id"].(string)
	fileType, _ := ctx.Params["type"].(string)

	// 构建保存路径: data/{client_id}/{type}s/{filename}
	savePath := filename
	if clientID != "" {
		if fileType != "" {
			savePath = filepath.Join(clientID, fileType+"s", filename)
		} else {
			savePath = filepath.Join(clientID, filename)
		}
	}

	// 获取文件内容
	file, ok := ctx.Params["file"].(io.Reader)
	if !ok {
		return &runtime.ActionResult{Code: 400, Msg: "missing file content"}, nil
	}

	// 保存文件
	if err := p.SaveFile(savePath, file); err != nil {
		return &runtime.ActionResult{Code: 500, Msg: "failed to save file: " + err.Error()}, nil
	}

	alog.Info(alog.CatSystem, "文件上传成功", "filename", filename, "path", savePath)
	return &runtime.ActionResult{
		Code: 200,
		Msg:  "file uploaded successfully",
		Data: map[string]any{"filename": filename, "path": savePath},
	}, nil
}

func (p *FilePlugin) handlePull(ctx *runtime.ActionContext) (*runtime.ActionResult, error) {
	filename, _ := ctx.Params["path"].(string)
	if filename == "" {
		return &runtime.ActionResult{Code: 400, Msg: "missing path parameter"}, nil
	}

	fullPath := filepath.Join(p.dataDir, filename)
	file, err := os.Open(fullPath)
	if err != nil {
		return &runtime.ActionResult{Code: 404, Msg: "file not found"}, nil
	}
	stat, _ := file.Stat()
	return &runtime.ActionResult{
		Code:     200,
		Body:     file,
		BodyName: filepath.Base(filename),
		BodySize: stat.Size(),
	}, nil
}

func (p *FilePlugin) listFiles(dir string) (*runtime.ActionResult, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return &runtime.ActionResult{Code: 500, Msg: err.Error()}, nil
	}

	files := walkEntries(dir, "", entries)
	return &runtime.ActionResult{
		Code: 200,
		Data: map[string]any{"files": files},
	}, nil
}

func walkEntries(baseDir, prefix string, entries []os.DirEntry) []map[string]any {
	var result []map[string]any
	for _, entry := range entries {
		relPath := prefix + entry.Name()
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if entry.IsDir() {
			subEntries, _ := os.ReadDir(filepath.Join(baseDir, relPath))
			result = append(result, map[string]any{
				"path":    relPath + "/",
				"is_dir":  true,
				"size":    info.Size(),
				"modTime": info.ModTime().Format("2006-01-02 15:04:05"),
			})
			result = append(result, walkEntries(baseDir, relPath+"/", subEntries)...)
		} else {
			result = append(result, map[string]any{
				"path":    relPath,
				"is_dir":  false,
				"size":    info.Size(),
				"modTime": info.ModTime().Format("2006-01-02 15:04:05"),
			})
		}
	}
	return result
}

func (p *FilePlugin) SaveFile(path string, reader io.Reader) error {
	fullPath := filepath.Join(p.dataDir, path)
	// 防止路径穿越：解析后必须在 dataDir 内
	absDataDir, _ := filepath.Abs(p.dataDir)
	absFullPath, _ := filepath.Abs(fullPath)
	if !strings.HasPrefix(absFullPath, absDataDir+string(filepath.Separator)) {
		return fmt.Errorf("path traversal detected: %s", path)
	}
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, reader)
	return err
}

func (p *FilePlugin) LoadFile(path string) (io.ReadCloser, error) {
	fullPath, err := p.safePath(path)
	if err != nil {
		return nil, err
	}
	return os.Open(fullPath)
}

func (p *FilePlugin) DeleteFile(path string) error {
	fullPath, err := p.safePath(path)
	if err != nil {
		return err
	}
	return os.Remove(fullPath)
}

func (p *FilePlugin) GetDataDir() string {
	return p.dataDir
}

func (p *FilePlugin) GetFilePath(subPath string) string {
	return filepath.Join(p.dataDir, subPath)
}

func (p *FilePlugin) FileExists(path string) bool {
	fullPath, err := p.safePath(path)
	if err != nil {
		return false
	}
	_, err = os.Stat(fullPath)
	return err == nil
}

func (p *FilePlugin) MkdirAll(path string) error {
	fullPath := filepath.Join(p.dataDir, path)
	return os.MkdirAll(fullPath, 0755)
}

func (p *FilePlugin) ReadFile(path string) ([]byte, error) {
	fullPath := filepath.Join(p.dataDir, path)
	return os.ReadFile(fullPath)
}

func (p *FilePlugin) WriteFile(path string, data []byte) error {
	fullPath, err := p.safePath(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(fullPath, data, 0644)
}

// safePath 校验路径不逃逸出 dataDir
func (p *FilePlugin) safePath(subPath string) (string, error) {
	fullPath := filepath.Join(p.dataDir, subPath)
	absDataDir, _ := filepath.Abs(p.dataDir)
	absFullPath, _ := filepath.Abs(fullPath)
	if !strings.HasPrefix(absFullPath, absDataDir+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal detected: %s", subPath)
	}
	return fullPath, nil
}
