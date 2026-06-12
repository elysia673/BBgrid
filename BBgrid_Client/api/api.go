// Package api 提供统一的 API 客户端。
package api

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client API 客户端
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// Config API 客户端配置
type Config struct {
	ServerURL string `json:"server_url"`
	Token     string `json:"token"`
	Insecure  bool   `json:"insecure"`
	Timeout   time.Duration
}

// New 创建 API 客户端
func New(config Config) *Client {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	// 转换 WebSocket URL 为 HTTP URL
	baseURL := config.ServerURL
	if strings.HasPrefix(baseURL, "wss://") {
		baseURL = "https://" + baseURL[6:]
	} else if strings.HasPrefix(baseURL, "ws://") {
		baseURL = "http://" + baseURL[5:]
	}
	baseURL = strings.TrimSuffix(baseURL, "/ws")

	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: config.Insecure,
				},
			},
			Timeout: timeout,
		},
		token: config.Token,
	}
}

// SetToken 设置认证 Token
func (c *Client) SetToken(token string) {
	c.token = token
}

// do 执行 HTTP 请求
func (c *Client) do(method, path string, body any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	url := c.baseURL + path
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("X-CLIENT-TOKEN", c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.httpClient.Do(req)
}

// Result API 响应结果
type Result struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data any    `json:"data,omitempty"`
}

// get 执行 GET 请求
func (c *Client) get(path string) (*Result, error) {
	resp, err := c.do("GET", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result Result
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if result.Code != 0 {
		return &result, fmt.Errorf("api error: %s", result.Msg)
	}

	return &result, nil
}

// post 执行 POST 请求
func (c *Client) post(path string, body any) (*Result, error) {
	resp, err := c.do("POST", path, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result Result
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if result.Code != 0 {
		return &result, fmt.Errorf("api error: %s", result.Msg)
	}

	return &result, nil
}

// delete 执行 DELETE 请求
func (c *Client) delete(path string) (*Result, error) {
	resp, err := c.do("DELETE", path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result Result
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if result.Code != 0 {
		return &result, fmt.Errorf("api error: %s", result.Msg)
	}

	return &result, nil
}

// Ping 健康检查
func (c *Client) Ping() error {
	resp, err := c.httpClient.Get(c.baseURL + "/PING")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Status 获取服务器状态
func (c *Client) Status() (map[string]any, error) {
	result, err := c.get("/status")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		return data, nil
	}
	return nil, nil
}

// Login 登录获取 JWT
func (c *Client) Login(apiKey string) (string, error) {
	result, err := c.post("/api/v1/auth/login", map[string]string{
		"api_key": apiKey,
	})
	if err != nil {
		return "", err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if token, ok := data["token"].(string); ok {
			return token, nil
		}
	}
	return "", fmt.Errorf("no token in response")
}

// RegisterApply 提交注册申请
func (c *Client) RegisterApply(clientID, publicKey, token string) error {
	_, err := c.post("/api/v1/register/apply", map[string]string{
		"client_id":  clientID,
		"public_key": publicKey,
		"token":      token,
	})
	return err
}

// RegisterList 获取已注册客户端列表
func (c *Client) RegisterList() ([]map[string]any, error) {
	result, err := c.get("/api/v1/register/list")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if clients, ok := data["clients"].([]any); ok {
			result := make([]map[string]any, 0, len(clients))
			for _, client := range clients {
				if m, ok := client.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// RegisterApprove 审核签发证书
func (c *Client) RegisterApprove(clientID, namespace, role string) error {
	_, err := c.post("/api/v1/register/approve", map[string]string{
		"client_id":  clientID,
		"namespace":  namespace,
		"role":       role,
	})
	return err
}

// RegisterRevoke 吊销证书
func (c *Client) RegisterRevoke(clientID string) error {
	_, err := c.post("/api/v1/register/revoke", map[string]string{
		"client_id": clientID,
	})
	return err
}

// RegisterPending 获取待审核列表
func (c *Client) RegisterPending() ([]map[string]any, error) {
	result, err := c.get("/api/v1/register/pending")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if clients, ok := data["clients"].([]any); ok {
			result := make([]map[string]any, 0, len(clients))
			for _, client := range clients {
				if m, ok := client.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// Node 获取所有客户端
func (c *Client) Nodes() ([]map[string]any, error) {
	result, err := c.get("/api/v1/nodes")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if clients, ok := data["clients"].([]any); ok {
			result := make([]map[string]any, 0, len(clients))
			for _, client := range clients {
				if m, ok := client.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// NodeView 获取客户端详情
func (c *Client) NodeView(clientID string) (map[string]any, error) {
	result, err := c.get("/api/v1/nodes/" + clientID)
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		return data, nil
	}
	return nil, nil
}

// Proxies 获取所有代理
func (c *Client) Proxies() ([]map[string]any, error) {
	result, err := c.get("/api/v1/proxies")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if proxies, ok := data["proxies"].([]any); ok {
			result := make([]map[string]any, 0, len(proxies))
			for _, proxy := range proxies {
				if m, ok := proxy.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// ProxyCreate 创建代理
func (c *Client) ProxyCreate(clientID string, remotePort, localPort int, localIP, protocol, bindAddr string) error {
	_, err := c.post("/api/v1/proxies", map[string]any{
		"client_id":   clientID,
		"remote_port": remotePort,
		"local_port":  localPort,
		"local_ip":    localIP,
		"protocol":    protocol,
		"bind_addr":   bindAddr,
	})
	return err
}

// ProxyDelete 删除代理
func (c *Client) ProxyDelete(port int) error {
	_, err := c.delete(fmt.Sprintf("/api/v1/proxies/%d", port))
	return err
}

// Relays 获取所有中继
func (c *Client) Relays() ([]map[string]any, error) {
	result, err := c.get("/api/v1/relay")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if relays, ok := data["relays"].([]any); ok {
			result := make([]map[string]any, 0, len(relays))
			for _, relay := range relays {
				if m, ok := relay.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// RelayCreate 创建中继
func (c *Client) RelayCreate(sourceClient, targetClient, protocol string, sourcePort, targetPort int, sourceLocalIP, targetLocalIP string) (string, error) {
	result, err := c.post("/api/v1/relay", map[string]any{
		"source_client":  sourceClient,
		"target_client":  targetClient,
		"protocol":       protocol,
		"source_port":    sourcePort,
		"target_port":    targetPort,
		"source_local_ip": sourceLocalIP,
		"target_local_ip": targetLocalIP,
	})
	if err != nil {
		return "", err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if sessionID, ok := data["session_id"].(string); ok {
			return sessionID, nil
		}
	}
	return "", nil
}

// RelayDelete 删除中继
func (c *Client) RelayDelete(sessionID string) error {
	_, err := c.delete("/api/v1/relay/" + sessionID)
	return err
}

// Namespaces 获取命名空间列表
func (c *Client) Namespaces() ([]map[string]any, error) {
	result, err := c.get("/api/v1/namespaces")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if namespaces, ok := data["namespaces"].([]any); ok {
			result := make([]map[string]any, 0, len(namespaces))
			for _, ns := range namespaces {
				if m, ok := ns.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// NamespaceInfo 获取命名空间详情
func (c *Client) NamespaceInfo(name string) (map[string]any, error) {
	result, err := c.get("/api/v1/namespaces/" + name)
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		return data, nil
	}
	return nil, nil
}

// NamespaceClients 获取命名空间客户端
func (c *Client) NamespaceClients(name string) ([]map[string]any, error) {
	result, err := c.get("/api/v1/namespaces/" + name + "/clients")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if clients, ok := data["clients"].([]any); ok {
			result := make([]map[string]any, 0, len(clients))
			for _, client := range clients {
				if m, ok := client.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// NamespaceAssign 分配命名空间
func (c *Client) NamespaceAssign(clientID, namespace, role string) error {
	_, err := c.post("/api/v1/namespaces/assign", map[string]string{
		"client_id": clientID,
		"namespace": namespace,
		"role":      role,
	})
	return err
}

// Capabilities 获取插件能力列表
func (c *Client) Capabilities() ([]map[string]any, error) {
	result, err := c.get("/runtime/capabilities")
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		if capabilities, ok := data["capabilities"].([]any); ok {
			result := make([]map[string]any, 0, len(capabilities))
			for _, cap := range capabilities {
				if m, ok := cap.(map[string]any); ok {
					result = append(result, m)
				}
			}
			return result, nil
		}
	}
	return nil, nil
}

// Call 执行插件操作
func (c *Client) Call(action string, params map[string]any) (*Result, error) {
	return c.post("/runtime/call?action="+action, params)
}

// Query 查询状态
func (c *Client) Query(resourceType, name string) (map[string]any, error) {
	path := "/runtime/query"
	if resourceType != "" {
		path += "?type=" + resourceType
		if name != "" {
			path += "&name=" + name
		}
	}
	result, err := c.get(path)
	if err != nil {
		return nil, err
	}
	if data, ok := result.Data.(map[string]any); ok {
		return data, nil
	}
	return nil, nil
}
