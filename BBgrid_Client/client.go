package main

import (
	"BBgrid/BBgrid_Client/conn"
	"BBgrid/BBgrid_Client/handler"
	"BBgrid/BBgrid_Client/register"
	alog "BBgrid/common/log"
	"BBgrid/common/model"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// Client 是 Aether 客户端，维护与服务器的 WebSocket 连接，
// 处理注册并将隧道管理委托给 handler 包。
type Client struct {
	url            string
	id             string
	token          string
	voucher        string
	privateKeyPath string
	publicKeyPath  string
	certPath       string
	useHTTP        bool
	insecure       bool
	tlsSNI         string
	origin         string
	udpTunnelKey   string
	dataDir        string
	reconnectDelay time.Duration
	stopCh         chan struct{}
	logCollector   *LogCollector
}

// NewClient 创建新的客户端实例。
func NewClient(url, id, token, voucher, privateKeyPath, publicKeyPath, certPath string, useHTTP, insecure bool, tlsSNI, origin, udpTunnelKey, dataDir string, reconnectDelay time.Duration, logCollector *LogCollector) *Client {
	return &Client{
		url:            url,
		id:             id,
		token:          token,
		voucher:        voucher,
		privateKeyPath: privateKeyPath,
		publicKeyPath:  publicKeyPath,
		certPath:       certPath,
		useHTTP:        useHTTP,
		insecure:       insecure,
		tlsSNI:         tlsSNI,
		origin:         origin,
		udpTunnelKey:   udpTunnelKey,
		dataDir:        dataDir,
		reconnectDelay: reconnectDelay,
		stopCh:         make(chan struct{}),
		logCollector:   logCollector,
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func mustReadFile(path string) []byte {
	data, _ := os.ReadFile(path)
	return data
}

// Run 启动客户端主循环，支持自动重连。
func (c *Client) Run() {
	// 检查私钥和证书是否匹配
	keyPairGenerated := false
	if fileExists(c.certPath) && fileExists(c.privateKeyPath) {
		if _, err := tls.LoadX509KeyPair(c.certPath, c.privateKeyPath); err != nil {
			alog.Warn(alog.CatAuth, "密钥与证书不匹配，重新注册", "error", err)
			os.Remove(c.certPath)
			os.Remove(c.privateKeyPath)
		}
	}

	// 1. 检查私钥，没有才生成
	if !fileExists(c.privateKeyPath) {
		alog.Info(alog.CatAuth, "私钥不存在，生成新密钥对")
		if err := register.GenerateKeyPair(c.privateKeyPath, c.publicKeyPath); err != nil {
			alog.Fatal(alog.CatAuth, "生成密钥对失败", "error", err)
		}
		alog.Info(alog.CatAuth, "密钥对已生成")
		keyPairGenerated = true
	}

	// 2. 检查证书，没有才注册
	if !fileExists(c.certPath) {
		alog.Info(alog.CatAuth, "证书不存在，开始注册")

		if !keyPairGenerated {
			// 用的是旧密钥对，先查服务器有没有已签发的证书
			if status, certPEM, err := checkApprovalStatus(c.url, c.id, c.token, c.insecure); err == nil && status == "approved" && certPEM != "" {
				// 校验证书是否匹配当前私钥
				if _, err := tls.X509KeyPair([]byte(certPEM), mustReadFile(c.privateKeyPath)); err == nil {
					if err := os.WriteFile(c.certPath, []byte(certPEM), 0600); err != nil {
						alog.Fatal(alog.CatAuth, "保存证书失败", "error", err)
					}
					alog.Info(alog.CatAuth, "证书已签发，直接下载")
					goto connect
				}
				alog.Warn(alog.CatAuth, "服务器上的证书与本地密钥不匹配，重新注册")
			}
		}

		// 走注册流程
		if c.voucher != "" {
			if err := submitVoucherRegistration(c.url, c.id, c.voucher, c.publicKeyPath, c.certPath, c.insecure); err != nil {
				alog.Fatal(alog.CatAuth, "凭证注册失败", "error", err)
			}
			alog.Info(alog.CatAuth, "凭证注册成功，证书已签发")
		} else {
			if err := submitRegistration(c.url, c.id, c.token, c.publicKeyPath, c.insecure); err != nil {
				if strings.Contains(err.Error(), "already exists") {
					alog.Info(alog.CatAuth, "注册申请已存在，等待管理员审核")
				} else {
					alog.Fatal(alog.CatAuth, "提交注册申请失败", "error", err)
				}
			} else {
				alog.Info(alog.CatAuth, "注册申请已提交，等待管理员审核")
			}
			if err := waitForApprovalAndDownloadCert(c.url, c.id, c.token, c.certPath, c.insecure, c.stopCh); err != nil {
				alog.Fatal(alog.CatAuth, "等待审核失败", "error", err)
			}
		}
		alog.Info(alog.CatAuth, "证书已签发并下载，继续启动")
	}

connect:
	// 3. 连接服务器

	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		if err := c.connectAndServe(); err != nil {
			alog.Error(alog.CatClient, "connection error", "error", err)
		}

		select {
		case <-c.stopCh:
			return
		case <-time.After(c.reconnectDelay):
		}
	}
}

// waitForApprovalAndDownloadCert 等待审核通过并下载证书
func waitForApprovalAndDownloadCert(serverURL, clientID, token, certPath string, insecure bool, stopCh <-chan struct{}) error {
	// 立即检查一次
	status, certPEM, err := checkApprovalStatus(serverURL, clientID, token, insecure)
	if err == nil && status == "approved" && certPEM != "" {
		if err := os.WriteFile(certPath, []byte(certPEM), 0600); err != nil {
			return fmt.Errorf("save certificate: %w", err)
		}
		return nil
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return fmt.Errorf("收到退出信号")
		case <-ticker.C:
			// 查询审核状态
			status, certPEM, err := checkApprovalStatus(serverURL, clientID, token, insecure)
			if err != nil {
				alog.Warn(alog.CatAuth, "查询状态失败，继续等待", "error", err)
				continue
			}

			if status == "approved" && certPEM != "" {
				// 保存证书
				if err := os.WriteFile(certPath, []byte(certPEM), 0600); err != nil {
					return fmt.Errorf("save certificate: %w", err)
				}
				return nil
			}

			alog.Info(alog.CatAuth, "等待管理员审核")
		}
	}
}

// checkApprovalStatus 查询审核状态
func checkApprovalStatus(serverURL, clientID, token string, insecure bool) (string, string, error) {
	// 转换 WebSocket URL 为 HTTP URL
	apiURL := serverURL
	if len(apiURL) > 6 && apiURL[:6] == "wss://" {
		apiURL = "https://" + apiURL[6:]
	} else if len(apiURL) > 5 && apiURL[:5] == "ws://" {
		apiURL = "http://" + apiURL[5:]
	}

	// 拼接 API 路径
	if len(apiURL) > 3 && apiURL[len(apiURL)-3:] == "/ws" {
		apiURL = apiURL[:len(apiURL)-3] + "/api/v1/register/list"
	} else {
		apiURL = apiURL + "/api/v1/register/list"
	}

	// 发送请求
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("create request: %w", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result struct {
		Code int `json:"code"`
		Data struct {
			Clients []struct {
				ClientID    string `json:"client_id"`
				CertPrefix  string `json:"cert_prefix"`
				Certificate string `json:"certificate"`
			} `json:"clients"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", "", fmt.Errorf("unmarshal response: %w (body: %.200s)", err, string(respBody))
	}

	if result.Code != 0 {
		return "", "", fmt.Errorf("query failed")
	}

	// 查找当前客户端
	for _, c := range result.Data.Clients {
		if c.ClientID == clientID {
			return "approved", c.Certificate, nil
		}
	}

	return "pending", "", nil
}

// Stop 通知客户端关闭。
func (c *Client) Stop() {
	close(c.stopCh)
}

// connectAndServe 连接服务器，注册客户端，然后运行消息泵直到连接终止。
func (c *Client) connectAndServe() error {
	alog.Info(alog.CatClient, "connecting", "url", c.url)

	dialer := &websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	if !c.useHTTP {
		cert, err := tls.LoadX509KeyPair(c.certPath, c.privateKeyPath)
		if err != nil {
			return fmt.Errorf("load X509 key pair: %w", err)
		}
		alog.Info(alog.CatClient, "client certificate loaded", "cert", c.certPath, "key", c.privateKeyPath)

		dialer.TLSClientConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return &cert, nil
			},
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: c.insecure,
		}

		if sni := tlsServerName(c.url, c.tlsSNI); sni != "" {
			dialer.TLSClientConfig.ServerName = sni
		}
	}

	header := http.Header{}
	if !c.useHTTP {
		if origin := originHeader(c.url, c.useHTTP, c.origin); origin != "" {
			header.Set("Origin", origin)
		}
	}

	wsConn, resp, err := dialer.Dial(c.url, header)
	if err != nil {
		return formatHandshakeError(err, resp)
	}

	// 在启动消息泵之前进行注册。
	if err := c.registerRaw(wsConn); err != nil {
		wsConn.Close()
		return err
	}

	h := handler.New(handler.Config{
		ClientID:       c.id,
		UseHTTP:        c.useHTTP,
		Insecure:       c.insecure,
		SNIOverride:    tlsServerName(c.url, c.tlsSNI),
		OriginOverride: originHeader(c.url, c.useHTTP, c.origin),
		UDPTunnelKey:   c.udpTunnelKey,
	})

	connection := conn.New(wsConn, h.Handle)
	h.SetSender(connection)

	connection.Start()
	defer func() {
		h.Stop()
	}()

	select {
	case <-connection.Done():
	case <-c.stopCh:
		connection.Close()
	}

	return nil
}

func formatHandshakeError(err error, resp *http.Response) error {
	if resp == nil {
		return err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
	if readErr != nil {
		return fmt.Errorf("%w (status=%s, read body: %v)", err, resp.Status, readErr)
	}
	bodyText := strings.TrimSpace(string(body))
	if bodyText == "" {
		return fmt.Errorf("%w (status=%s)", err, resp.Status)
	}
	return fmt.Errorf("%w (status=%s, body=%q)", err, resp.Status, bodyText)
}

// registerRaw 在消息泵启动之前执行注册握手。
func (c *Client) registerRaw(wsConn *websocket.Conn) error {
	regMsg := model.WSMessage{
		Type: "register",
		Data: model.RegisterData{
			ClientID: c.id,
			Token:    c.token,
		},
	}
	if err := wsConn.WriteJSON(&regMsg); err != nil {
		return fmt.Errorf("write register: %w", err)
	}

	var resp model.WSMessage
	if err := wsConn.ReadJSON(&resp); err != nil {
		return fmt.Errorf("read register response: %w", err)
	}

	if resp.Type != "registered" {
		return fmt.Errorf("registration failed: %v", resp)
	}

	var regData model.RegisteredData
	switch data := resp.Data.(type) {
	case model.RegisteredData:
		regData = data
	case map[string]any:
		if v, ok := data["client_id"].(string); ok {
			regData.ClientID = v
		}
		if v, ok := data["server_host"].(string); ok {
			regData.ServerHost = v
		}
	case string:
		if err := json.Unmarshal([]byte(data), &regData); err != nil {
			return fmt.Errorf("unmarshal registered data: %w", err)
		}
	}
	alog.Info(alog.CatClient, "registered", "clientID", regData.ClientID, "serverHost", regData.ServerHost)
	return nil
}

// submitRegistration 提交注册申请
func submitRegistration(serverURL, clientID, token, publicKeyPath string, insecure bool) error {
	// 读取公钥
	pubKeyData, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return fmt.Errorf("read public key: %w", err)
	}

	// 构造请求
	body := map[string]string{
		"client_id":  clientID,
		"public_key": string(pubKeyData),
		"token":      token,
	}
	bodyData, _ := json.Marshal(body)

	// 转换 WebSocket URL 为 HTTP URL
	apiURL := serverURL
	if len(apiURL) > 6 && apiURL[:6] == "wss://" {
		apiURL = "https://" + apiURL[6:]
	} else if len(apiURL) > 5 && apiURL[:5] == "ws://" {
		apiURL = "http://" + apiURL[5:]
	}

	// 拼接 API 路径
	if len(apiURL) > 3 && apiURL[len(apiURL)-3:] == "/ws" {
		apiURL = apiURL[:len(apiURL)-3] + "/api/v1/register/apply"
	} else {
		apiURL = apiURL + "/api/v1/register/apply"
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
		Timeout: 10 * time.Second,
	}
	resp, err := client.Post(apiURL, "application/json", bytes.NewReader(bodyData))
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("unmarshal response: %w (body: %.200s)", err, string(respBody))
	}

	if result.Code != 0 {
		return fmt.Errorf("registration failed: %s", result.Msg)
	}

	return nil
}

// submitVoucherRegistration 凭证注册：提交凭证 + 公钥，自动 approve，保存证书
func submitVoucherRegistration(serverURL, clientID, voucher, publicKeyPath, certPath string, insecure bool) error {
	pubKeyData, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return fmt.Errorf("read public key: %w", err)
	}

	body := map[string]string{
		"client_id":  clientID,
		"public_key": string(pubKeyData),
		"voucher":    voucher,
	}
	bodyData, _ := json.Marshal(body)

	apiURL := serverURL
	if len(apiURL) > 6 && apiURL[:6] == "wss://" {
		apiURL = "https://" + apiURL[6:]
	} else if len(apiURL) > 5 && apiURL[:5] == "ws://" {
		apiURL = "http://" + apiURL[5:]
	}
	if len(apiURL) > 3 && apiURL[len(apiURL)-3:] == "/ws" {
		apiURL = apiURL[:len(apiURL)-3] + "/api/v1/register/voucher"
	} else {
		apiURL = apiURL + "/api/v1/register/voucher"
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: insecure},
		},
		Timeout: 10 * time.Second,
	}
	resp, err := client.Post(apiURL, "application/json", bytes.NewReader(bodyData))
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Certificate string `json:"certificate"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("unmarshal response: %w (body: %.200s)", err, string(respBody))
	}
	if result.Code != 0 {
		return fmt.Errorf("voucher registration failed: %s", result.Msg)
	}
	if result.Data.Certificate == "" {
		return fmt.Errorf("no certificate returned")
	}

	// 保存证书到配置指定的路径
	if err := os.WriteFile(certPath, []byte(result.Data.Certificate), 0600); err != nil {
		return fmt.Errorf("save certificate: %w", err)
	}

	alog.Info(alog.CatAuth, "凭证注册成功，证书已保存", "cert", certPath)
	return nil
}
