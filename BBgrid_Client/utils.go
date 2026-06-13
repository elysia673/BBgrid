package main

import (
	"net/url"
)

// tlsServerName 从 URL 中提取 TLS 服务器名称，支持覆盖。
func tlsServerName(serverURL, override string) string {
	if override != "" {
		return override
	}
	u, err := url.Parse(serverURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// originHeader 生成 WebSocket Origin 头，支持覆盖。
func originHeader(serverURL string, useHTTP bool, override string) string {
	if override != "" {
		return override
	}
	host := tlsServerName(serverURL, "")
	if host == "" {
		return ""
	}
	if useHTTP {
		return "http://" + host
	}
	return "https://" + host
}
