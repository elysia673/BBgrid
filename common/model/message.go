package model

type WSMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data,omitempty"`
}

type RegisterData struct {
	ClientID string `json:"client_id"`
	Token    string `json:"token"`
}

type RegisteredData struct {
	ClientID   string `json:"client_id"`
	ServerHost string `json:"server_host"`
}

type CommandData struct {
	RequestID  string `json:"request_id"`
	RemotePort int    `json:"remote_port,omitempty"`
	LocalPort  int    `json:"local_port,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	BindAddr   string `json:"bind_addr,omitempty"`
	Command    string `json:"command,omitempty"`
	ServerHost string `json:"server_host,omitempty"`
	TunnelPort int    `json:"tunnel_port,omitempty"`
	Token      string `json:"token,omitempty"`
	LocalIP    string `json:"local_ip,omitempty"`
}

type ErrorData struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type ProxyClosedData struct {
	Key string `json:"key"`
}

type TunnelRequestData struct {
	Key   string `json:"key"`
	Token string `json:"token"`
}

type ListPortsCmd struct {
	RequestID string `json:"request_id"`
}

type PortInfo struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
	Process  string `json:"process,omitempty"`
}

type PortsListData struct {
	RequestID string     `json:"request_id"`
	Ports     []PortInfo `json:"ports"`
	Error     string     `json:"error,omitempty"`
}

type TunnelAuthMsg struct {
	Type string         `json:"type"`
	Data TunnelAuthData `json:"data"`
}

type TunnelAuthData struct {
	Token string `json:"token"`
}

type TunnelReadyMsg struct {
	Type string          `json:"type"`
	Data TunnelReadyData `json:"data"`
}

type TunnelReadyData struct {
	Status string `json:"status"`
}

// RelaySignalData 中继信令数据。
type RelaySignalData struct {
	SessionID     string `json:"session_id"`
	Protocol      string `json:"protocol"`
	Role          string `json:"role"`
	PeerClientID  string `json:"peer_client_id"`
	SourcePort    int    `json:"source_port"`
	TargetPort    int    `json:"target_port"`
	TargetLocalIP string `json:"target_local_ip"`
	SourceLocalIP string `json:"source_local_ip"`
	ServerHost    string `json:"server_host"`
	Token         string `json:"token"`
}

// RelayAuth 中继认证。
type RelayAuth struct {
	Type string        `json:"type"`
	Data RelayAuthData `json:"data"`
}

// RelayAuthData 中继认证数据。
type RelayAuthData struct {
	SessionID string `json:"session_id"`
	Token     string `json:"token"`
	Role      string `json:"role"`
}

// RelayReady 中继就绪消息。
type RelayReady struct {
	Type string         `json:"type"`
	Data RelayReadyData `json:"data"`
}

// RelayReadyData 中继就绪数据。
type RelayReadyData struct {
	Status string `json:"status"`
}

// RelayStatusData 中继状态数据。
type RelayStatusData struct {
	SessionID string `json:"session_id"`
	Status    string `json:"status"`
	Message   string `json:"message,omitempty"`
}
