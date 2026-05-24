# BBgrid Server 架构图

## 模块关系

```
┌─────────────────────────────────────────────────────────────┐
│                         Main                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                    Supervisor                        │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │   │
│  │  │  Auth   │ │  State  │ │  Data   │ │ Control │   │   │
│  │  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │   │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘   │   │
│  │       │           │           │           │         │   │
│  │       │           │           │           │         │   │
│  │       └───────────┴───────────┴───────────┘         │   │
│  │                      │                               │   │
│  │                      ▼                               │   │
│  │                 ┌─────────┐                          │   │
│  │                 │   Bus   │                          │   │
│  │                 └─────────┘                          │   │
│  │  ┌─────────┐                                         │   │
│  │  │   WS    │                                         │   │
│  │  │ Worker  │                                         │   │
│  │  └─────────┘                                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐                  │
│  │ HTTP Server     │  │ Tunnel Listener │                  │
│  │ (Gin)           │  │ (TCP/UDP)       │                  │
│  └─────────────────┘  └─────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
```

## 控制面/数据面分离

```
┌─────────────────────────────────────────────────────────────┐
│ Control Plane (管理态)                                       │
├─────────────────────────────────────────────────────────────┤
│ Auth Worker: CA、JWT、注册表、命名空间                         │
│ State Worker: 客户端状态、代理状态、中继会话                    │
│ Control Worker: REST API、命令分发                            │
│ WS Worker: WebSocket 连接管理                                │
├─────────────────────────────────────────────────────────────┤
│ 特点:                                                       │
│ • 低频操作                                                  │
│ • 带锁、可阻塞                                              │
│ • 毫秒级响应                                                │
│ • 允许序列化/反序列化                                         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Data Plane (转发态)                                          │
├─────────────────────────────────────────────────────────────┤
│ Data Worker: TCP 代理、UDP 代理、隧道配对、字节流 Pipe          │
├─────────────────────────────────────────────────────────────┤
│ 特点:                                                       │
│ • 高并发                                                    │
│ • 零锁、零分配                                              │
│ • 微秒级响应                                                │
│ • 缓存友好                                                  │
└─────────────────────────────────────────────────────────────┘
```

## Worker 职责

```
┌─────────────────────────────────────────────────────────────┐
│ Auth Worker (管理态)                                         │
├─────────────────────────────────────────────────────────────┤
│ • CA 证书管理 (签发/验证)                                    │
│ • JWT 签发/验证                                              │
│ • 客户端注册表 (CRUD)                                        │
│ • 命名空间管理                                               │
│ • Init() 同步初始化                                          │
│ • Run() 异步订阅事件                                         │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ State Worker (管理态)                                        │
├─────────────────────────────────────────────────────────────┤
│ • 客户端连接管理 (sync.Map)                                  │
│ • 代理状态管理 (内存)                                        │
│ • 端口索引 (port -> clientID)                                │
│ • 隧道令牌管理                                               │
│ • 中继会话管理                                               │
│ • 无持久化（后续通过插件实现）                                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Data Worker (转发态)                                         │
├─────────────────────────────────────────────────────────────┤
│ • TCP 代理监听                                               │
│ • KCP/UDP 代理监听                                           │
│ • TCP 隧道配对 (pendingMap)                                  │
│ • 双向字节流 Pipe (零拷贝)                                   │
│ • 唯一需要锁的地方: pendingMap                                │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Control Worker (管理态)                                      │
├─────────────────────────────────────────────────────────────┤
│ • REST API 处理 (Gin)                                        │
│ • 代理 CRUD                                                  │
│ • 中继 CRUD                                                  │
│ • 命名空间 API                                               │
│ • ListClients() 合并 Auth + State 数据                       │
│ • 调用 DataWorker 启动代理                                   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ WS Worker (管理态)                                           │
├─────────────────────────────────────────────────────────────┤
│ • WebSocket 连接管理                                         │
│ • 常驻节点连接 (mTLS)                                        │
│ • 临时节点连接 (Token)                                       │
│ • WebSocket 隧道                                             │
│ • 中继 WebSocket                                             │
│ • clientConn 封装 (readPump/writePump)                       │
└─────────────────────────────────────────────────────────────┘
```

## 数据流

```
创建代理:
CLI ──HTTP──> Control Worker ──StateStore──> State Worker
                │                              │
                │                              ├──> AddProxy()
                │                              ├──> RegisterPort()
                │                              └──> StoreTunnelToken()
                │
                └──> Data Worker
                       │
                       └──> StartTCPProxy()

隧道连接 (TCP):
User ──TCP──> Data Worker ──pending──> Client
                  │
                  └──> tunnel_request ──> Client
                  └──> tunnel_auth ──> Server
                  └──> 配对成功 ──> pipeBidir()

隧道连接 (KCP/UDP):
User ──KCP──> Data Worker ──pending──> Client
                  │
                  └──> tunnel_auth (TUNL magic) ──> Client
                  └──> 配对成功 ──> pipeBidir()

UDP 代理:
User ──UDP──> Data Worker ──[0xAE]──> Client
                  │
                  └──> 公网用户包 ──> 封装转发
                  └──> 隧道回包 ──> 解封装转发

客户端列表:
CLI ──HTTP──> Control Worker
                │
                ├──> Auth Worker (GetApproved)
                └──> State Worker (ListClients)
                │
                └──> 合并: 在线/离线状态
```

## 客户端状态逻辑

```
┌─────────────────────────────────────────────────────────────┐
│ 审核制：只有已注册的客户端才能连接                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Auth Worker (注册表)          State Worker (在线表)         │
│  ┌─────────────────┐          ┌─────────────────┐          │
│  │ client-1 ✓      │          │ client-1 ✓      │          │
│  │ client-2 ✓      │          │ client-3 ✓      │          │
│  │ client-3 ✓      │          │                 │          │
│  └─────────────────┘          └─────────────────┘          │
│                                                             │
│  合并结果:                                                   │
│  ┌─────────────────────────────────────────────┐            │
│  │ client-1  在线  (注册 + 在线)                │            │
│  │ client-2  离线  (注册 + 不在线)              │            │
│  │ client-3  在线  (注册 + 在线)                │            │
│  └─────────────────────────────────────────────┘            │
│                                                             │
│  未注册 + 在线 = 不会出现（审核制 + Token 验证）              │
└─────────────────────────────────────────────────────────────┘
```

## 接口设计 (gRPC 扩展点)

```
┌─────────────────────────────────────────────────────────────┐
│ StateStore 接口 (管理态)                                     │
├─────────────────────────────────────────────────────────────┤
│ AddClient(clientID, conn, remoteAddr)                       │
│ RemoveClient(clientID)                                      │
│ GetClient(clientID) -> ClientState                          │
│ ListClients() -> []ClientInfo                               │
│ SendCommand(clientID, cmd)                                  │
│ AddProxy(clientID, proxy)                                   │
│ RemoveProxy(clientID, port)                                 │
│ GetProxy(clientID, port) -> ProxyState                      │
│ ListProxies() -> []ProxyInfo                                │
│ RegisterPort(clientID, port)                                │
│ UnregisterPort(port)                                        │
│ GetClientIDByPort(port) -> string                           │
│ StoreTunnelToken(token, key)                                │
│ RemoveTunnelTokenByKey(key)                                 │
│ FindTableByWSToken(token) -> ClientState, string, error     │
│ CreateRelaySession(session)                                 │
│ GetRelaySession(sessionID) -> RelaySession                  │
│ RemoveRelaySession(sessionID)                               │
│ ListRelaySessions() -> []RelaySession                       │
│ UpdateRelaySessionStatus(sessionID, status, err)            │
│ GetPublicIP() -> string                                     │
└─────────────────────────────────────────────────────────────┘

未来可用 gRPC 实现此接口，支持分布式部署。
```

## Bus vs 接口调用边界

```
┌─────────────────────────────────────────────────────────────┐
│ 接口调用 (同步)                                              │
├─────────────────────────────────────────────────────────────┤
│ 用途: REST API，需要返回值                                   │
│                                                             │
│ ControlWorker → StateStore:                                 │
│   GetClient() -> ClientState                                │
│   ListClients() -> []ClientInfo                             │
│   AddProxy()                                                │
│   RemoveProxy()                                             │
│   SendCommand()                                             │
│   CreateRelaySession()                                      │
│   ...                                                       │
│                                                             │
│ ControlWorker → AuthWorker:                                 │
│   GetApproved() -> []ClientRecord                           │
│   GetNamespace() -> *NamespaceInfo                          │
│   ...                                                       │
│                                                             │
│ ControlWorker → DataWorker:                                 │
│   StartTCPProxy()                                           │
│   StartKCPProxy()                                           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Bus 事件 (异步通知)                                          │
├─────────────────────────────────────────────────────────────┤
│ 用途: 日志、监控、清理，不需要返回值                          │
│                                                             │
│ client.ready          -> 客户端注册完成通知                  │
│ client.disconnected   -> 客户端断开连接通知                  │
│ proxy.created         -> 代理创建完成通知                    │
│ proxy.closed          -> 代理关闭通知                       │
│ relay.created         -> 中继创建完成通知                    │
│ relay.closed          -> 中继关闭通知                       │
│                                                             │
│ 订阅者: StateWorker (用于日志、监控)                         │
└─────────────────────────────────────────────────────────────┘
```

## 事件类型

```
EventClientReady       { ClientID, RemoteAddr }
EventClientDisconnected { ClientID, IsTemp }
EventProxyCreated      { ClientID, RemotePort, LocalPort, Protocol, PublicAddr }
EventProxyClosed       { ClientID, RemotePort }
EventRelayCreated      { SessionID, SourceClient, TargetClient, Protocol }
EventRelayClosed       { SessionID }
```

---

## 插件系统

```
┌─────────────────────────────────────────────────────────────┐
│ 插件架构                                                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  plugins/                                                   │
│  ├── latency/                                               │
│  │   └── latency.go        # 延迟插件                       │
│  └── monitor/                                               │
│      └── monitor.go        # 监控插件                       │
│                                                             │
│  插件通过 init() 自动注册到全局注册表                         │
│  main.go 中 import 插件包即可                                │
│  配置文件控制启用/禁用                                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## 插件配置 (config.json)

```json
{
  "plugins": {
    "latency": {
      "enabled": true,
      "config": {
        "cleanup_interval": 300
      }
    },
    "monitor": {
      "enabled": true,
      "config": {
        "interval": 60
      }
    },
    "alert": {
      "enabled": false,
      "config": {}
    }
  }
}
```

## 插件接口

```go
type Plugin interface {
    Name() string
    Version() string
    Init(bus Bus, state StateStore, config map[string]any) error
    Run() error
    Stop()
    Actions() []Action  // 声明能力
}
```

## 插件开发流程

```bash
# 1. 编写插件 plugins/latency/latency.go

# 2. 在 main.go 中 import
_ "BBgrid/plugins/latency"

# 3. 更新配置文件启用插件
vim data/config.json

# 4. 编译运行
make build
make run-server
```

## 插件 API

```
GET  /api/v1/sync   - 获取所有插件能力
POST /api/v1/run    - 执行指定能力
```

## CLI 使用

```bash
# 同步能力
bbgrid-cli sync

# 执行能力
bbgrid-cli run latency.get --client_id=my-client
bbgrid-cli run latency.list
```

---

## 学习路径

### 第一阶段：理解基础概念

```
1. 消息总线 (bus.go)
   - 什么是发布/订阅模式
   - 为什么用 channel 而不是直接调用
   - 同步 vs 异步发布

2. 接口设计 (workers/types.go)
   - 什么是面向接口编程
   - 为什么用 interface 而不是 struct
   - 依赖注入的概念

3. Worker 模式 (supervisor.go)
   - 什么是 Worker
   - 为什么每个 Worker 独立 goroutine
   - Supervisor 如何监控和重启
```

### 第二阶段：管理态 Worker

```
1. Auth Worker (workers/auth.go)
   - CA 证书是什么
   - JWT Token 如何工作
   - 注册表如何管理客户端

2. State Worker (workers/state.go)
   - sync.Map 的并发安全
   - 客户端状态如何存储
   - 代理状态如何管理

3. Control Worker (workers/control.go)
   - REST API 如何处理
   - 如何调用其他 Worker
   - 如何合并多个数据源

4. WS Worker (workers/ws.go)
   - WebSocket 如何工作
   - readPump/writePump 模式
   - 如何处理消息分发
```

### 第三阶段：数据面 Worker

```
1. Data Worker (workers/data.go)
   - 什么是控制面/数据面分离
   - 为什么数据面要零锁
   - pendingMap 如何工作

2. 隧道配对流程
   - 客户端如何连接隧道
   - 如何验证 Token
   - 如何配对公网连接和隧道连接

3. 字节流 Pipe
   - 什么是零拷贝
   - io.Copy 如何工作
   - 双向转发如何实现
```

### 第四阶段：实际流程

```
1. 客户端注册流程
   main.go -> ws.go -> auth.go -> state.go
   - WebSocket 连接
   - 证书验证
   - 状态注册

2. 创建代理流程
   main.go -> control.go -> state.go -> data.go
   - REST API 调用
   - 状态更新
   - TCP 监听启动

3. 隧道连接流程
   main.go -> data.go -> client
   - TCP 连接到达
   - Token 验证
   - 双向转发

4. 中继流程
   main.go -> control.go -> state.go -> ws.go
   - 创建会话
   - 信令发送
   - WebSocket 桥接
```

### 第五阶段：插件系统

```
1. 插件接口 (workers/plugin.go)
   - Plugin 接口定义
   - 插件配置结构体
   - 插件清单结构体

2. 插件管理器 (workers/plugin_manager.go)
   - 如何加载配置文件
   - 如何验证 MD5 校验和
   - 如何动态加载 .so 文件
   - 如何管理插件生命周期

3. 插件开发 (plugins/latency/)
   - 如何实现 Plugin 接口
   - 如何订阅事件
   - 如何提供 API
   - 如何编译为 .so 文件

4. 插件 API
   - GET /api/v1/plugins - 列出插件
   - POST /api/v1/plugins/:name/reload - 重新加载

5. 编译和部署
   - make plugin-latency - 编译插件
   - make md5 - 计算校验和
   - 更新 plugins.json - 配置插件
```

---

## 代码阅读顺序

```
推荐顺序（从简单到复杂）：

1. bus.go                    - 消息总线，理解发布/订阅
2. supervisor.go             - 进程监控，理解 Worker 模式
3. workers/types.go          - 接口定义，理解面向接口编程
4. workers/auth.go           - 认证，理解 CA/JWT/注册表
5. workers/state.go          - 状态机，理解并发安全存储
6. workers/data.go           - 数据面，理解控制面/数据面分离
7. workers/control.go        - 控制器，理解 REST API 处理
8. workers/ws.go             - WebSocket，理解连接管理
9. workers/conn.go           - 连接封装，理解 readPump/writePump
10. main.go                  - 组装入口，理解依赖注入
```

---

## 关键概念

```
1. 发布/订阅 (Pub/Sub)
   - 解耦模块间依赖
   - 异步消息传递
   - 事件驱动架构

2. 接口隔离 (Interface Segregation)
   - 每个接口只暴露必要方法
   - 依赖接口而不是实现
   - 便于测试和扩展

3. 控制面/数据面分离
   - 控制面：低频、带锁、可阻塞
   - 数据面：高频、零锁、零分配
   - 提高系统整体性能

4. 依赖注入 (Dependency Injection)
   - 外部创建依赖
   - 构造函数注入
   - 便于测试和替换

5. Worker 模式
   - 每个 Worker 独立 goroutine
   - Supervisor 监控重启
   - 优雅关闭
```

---

## 常见问题

```
Q: 为什么用 channel 而不是直接调用？
A: 解耦模块、支持异步、便于测试、支持分布式扩展

Q: 为什么 State Worker 和 Data Worker 分开？
A: 管理态需要锁保证一致性，数据态需要零锁保证性能

Q: 为什么用 sync.Map 而不是普通 map？
A: 并发安全，适合读多写少场景

Q: 如何扩展为分布式？
A: 将 StateStore 接口改为 gRPC 实现，State Worker 变为独立服务

Q: 如何添加新功能？
A: 创建新 Worker，通过 Bus 订阅/发布事件
```
