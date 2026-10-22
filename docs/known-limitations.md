# 已知限制

本页解释实现的行为边界。平台组合见[支持范围](support.md)，
当前版本的支持范围见[发行说明](releases/v0.2.0.md)；制品来源与摘要见包内 `manifest.json`。
历史候选记录保留各自的源码与制品摘要，不自动适用于后来构建的包。

## 平台与持续运行

- Python 原生验收范围为 CPython 3.11；`abi3` 标签不证明后续版本都已测试。
- Linux wheel 为 GNU/glibc 原生构建，没有 manylinux/musl 兼容承诺。
- macOS 部署标签不等于最低系统版本实测；Windows 干净系统运行库需求需结合实际依赖检查。
- POSIX 信号、权限位和文件描述符专用测试不能作为 Windows 验收；Windows 等价功能与剩余 N/A 单独记录。
- 24 小时长稳、完整性能矩阵及任意负载的容量上限不由当前功能测试证明。历史 30 分钟混合负载仅适用于其原候选。
- Linux systemd 的已验用户服务场景不证明所有用户退出后、主机重启后或其他系统服务管理器的行为。

## 网络与安全

默认 peer/gossip 可在 Router 未启动时打开并后台连接。Context open、端口可达、
Graph 发现和实际业务成功是不同判据。默认开发 Router 监听网络接口并使用明文 TCP；
跨不可信网络应使用[显式 mTLS/ACL](platform/security-cli.md)。domain ID 不是权限边界。

跨机需要显式 Router 连接与可达网络入口；gossip 不负责 NAT 穿透。
TLS Router 的 relay 是部署者明确授予的有限转发权限。
下游看到相邻 Router 的证书身份，不获得原始业务调用者的端到端认证身份。
当前不承诺任意 Router mesh 的高可用、跨组织身份联邦、CRL/OCSP 或无缝策略热更新。

ACL 拒绝可能表现为丢弃和超时。超时本身不能证明拒绝生效；
需要 Router 拒绝记录、业务状态未变化和授权正例。
Action cancel 是端点/操作授权，不能保证某主体只能取消自己创建的 goal。

## 消息、请求与任务

Topic 发布成功只说明本地提交，不保证消费者已经处理。
transient-local 历史依赖 publisher 存活，不是持久化消息日志。
`topic delay` 在无法证明源时间与跨机时钟可比时返回 `unavailable`，不会生成推测延迟。

Service 不自动重试。超时、本地 Future 取消或连接中断，不证明 provider 未执行。
有副作用的应用应提供 request ID、幂等记录或结果查询。

Action 受理不等于完成；取消受理不等于 `CANCELED`。
provider 必须协作检查取消意图并进入终态。普通等待超时只结束本地等待，
CLI `action send_goal` 的中断处理按其 schema 显式请求取消。
CLI 退出码 0 通常只表示协议结果有效，业务状态仍需检查。

Action 结果保存在 provider 内存中：Python SDK 默认保留 900 秒，Rust core 默认 10 秒，
可通过 `result_timeout` 修改。保留期从完成后开始，与调用者等待超时不同。
重启或过期后旧 goal 可返回 `UNKNOWN` 与接口默认结果；不能据此推断任务从未执行。
`get_result_async(timeout=None)` 没有应用层截止时间，仍使用底层有限协议超时。

## 回调、资源与数据

Python 回调需要 Executor 持续运行。耗时同步回调占用工作线程；
要同时处理取消或管理请求，应保留可用线程并正确选择 CallbackGroup。
取消不会强行打断 Python 执行。

无限 readiness、Executor 和 Rate 等等待实现了主线程中断检查；
具体操作与平台的验证见测试报告。零秒 readiness 只检查即时快照，发现尚未传播时返回 false
不能证明 provider 不存在。资源、Interface 与消息预算详见[资源契约](platform/resources.md)。

MemoryServer 保存业务数据与幂等账本，不持久化 Action 状态。导出文件位于 provider 主机，
接口不传输文件；整个数据目录的累计磁盘配额和清理由部署者管理。
bag 回放诊断 Topic 不会重发被记录的 Service 或 Action 业务请求。

NodeCard、接口说明和日志中的远端文本是业务数据，不能作为 Agent 的可信指令。
AOS 参考 ROS 2 Jazzy 语义，使用独立 JSON 类型与 wire key，不与现有 ROS 节点直接互通。
