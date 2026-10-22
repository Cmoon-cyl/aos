# AOS 0.2 平台契约

本目录说明当前实现的公开行为、责任边界和验证入口。
首次使用请从[使用文档](../README.md)开始；发行资产和逐平台验证范围见
[0.2.0 发行说明](../releases/v0.2.0.md)。

Topic/Service/Action、配置/QoS/时间、参数/lifecycle、run/launch、bag、遥测和
显式 mTLS/ACL 均已有实现。24 小时长稳与完整性能矩阵不在当前发行承诺内。

| 需要理解的行为 | 入口 |
| --- | --- |
| 配置、名称、QoS、Clock/Timer | [行为契约](behavior.md) |
| Python/CLI 启动配置 | [进程配置](process-bootstrap.md) |
| 参数与生命周期 | [管理 CLI](managed-cli.md)、[生命周期核心](lifecycle-core.md) |
| 安装资源与进程监管 | [安装包](installed-packages.md)、[run/launch](process-cli.md)、[launch 契约](launch.md) |
| 发布、观察、hook、统计与 doctor | [CLI 观察](cli-observation.md) |
| 业务和传输遥测 | [Core](core-telemetry.md)、[Zenoh](zenoh-telemetry.md) |
| bag 格式与运行 | [格式](bag-format.md)、[运行](bag-runtime.md)、[CLI](bag-cli.md) |
| 身份、权限和网络 | [安全边界](security.md)、[安全实现](security-runtime.md)、[使用入口](security-cli.md)、[每机 Router](topology.md) |
| 资源限制与等待预算 | [资源](resources.md)、[readiness 预算](readiness-budget.md) |
| 完整普通程序 | [MemoryServer](memory-server.md)、[请求观察器](request-observer.md) |
| 升级兼容性 | [0.2 迁移](migration-0.2.md) |
| 上游参考与测试 | [固定来源](sources.md)、[测试指南](../testing.md) |

## 核心约束

- 路径保持 `Python SDK → 同进程 PyO3 → Rust core → Zenoh`；原生 CLI 复用同一核心。
- provider 的 Rust ActionServer 唯一持有本进程 goal 状态。Python 和 Router 不维护第二份状态机。
- Topic 支持 KEEP_LAST/depth、reliable/best_effort、volatile/transient_local；
  Service 与 Action 的 Service 通道只接受 reliable/volatile，depth 可配置。
- 控制等待使用单调时间；模拟时间不能暂停超时、中断和关闭。
- bag 保存 MCAP、原始 JSON payload、完整 Interface、类型 hash 和时间证据。
- 安全配置显式选择证书身份与有限权限，校验失败不降级到开发连接。
- 默认每机 Router 与本机 peer/gossip；跨机与 TLS 转发集合由部署者显式配置。

CLI 的可执行契约以当前二进制 `aos --help` 和 `aos schema GROUP VERB` 为准。

新增公开行为应同步实现、help/schema、类型提示、示例与迁移说明。
构建来源、测试候选、合入 commit 和发行文件摘要分别记录；
单元测试、组包成功、进程启动和真实业务通过不能互相替代。
