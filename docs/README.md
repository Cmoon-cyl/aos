# AOS 使用文档

AOS 通过 Topic、Service 和 Action 连接不同机器上的程序与 Agent。
第一次使用时，从安装开始，完成一次真实请求，再选择需要的能力。

## 第一次使用

1. [安装平台包](install.md)：选择系统和架构，校验 SHA256，安装 Python wheel。
2. [运行第一个应用](getting-started.md)：启动 Router 和 provider，调用 Service，体验 Action。
3. [理解核心概念](concepts.md)：Node、Interface、发现、三种通信方式和状态含义。
4. [挑选一个示例](examples.md)：消息、任务、生命周期或带持久化的 MemoryServer。

## 开发应用

| 文档 | 解决的问题 |
| --- | --- |
| [Python SDK](python-sdk.md) | 创建 Node、声明类型、运行回调、等待结果和释放资源 |
| [CLI](cli.md) | 发现能力、读取 schema、提交 JSON 请求与解释输出 |
| [配置、QoS 与时间](platform/behavior.md) | 配置优先级、名称映射、队列、时钟和 Timer |
| [参数与 lifecycle](platform/managed-cli.md) | 读取/更新参数、启停业务准入、查看状态 |
| [发布、过滤、hook 和诊断](platform/cli-observation.md) | 重复发布、等待条件、脚本判断、频率与带宽 |
| [安装包索引](platform/installed-packages.md) | 声明程序入口和资源，从任意工作目录定位安装包 |
| [run / launch](platform/process-cli.md) | 启动进程、等待就绪、组织依赖、重启与停止 |
| [进程配置](platform/process-bootstrap.md) | 把 Context、Node 和部署配置传给被启动程序 |
| [bag](platform/bag-cli.md) | 录制、检查、恢复、暂停和回放消息 |
| [MemoryServer](platform/memory-server.md) | 组合 Service、Action、参数、生命周期与 SQLite 持久化 |

## 部署和排查

| 文档 | 解决的问题 |
| --- | --- |
| [部署指南](deployment.md) | 单机、多机、跨网与系统服务的选择 |
| [每机 Router 拓扑](platform/topology.md) | peer/gossip、显式 Router 连接、TLS 转发权限 |
| [安全 CLI 与 SDK](platform/security-cli.md) | profile、证书身份、ACL、受控 Router |
| [安全策略契约](platform/security-runtime.md) | 编写有限权限策略并理解权限展开 |
| [系统服务](platform/system-service.md) | Linux systemd 用户服务的安装、检查与停止 |
| [故障排查](troubleshooting.md) | 无法导入、看不到 Node、请求超时、无法退出 |
| [Core 遥测](platform/core-telemetry.md) / [Zenoh 遥测](platform/zenoh-telemetry.md) | 关联请求、Goal、连接与资源计数 |
| [请求观察器](platform/request-observer.md) | 观察脱敏请求元数据，录制和回放诊断事件 |

## 版本、兼容性和维护

- [支持范围](support.md)与[已知限制](known-limitations.md)：平台、Python ABI、网络和恢复边界。
- [0.2.0 发行说明](releases/v0.2.0.md)、[更新日志](../CHANGELOG.md)与[迁移指南](platform/migration-0.2.md)。
- [源码构建与发行](development.md)、[依赖来源](../upstream/README.md)和[源码包](platform/source-distribution.md)。
- [平台契约索引](platform/README.md)：深入理解实现责任与可验证行为。

构建和诊断工具见[测试指南](testing.md)；参与开发前请阅读[贡献指南](../CONTRIBUTING.md)。
