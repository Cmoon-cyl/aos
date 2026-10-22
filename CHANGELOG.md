# 更新日志

## 0.2.0

发行内容与验收状态见 [0.2.0 发行说明](docs/releases/v0.2.0.md)。

### 新增

- 配置分层、精确 remapping、公开 QoS 配置、Clock/Timer/Rate。
- 参数、LifecycleNode 与管理 CLI。
- 安装包索引、run/launch、依赖就绪、有限重启和进程清理。
- Topic 发布与观察增强、脚本 hook、诊断与有界遥测。
- MCAP bag 录制、恢复与回放。
- mTLS 身份、有限 ACL 编译、本机安全 peer 与多机 Router 连接。
- SQLite MemoryServer 与独立请求观察器。
- 三平台发行包布局、附带固定 Router、分层使用文档。

### 修复

- 补齐 Windows Job Object 进程管理、原生 hook/doctor、CLI bag、Memory 与控制台输出处理。
- macOS 拒绝隐式 shell 回退，准确报告无效执行文件并清理已启动进程。
- TLS 证书到期清理等待连接初始化完成，拒绝已到期连接，避免到期同秒重连漏关。

### 行为变化

- 默认采用每机 Router 与本机 peer/gossip；peer 支持后台连接和重连。
- Topic pub 默认不等待订阅者，显式字段控制匹配等待与重复。
- Context 程序初值可被部署环境覆盖；安全 profile 的显式选择仍优先。
- Interface 校验实施有界资源准入；Service 等待区分无响应、队列溢出与真正超时。

完整兼容说明见 [0.1 → 0.2 迁移](docs/platform/migration-0.2.md)。

## 0.1.0

建立 Rust core、Zenoh adapter、Python SDK、JSON CLI、Node/Graph/NodeCard、
Topic、Service 和 Action。
