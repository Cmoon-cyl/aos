# 支持范围

本页说明 0.2.0 的分发目标和兼容性边界。每个平台的实际系统、解释器、
兼容性边界以[发行说明](releases/v0.2.0.md)为准；制品摘要见包内 `manifest.json`。

## 平台包

| 资产名称中的平台 | 目标 | Python | 格式 |
| --- | --- | --- | --- |
| `macos-arm64` | Apple Silicon，`aarch64-apple-darwin` | arm64 CPython 3.11 | `.tar.gz` |
| `linux-x86_64` | GNU/Linux，`x86_64-unknown-linux-gnu` | x86_64 CPython 3.11 | `.tar.gz` |
| `windows-x86_64` | Windows MSVC，`x86_64-pc-windows-msvc` | x64 CPython 3.11 | `.zip` |

完整包包含 CLI、SDK wheel、固定版本 Router 及其随附插件、配置、接口、示例、
公开安装器、文档、上游许可和 SHA256 清单。Python 解释器由使用者提供；
不需要安装 Rust 工具链，也不需要从 PyPI 安装 AOS。

wheel 使用 `cp311-abi3`。这表示 Python ABI 下限为 3.11，不等于已经实测所有后续 Python 版本。
PyPy、其他架构、macOS Intel、Linux ARM64 和未列出的系统版本不在当前原生验收矩阵中。

Linux wheel 使用原生 GNU/glibc 标签，**不承诺 manylinux 或 musl 兼容**。
macOS wheel 的部署标签不是最低系统版本实测。Windows 使用 MSVC 原生产物；
原生 CLI、SDK 扩展、官方 Router 和插件依赖 `VCRUNTIME140.dll`，需预先安装
[Microsoft Visual C++ v14 x64 运行库](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)。
本次宿主验证使用 `14.51.36247.0`；这不是对所有干净 Windows 镜像的预装依赖承诺。

## 能力和平台差异

Topic、Service、Action、Graph、参数、lifecycle、QoS、Clock/Timer、bag、安全配置与
本机进程管理属于 0.2 平台能力。Windows 的进程和文件实现使用原生机制，
不能用 POSIX 信号、权限位或文件描述符测试代替 Windows 等价检查。
准确的已执行项目与 N/A 原因见[Windows 原生范围](platform/windows-native-scope.md)。

Linux systemd 用户服务是单独的系统集成；不代表 Windows 服务或 macOS launchd 已自动配置。
进程管理不等于开机自启，连接恢复也不等于业务任务恢复。

## 网络与运行边界

- 默认开发模式使用 TCP、本机 peer/gossip 和每机 Router；跨机显式连接 Router。
- 安全模式使用 mTLS、证书 CN 身份和默认拒绝 ACL；peer/Router 转发集合显式配置。
- NAT、网关、防火墙、证书签发和磁盘配额由部署方管理。
- Action goal 和结果不跨 provider 重启持久化；需要恢复的业务自行设计存储和幂等契约。
- 当前没有硬实时、任意 Router mesh 高可用、端到端调用者身份透传或 ROS wire 兼容承诺。
- 24 小时长稳及完整性能矩阵是后续专项验证，不能由功能测试或短时混合负载推导。

更多边界见[已知限制](known-limitations.md)。
升级时对照[迁移指南](platform/migration-0.2.md)，不要仅比较版本字符串。
