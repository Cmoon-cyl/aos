# 测试与诊断工具

先按[开发指南](development.md)准备固定的 Rust 工具链和 CPython 3.11。
运行网络测试前，确认使用的 Router、端口和测试目录属于本次运行。

## 本地检查

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
python -I -m unittest discover -s scripts -p 'test_*.py' -v
```

Rust 集成测试位于各 crate 的 `tests/`；SDK 测试位于 `python/tests/`。
SDK 的安装验证应使用新 venv 中的 wheel，并在源码树外运行，避免误导入工作区源码。
需要 CLI 或 Router 的用例按各测试文件说明配置；被跳过的用例没有得到执行。

## 可复用工具

| 工具 | 用途 |
| --- | --- |
| `scripts/native_acceptance.py` | 原生构建、Rust 测试、wheel 隔离安装及实际业务检查 |
| `scripts/platform_probe.py` | 安装后的独立进程 Topic、Service、Action 与 provider 重启检查 |
| `scripts/platform_security_probe.py` | TLS/ACL、本机与两台主机分别运行的安全通信检查 |
| `scripts/platform_launch_probe.py` | 已安装程序的启动、依赖和退出检查 |
| `scripts/platform_compatibility_probe.py` | 两个已安装 SDK 版本的通信、升级和回滚检查 |
| `scripts/platform_cli_compatibility_probe.py` | 旧 CLI/新 provider 与新 CLI/旧 provider 的业务检查 |
| `scripts/platform_soak_probe.py` | 按显式配置进行本机负载、基线和持续运行测量 |
| `scripts/check_sdist.py` | 源码归档、执行权限、离线重建和安装检查 |
| `scripts/test_package_mvp.py` | 平台包内容、摘要、安装器和归档结构检查 |

使用实际工具的帮助确认参数，例如：

```sh
python3.11 -I scripts/platform_probe.py --help
python3.11 -I scripts/platform_security_probe.py --help
python3.11 -I scripts/platform_soak_probe.py --help
```

工具输出放在仓库外的新目录。保留完整记录用于定位失败，但不要把主机路径、
原始测试日志、凭据或业务数据作为公开发行内容。

单机检查不能证明跨机部署；短时运行不能证明持续运行能力。
升级版本、平台或网络配置后，应重新运行与实际使用方式相符的检查。
