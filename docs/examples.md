# 示例选择

示例随完整平台包提供。普通 Python 示例从包根目录运行，解释器使用已安装 wheel 的 venv。
先启动 Router；所有进程使用一致的 domain 和 session 配置。

| 想体验什么 | 入口 | 预期结果 |
| --- | --- | --- |
| Topic 发布/订阅 | `examples.typed_listener` 与 `examples.typed_talker` | 收到实际文本计数消息 |
| Service 请求/响应 | `examples.add_two_ints_server` 与 `examples.add_two_ints_client` | 输入 17、25 得到 42 |
| Action 反馈、成功和取消 | `examples.fibonacci_server` 与 `examples.fibonacci_client` | 反馈序列、真实终态和取消回复 |
| 参数、QoS、Clock/Timer | `examples.platform_foundation` | 按示例输出有效配置与时间行为 |
| LifecycleNode | `examples.lifecycle_node` | 通过管理接口改变生命周期并检查状态 |
| 安装包与依赖启动 | `scripts/install_demo_package.py` | 从安装前缀启动 provider/consumer，ready 后放行依赖 |
| 持久化业务 | `scripts/install_memory_package.py` | Put/Get/List、实际 JSONL 导出与状态订阅 |
| 请求诊断观察 | Memory 安装器的 `--export-request-telemetry` | 脱敏元数据、独立观察与 bag 回放 |

## Topic

先在终端 1 运行 listener，保持运行，再在终端 2 运行 talker：

```sh
python -m examples.typed_listener --count 3 --config config/zenoh-session.json5
python -m examples.typed_talker --config config/zenoh-session.json5
```

体验最后值保留时，先启动 `typed_talker --count 1 --transient-local --hold 20`，
再在它仍存活时运行 `typed_listener --count 1 --transient-local`。
双方使用相同配置；publisher 退出后不提供持久消息历史。

## Service 与 Action

[快速开始](getting-started.md)给出三平台命令、执行顺序和结果判据。
API 解释见 [Topic/Service](../examples/typed_sdk.md) 与 [Action](../examples/action_sdk.md)。

## MemoryServer

MemoryServer 是包含 SQLite、幂等写入、生命周期、参数、Action 导出与 Topic 状态的完整应用。
从[安装与第一次调用](platform/memory-server.md#从安装到第一次调用)开始。
安装器只写新 prefix；实际业务数据由 provider 启动后在调用方指定的目录创建。
消费者输出 `demo_completed` 表示这一轮真实业务已核对，provider 会继续运行直到显式停止。

## 维护者探针

源码仓库还包含原生、跨机、权限、重启与混合负载探针。
这些用于固定来源的验收，不是平台包用户安装时必须执行的步骤。
运行入口见[开发与发行](development.md)；历史探针结果不能代替当前制品的测试。
