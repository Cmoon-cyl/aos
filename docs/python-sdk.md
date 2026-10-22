# Python SDK

包名是 `aos_sdk`，原生扩展随 wheel 安装。下面的程序需要已启动的本机 Router；
从完整发行包根目录运行，或把 Interface 文件复制到自己的项目并调整路径。
安装和网络准备见[快速开始](getting-started.md)。

## 一个完整 Service

保存为 `calculator.py`。这个程序读取包内的 AddTwoInts Interface、创建 Node，
注册同步回调，并通过 Executor 持续处理请求：

```python
from pathlib import Path
import aos_sdk as aos

interface = aos.Interface.load(Path("interfaces/AddTwoInts.json"))

with aos.Context() as context, aos.Node(
    "calculator", context=context,
    summary="Adds two integers.", tags=["math"],
) as node, aos.SingleThreadedExecutor(context=context) as executor:
    node.create_service(
        interface, "/add_two_ints",
        lambda request: {"sum": request["a"] + request["b"]},
        summary="Add two integers without changing state.",
        side_effects="read_only", contract_version="1",
    )
    executor.add_node(node)
    print("calculator ready", flush=True)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
```

用另一个进程调用一次：

```python
from pathlib import Path
import aos_sdk as aos

interface = aos.Interface.load(Path("interfaces/AddTwoInts.json"))

with aos.Context() as context, aos.Node(
    "calculator_client", context=context,
) as node, aos.SingleThreadedExecutor(context=context) as executor:
    client = node.create_client(interface, "/add_two_ints")
    executor.add_node(node)
    if not client.wait_for_service(timeout_sec=5):
        raise TimeoutError("calculator was not discovered")
    future = client.call_async({"a": 17, "b": 25}, timeout=5)
    executor.spin_until_future_complete(future, timeout_sec=6)
    print(future.result(timeout=0))  # {"sum": 42}
```

`Interface.load()` 接受 JSON 文件路径或 Python 字典。request 和 response 都由 Rust 校验。
Service 超时只停止本地等待；应用应保留业务请求标识，不自动重新提交有副作用的请求。

## 发布和订阅 Topic

订阅回调需要 Executor spin；不传 callback 时，可以直接调用 subscription 的 `recv(timeout)`。
下面先启动订阅者，再从另一进程启动发布者。

```python
# listener.py
from pathlib import Path
import aos_sdk as aos

interface = aos.Interface.load(Path("interfaces/String.json"))
with aos.Context() as context, aos.Node("listener", context=context) as node:
    node.create_subscription(interface, "/chatter", lambda message: print(message, flush=True))
    try:
        aos.spin(node)
    except KeyboardInterrupt:
        pass
```

```python
# talker.py
from pathlib import Path
import time
import aos_sdk as aos

interface = aos.Interface.load(Path("interfaces/String.json"))
with aos.Context() as context, aos.Node("talker", context=context) as node:
    publisher = node.create_publisher(interface, "/chatter", aos.QoSProfile(depth=10))
    for sequence in range(20):
        publisher.publish({"data": f"hello AOS {sequence}"})
        time.sleep(0.2)
```

发现和连接需要时间，刚启动时的 volatile 消息可能尚无匹配订阅者。
需要保留最后值时，双方显式选择 transient-local，并保持 publisher 存活；
这不是磁盘持久化。完整用例见 [Topic 示例](../examples/typed_sdk.md)。

## Action

`ActionServer(node, interface, name, execute_callback, ...)` 接收 goal，
执行回调通过 `goal.publish_feedback()` 发布进度，最后明确调用
`goal.succeed()`、`goal.abort()` 或 `goal.canceled()`，并返回结果 JSON。
默认接受 goal、开始执行并拒绝取消；业务需要取消时提供 `cancel_callback`，
在执行中的安全位置检查 `goal.is_cancel_requested`。

`ActionClient.send_goal_async()` 返回的 Future 解析为 `ClientGoalHandle`。
先检查 `accepted`，再通过 `get_result_async()` 等待结果；
需要取消时独立调用 `cancel_goal_async()`。
结果等待超时或本地 Future 取消不会取消 provider 的 goal。

耗时同步执行回调占用工作线程。Fibonacci 示例使用四线程
`MultiThreadedExecutor` 和 `ReentrantCallbackGroup`，为反馈、控制和协作取消保留执行机会。
完整代码与返回状态见 [Action 示例](../examples/action_sdk.md)、
[`fibonacci_server.py`](../examples/fibonacci_server.py) 和
[`fibonacci_client.py`](../examples/fibonacci_client.py)。

## 配置与安全

```python
# 开发配置：参数属于程序初值，后续部署配置可能覆盖。
context = aos.Context(config="/absolute/session.json5", domain_id=7)
context.shutdown()

# 调用方需要最高优先级时，使用明确的 CLI 覆盖层。
context = aos.Context(cli_overrides={"domain_id": 7, "zenoh_config": "/absolute/session.json5"})
context.shutdown()

# 安全部署：显式 profile 优先于 AOS_SECURITY_PROFILE。
context = aos.Context(security_profile="/absolute/peer.json")
context.shutdown()
```

这些是独立选择示例，实际程序只使用符合部署要求的配置。
默认、程序初值、文件、允许的环境变量、launch、CLI 覆盖按顺序合并；
最终安全配置不能引入明文回退。细节见[行为契约](platform/behavior.md)和
[安全入口](platform/security-cli.md)。

## 回调、等待与退出

| 场景 | 用法与边界 |
| --- | --- |
| 普通同步回调 | `SingleThreadedExecutor`；回调保持短小，不在互斥组内等待自身才能完成的工作 |
| 多个并发工作 | `MultiThreadedExecutor`；线程数与 CallbackGroup 按业务并发约束设置 |
| 等待一次结果 | `future.result(timeout)` 可以直接等待；`spin_until_future_complete()` 同时调度回调 |
| 用户 done callback | `add_done_callback()` 需要对应 Node 的 Executor 持续 spin |
| asyncio 适配 | Future 提供适配能力，仍需运行 Executor；不是把同步业务回调自动变为协程 |
| 控制等待 | 使用单调时间；模拟时间暂停不应暂停关闭和超时 |
| 释放资源 | 先停止使用端点与回调，再关闭 Executor/Node，最后关闭共享 Context；推荐上下文管理器 |

异常来自 `aos_sdk` 的公开异常类型。`InvalidArgumentError` 表示输入或接口不满足要求；
`ResourceLimitError` 是其子类；`QueueOverflowError`、`NoResponseError` 和等待超时需要分别处理。
通信失败不能证明远端业务没有执行。

## 进一步使用

- [参数、生命周期](platform/managed-cli.md)与[配置、QoS、时钟](platform/behavior.md)。
- [Bag API](platform/bag-runtime.md)、[诊断](sdk-doctor.md)、[有界资源](platform/resources.md)。
- [MemoryServer](platform/memory-server.md)：完整应用中的幂等写入、持久化、导出和取消。
- 原生签名：[`_native.pyi`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/python/aos_sdk/_native.pyi)；Python 包装层：
  [`node.py`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/python/aos_sdk/node.py)、
  [`action.py`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/python/aos_sdk/action.py)、
  [`executors.py`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/python/aos_sdk/executors.py)。这些链接打开 GitHub 源码。
