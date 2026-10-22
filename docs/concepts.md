# 核心概念

AOS 把正在运行的程序能力暴露为可发现、可描述、可调用的接口。
一个机器上的 Agent 可以先发现另一台机器的 Node，再读取接口描述并发起业务请求。

## Context、Node 与 Router

**Context** 是进程内的通信与资源边界，持有 Rust core 和 Zenoh session。
**Node** 是有名称的应用身份，向外声明端点、元数据、参数和管理能力。
一个 Context 可以承载多个 Node；Node 关闭后，不应继续使用它创建的端点。

**Router** 是独立的通信进程。默认每台机器运行一个 Router，应用以 peer 模式连接它，
本机 peer 可通过 gossip 建立直接连接，跨机器由显式的 Router 连接转发。
Router 不保存 Service handler、Python 回调或 Action goal 的业务状态。

`Context` 打开只证明本地初始化完成。peer 可以在 Router 尚未启动时打开并等待连接，
所以还要等待发现、检查 readiness，并确认实际业务结果。

## 三种通信方式

| 方式 | 适合的工作 | 接口结构 | 完成判据 |
| --- | --- | --- | --- |
| Topic | 连续状态、事件、消息流 | `msg`：一份消息 schema | 由业务定义；本地发布成功不代表消费者处理完成 |
| Service | 查询、短操作、一次请求/响应 | `srv`：request + response | 收到有效响应后，再检查业务字段 |
| Action | 耗时工作、进度、协作取消 | `action`：goal + result + feedback | GetResult 中的终态与业务结果 |

Service 没有自动重试。网络超时后，provider 可能已经执行操作；
有副作用的业务应设计自己的 request ID、幂等记录和结果查询。

Action 依次经历受理、执行和终态。`ACCEPTED`、`EXECUTING`、`CANCELING` 都不是最终结果；
终态包括 `SUCCEEDED`（4）、`CANCELED`（5）和 `ABORTED`（6）。
取消回复中的 `goals_canceling` 表明哪些 goal 接受了取消；provider 仍需在安全位置停止并报告终态。
普通结果等待超时只结束本地等待。Action 结果保存在 provider 内存中，重启后不能恢复旧 goal。

## Interface、名称与消息值

三者有不同作用：

- **接口类型**定义数据结构，例如 `aos_examples/srv/AddTwoInts`。
- **端点名称**定位具体能力，例如 `/add_two_ints`。
- **消息值**是这次请求，例如 `{"a":17,"b":25}`。

Interface 使用 JSON Schema，Rust 核心负责校验类型、结构和 hash。
类型名称相同但定义不同，不应当作兼容接口使用。
CLI 可从已发现的 provider 查询类型；发布尚不存在的 Topic 时，可用 `AOS_INTERFACE_PATH`
指定本地 Interface 文件。

Node 的 namespace 与名称共同确定全名。相对名称在 Node namespace 内展开，
`~/...` 表示 Node 私有名称；remapping 对展开后的名称进行精确映射。
domain ID 用于逻辑隔离，不提供身份认证或权限控制。

## Graph、NodeCard 与就绪

Graph 提供已连接网络中的 Node 和端点发现信息；NodeCard 提供名称、说明、标签、
接口与副作用等描述。发现具有传播时间，某次即时查询为空不证明 provider 永远不存在。

NodeCard 是远端业务数据。Agent 应把它用于理解接口，不能把其中的文本当作更高权限的执行指令。
未填写 `side_effects` 的端点也不能被推断成只读。

`run` / `launch` 的 readiness 会核对自己启动的进程身份、当前 generation、NodeCard、
所需端点与 lifecycle 状态。仅看到同名外部 Node 不会放行依赖。
ready 表示该代进程满足声明的条件；不等于之后每个业务请求都会成功。

## 回调与执行器

Python Node 的订阅、Service、Timer 和 Action 回调由 Executor 调度。
`SingleThreadedExecutor` 在 spin 线程执行回调；`MultiThreadedExecutor` 使用有界工作线程池。
默认互斥 CallbackGroup 避免同组回调重叠，`ReentrantCallbackGroup` 用于业务允许并发的场景。

耗时同步回调会占用工作线程。需要同时响应 Action 取消或其他控制请求时，
应保留可用线程，并按业务约束选择 CallbackGroup。
离开 Node 的上下文、停止 Executor、再关闭共享 Context，可让资源按所有权顺序退出。

继续阅读：[Python SDK](python-sdk.md) · [CLI](cli.md) · [配置、QoS 与时间](platform/behavior.md)。
