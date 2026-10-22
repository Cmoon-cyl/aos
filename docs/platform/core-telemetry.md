# Core 业务遥测

`Context::telemetry()` 与支持遥测的 transport adapter 共用一个进程内记录器；旧 adapter 使用 Context 本地记录器。默认保留有界计数，显式 `enable_log_capture(capacity)` 后才保留 JSONL 记录。记录函数不执行 I/O、不调用应用回调、不推进业务状态。日志输出由调用方显式 drain 或 flush 到 stderr。

正式示例的 [请求诊断适配器](request-observer.md)复用这三个公共 capture/drain/snapshot 接口，显式导出真实 Service/Action metadata 到固定 Topic。它独占源 Context 的 drain，拒绝已有 capture/flush 组合，并用 capture 关闭的独立 Context 发布。该适配器不改变 Core 事件、ID 或业务状态；真实接线验收与离线规则测试分别报告。

## 事件与关联

下表中的事件名称没有额外前缀。`component`、`event`、`outcome` 构成计数键；Node、端点、请求和 Goal 标识只放在记录中，不作为计数标签。

| component | event | 发生点与 request_id |
| --- | --- | --- |
| node | `created` | Node 完成创建和内置服务安装。包含 Node ID 与全限定名称。 |
| node | `destroyed` | 首次显式销毁或最后一个 Node 所有者释放；包含清理结果。Context 关闭会先使资源计数归零，`destroyed` 可在 Node 随后释放时记录。 |
| service | `callback_completed` | 受理的 `ServiceRequest` 首次提交回复之后；使用 adapter 提供的请求 ID。错误回复与回复发送失败都记录有限 outcome。`duration_ns` 从请求交给调用方开始，至回复提交返回，包含调用方等待、回调和回复提交时间，不包含传输队列等待。 |
| action | `goal_accepted`、`goal_rejected` | Provider 作出受理决定，包括重复 Goal ID 和未处理 admission 的默认拒绝；使用 send_goal 请求 ID。接受事件证明 provider 已登记 Goal，不证明客户端已收到回复。 |
| action | `execution_started` | 首次安排 Goal 执行；使用该 Goal 的 send_goal 请求 ID。 |
| action | `cancel_requested`、`goal_succeeded`、`goal_aborted`、`goal_canceled` | Provider 已完成相应状态变化；使用 send_goal 请求 ID。 |
| action | `result_completed` | Provider 已缓存执行结果或错误；使用 send_goal 请求 ID。它与终态事件分开，终态不代表执行结果已经可用。 |
| action | `result_sent` | 已尝试响应一个 get_result 请求；使用该 get_result 的请求 ID，并保留 Goal ID。有限 outcome 区分结果错误和发送错误。 |
| action | `cancel_accepted`、`cancel_rejected` | 每个取消候选的最终决定；使用独立 cancel 请求 ID。无候选时记录请求中的 Goal ID；默认拒绝也有记录。 |

所有 Action 记录都带 Action 的基础端点；底层 Service 记录使用对应 `/_action/...` 端点。Goal 状态、结果缓存和 registry 锁释放后才写业务记录，因此日志不会通过持有这些锁阻塞回调或 transport。并发事件的记录先后不替代业务状态机顺序；记录本身也不证明应用回调或远端处理成功。

请求 ID 必须来自 adapter 接收到的真实请求。缺失时保持空值，core 不伪造远端关联；它是未经认证的诊断字段。记录结构不接受载荷、配置、任意错误文本。`outcome=ok` 表示该记录描述的本地操作成功，不表示 Goal 最终成功或远端业务完成。

## 资源与边界

Core 计数逻辑 Node、ActionServer 和 ActionClient；adapter 负责底层 Topic、Service 和 transport 资源。克隆同一 Node 不增加计数，重复 close/Drop 不重复减计数；Context 关闭会释放已注册的逻辑资源计数。监控这些计数不能推断操作系统句柄或网络资源已经完成回收。

记录器最多保留 64 种计数键、4096 条捕获记录；超出计数和记录容量的损失有单独计数。Core 的业务完成计数与 adapter 的请求/回复计数描述不同阶段，不能相加为请求总量。`callback_completed` 不包含未回复而被调用方直接丢弃的 ServiceRequest；transport 可记录其后续超时或关闭。

本地 typed 测试验证资源计数、成功/失败回复、重复提交、Action admission/取消/结果关联及敏感错误文本排除。跨进程关联由 Zenoh 独立进程测试验证，内存 transport 测试本身不构成跨机验收。
