# Zenoh 传输遥测

Zenoh adapter 与 `Context` 共用一个 `Telemetry` 句柄。默认只保留有界计数；调用 `enable_log_capture(capacity)` 后才捕获记录。应用在自己的操作线程调用 `drain_telemetry()`，或调用 `flush_logs_to_stderr()` 输出 JSONL。中间件回调不写日志文件、stdout 或 stderr，也不启动日志线程。flush 的 I/O 失败不会自动重试；已取出但未写完的记录计入丢弃量，应用按返回错误处理。

需要观察打开会话的结果时，先创建 `Telemetry` 并启用捕获，再传给 `ZenohTransport::open_with_telemetry(config, domain_id, telemetry)`。配置值、载荷和任意错误文本不会进入记录。

## 计数与资源

`telemetry_snapshot()` 返回事件、失败、字节数、耗时累计值，以及活动资源计数。每个计数键只包含 `component`、`event`、`outcome`，最多保留 64 个键。Node、端点、请求和 Goal 标识不作为计数标签。日志最多捕获 4096 条记录；缓存淘汰、写入失败和拒绝记录均有单独计数。

一次调用可以产生接收、取出、回复和完成等多条事件。因此，`events_total` 与 `failures_total` 是事件计数，不能直接作为请求量或请求失败率。字节累计包含各阶段记录的载荷大小及失败尝试，不代表网络吞吐量。排查调用结果应查看 `zenoh.service.call_finished`，并结合有限 `outcome`；排查队列压力还可读取实体的 `queue_stats()`。

Adapter 统计底层 Topic、Service、Graph 和 Transport 资源。显式关闭或最终 Drop 各释放一次；会话关闭会先将仍被应用持有的实体计数归零。Core 单独统计逻辑 Node/Action 资源，定义见 [Core 业务遥测](core-telemetry.md)。这些计数描述 AOS 的资源所有权，不等于操作系统句柄统计。

## 请求关联

每次实际查询生成一个请求 ID，以小型 JSON 附件随 Zenoh 查询发送：

```json
{"schema_version":1,"request_id":"550e8400-e29b-41d4-a716-446655440000"}
```

附件最多 128 字节，只接受版本 1 的两个字段以及 36 字符十六进制 UUID 格式。接收方通过 `ServiceRequest::request_id()` 读取它；没有附件或附件无效时返回空值。成功回复回传有效 ID。附件的缺失、无效或不匹配仅影响诊断，不改变业务结果。

| `correlation_source` | 可确认的关联来源 |
| --- | --- |
| `outgoing_attachment` | 本次提交查询使用的附件。 |
| `incoming_attachment` | Provider 从收到的查询附件读取。 |
| `reply_attachment` | 成功回复带有有效 ID；客户端还核对其与本地请求一致。 |
| `query_context` | 本地查询回调或已有请求上下文。 |
| `missing_attachment` | 对端没有附件；客户端记录仍可保留本地请求 ID。 |
| `invalid_attachment` | 附件不符合大小、版本或字段约束。 |
| `mismatched_attachment` | 成功回复的 ID 与本地查询不同；记录保留本地查询 ID。 |

Zenoh 1.10.1 的 `ReplyErrBuilder` 没有附件接口，错误回复使用 `query_context`。接收端不会把它描述成对端回传的 ID。所有标识都未经认证，`identity_trust` 固定为 `unauthenticated`；它们不用于授权、路由或去重。

`zenoh.*` 事件的 `endpoint` 是实际 wire key，包含 domain、名称和类型身份。Core 业务事件使用逻辑端点，并补充其拥有的 Node/Goal 标识。Adapter 不从业务 JSON 推断 Goal 状态。

## 事件边界与验收

`request_received` 表示查询进入 adapter 回调；`request_taken` 表示请求从队列交给上层；`reply_sent` 表示本地回复提交返回。它们都不单独证明远端业务完成。Action 的受理、状态和结果事件由 Provider 的现有状态机记录，send_goal、cancel_goal、get_result 各自保留实际请求 ID。

以下测试启动两个独立应用进程，通过父进程中的 Zenoh router 交换数据：

```sh
cargo test -p aos-zenoh --test telemetry_process --locked
```

测试检查 Service 的两种客户端路径；Action 的成功、拒绝受理、拒绝后接受取消、结果错误；以及九次 Action 协议调用的请求 ID、Provider Goal ID、Node ID 和独立进程 ID。测试严格检查 stdout 只含应用 JSON，stderr 只含显式刷新的结构化记录，载荷与任意错误文本没有进入日志，Context 关闭后资源计数归零。这是本机 TCP 跨进程证据；记录里的单调时间仍只在本地句柄内可比较。
