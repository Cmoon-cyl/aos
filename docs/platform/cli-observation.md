# 发布、条件观察和诊断

这些命令使用与 SDK 相同的 Rust core 和 Zenoh adapter。先配置
`AOS_ZENOH_CONFIG`、`AOS_DOMAIN_ID`，运行 `aos --help` 和
`aos schema GROUP VERB` 读取当前二进制的输入、输出和默认值。
以下示例中的 `aos` 应指向本次安装的 CLI。

## 发布消息

`topic pub` 默认立即提交一条消息，默认不等待订阅者。发布新 Topic 前，
用 `AOS_INTERFACE_PATH` 指定本地 Interface 文件；已有 provider 提供该类型时，
CLI 也能查询其描述。

```sh
export AOS_INTERFACE_PATH="$PWD/interfaces/String.json"
aos topic pub --request - <<'JSON'
{"topic_name":"/demo/state","message_type":"aos_examples/msg/String","values":{"data":"ready"}}
JSON
```

| 需要的行为 | 请求字段 |
| --- | --- |
| 一条消息 | 省略模式字段，或写 `once:true` |
| 指定次数 | `times:3`；首条立即提交 |
| 持续发布 | `follow:true`；Ctrl-C 停止 |
| 发布频率 | `rate:10`，单位 Hz，默认 1 |
| 显式等待订阅者 | `wait_for_subscribers:1, match_timeout:5` |
| 提交后保留 publisher | `linger:0.2`，单位秒 |

`once:true`、`times`、`follow:true` 互斥；`once:false` 无效。
次数、订阅者数量、消息窗口和日志容量使用 JSON 整数，例如 `3`。
`3.0`、`3e0` 等浮点表示会在打开 session 前报 `INVALID_ARGUMENT`，不会使用默认值。
调度使用单调时间，阻塞后不会突发补发积欠消息。`times` 的计数表示本地提交成功次数，
不表示远端消费成功；`linger` 也不是消费者确认。

等待订阅者时，CLI 核对 Graph 中的端点名称、类型名和类型 hash，再检查 transport
是否存在至少一条匹配路由。数量是匹配身份的 Graph Subscription 数；QoS 兼容性仍为
诊断，不能据此推断 DDS 式匹配拒绝或所有消费者已处理消息。

`discovery_timeout` 限制类型发现，`match_timeout` 限制显式匹配等待。
旧 `timeout` 是这两项上限的别名，不能与任一新字段混用，也不会自动开启等待。
发布过程输出 `published` 事件和最终 `summary`，后者包含 `submitted_count`、
`observed_subscribers`、`requested_subscribers`、`stop_reason`。

## 观察完整消息或选定字段

```sh
aos topic echo --request - <<'JSON'
{
  "topic_name":"/demo/state",
  "timeout":15,
  "idle_timeout":3,
  "count":5,
  "filter":{"path":"/data","op":"ne","value":"starting"},
  "fields":["/data"]
}
JSON
```

默认观察窗口为订阅创建后的 30 秒。`follow:true` 只有在未提供 `timeout` 时才无限等待；
`once:true` 等价于通过过滤的一条消息；`count` 统计通过过滤的消息。
`discovery_timeout` 单独限制发现和类型描述查询。

每条消息按以下顺序处理：

1. 取出完整消息，更新接收计数和空闲时间。
2. 应用 `filter`。
3. 对通过过滤的完整消息执行 hook。
4. 按 `fields` 生成输出。
5. 检查 hook 命中和数量条件；同一条消息同时满足时，报告 `hook_matched`。

`fields` 使用 JSON Pointer，例如 `/data`、`/items/0/value`。
指定后，输出是 Pointer 到值的对象，例如 `{"/data":"ready"}`。
不存在的 Pointer 被省略，并计入 `projection_missing_fields`。
字段投影不会删掉 hook 的输入。

条件支持 `eq`、`ne`、`gt`、`gte`、`lt`、`lte`、`exists`，以及递归的 `all`、`any`。
比较采用严格 JSON 类型；数字按数值比较，不把大整数先转成浮点数。
缺失字段不等于 `null`；缺失字段以及类型不同的比较（包括 `ne`）均不匹配。
`exists` 在字段存在时匹配。

`summary.received` 是总时限内取出的有效消息数，`filtered` 是通过过滤的数量。
被过滤掉的有效消息会重置 `idle_timeout`，但不满足 `count`。
空闲判断基于取出的消息；尚有排队消息时先处理队列。

## 等待字段条件

```sh
aos topic echo --request - <<'JSON'
{
  "topic_name":"/demo/state",
  "timeout":30,
  "hook":{
    "kind":"condition",
    "condition":{"path":"/data","op":"eq","value":"ready"},
    "consecutive_matches":2
  }
}
JSON
```

`consecutive_matches` 默认 1，统计通过过滤的消息流中的连续命中。
任一不匹配消息会清零连续次数。最终 `summary.matched` 保留完整命中消息、
输出投影和 hook 结果。

## 使用本地脚本判断

脚本由本地 CLI 按显式 executable/argv 启动，不经过 shell。
脚本读取一份 JSON 输入并输出一份 JSON 结果：

```json
{
  "message":{"data":"ready"},
  "topic":"/demo/state",
  "sequence":3,
  "received_at":{"time_domain":"system","time_ns":1234567890,"scope":"adapter_delivery"}
}
```

输入中的消息不受 `fields` 投影影响。时间元数据不可用时相应字段为空并说明原因；
接收时间不代表源端发布时间。

```python
import json
import sys

record = json.load(sys.stdin)
matched = record["message"]["data"] == "ready"
print(json.dumps({
    "decision": "matched" if matched else "continue",
    "reason": "state inspected",
}))
```

将它保存为本地脚本后，在请求中提供：

```json
{
  "topic_name":"/demo/state",
  "timeout":30,
  "hook":{
    "kind":"script",
    "executable":"/absolute/path/to/python",
    "argv":["-I","/absolute/path/to/check_state.py"],
    "timeout":1,
    "max_output_bytes":65536
  }
}
```

结果的 `decision` 为 `continue`、`matched` 或 `error`；`reason` 可选。
非零退出、非法 JSON、超出输出上限、脚本超时和脚本显式报错分别有错误码。
stderr 上限为 64 KiB；脚本默认 stdout 上限也是 64 KiB。
任一流超过上限都会终止脚本并报 `HOOK_OUTPUT_LIMIT`，已捕获的诊断最多保留到上限。
脚本运行在观察线程，传输回调继续入队。若队列丢样，hook 判定不再完整，
观察以 `hook_overflow` 结束并报告丢弃量。

总观察时限包含脚本执行。先到总时限时报告 `total_timeout`，先到脚本时限时报告
`hook_error` / `HOOK_TIMEOUT`。Ctrl-C 报告 `interrupted`。
Mac/Linux 的脚本使用专属进程组，返回前清理该组并回收直接子进程；
脚本不得自行 daemonize 或脱离进程组。停止观察不会停止远端 provider。
CLI 被 SIGKILL 或操作系统强制终止时，无法执行进程组清理；需要外层进程管理器兜底。

## 显式重复 Service 调用

```sh
aos service call --request - <<'JSON'
{
  "service_name":"/add_two_ints",
  "service_type":"aos_examples/srv/AddTwoInts",
  "values":{"a":17,"b":25},
  "times":3,
  "interval":0.5,
  "timeout":5
}
JSON
```

未提供 `times` 时保持单次 JSON 回复。显式提供 `times`（包括 1）时输出 JSONL：
每次调用一条 `response`，最后一条 `summary`。
每次收到回复后才等待 `interval` 并开始下一次调用；遇到首个失败立即停止，不自动重试。
`interval` 仅在显式 `times` 下有效。

响应中的 `request_id_scope:"CLI operation"` 表示本地调用编号；
跨进程 wire 请求 ID 在结构化遥测记录中，两者不能混用。
超时或 Ctrl-C 结束本地等待，不能据此断言 provider 未执行，也不会自动取消业务。

## 频率、吞吐和延迟

`topic hz`、`topic bw`、`topic delay` 共享观察参数，并接受
`window`（默认 1000 条）及 `report_interval`（默认 1 秒）。
它们定期输出 `statistics`，结束时输出 `summary`。

频率按 adapter 交付时刻的单调时间间隔计算；带宽按原始序列化业务 payload 字节数计算，
不包括网络包头和 transport 开销。过滤生效后只统计选中消息；
历史补发和恢复交付也属于交付样本。窗口首条只锚定时间，带宽分子只计后续条目的字节数。
样本不足、接收时间缺失或乱序时返回 `unavailable`。

当前 Zenoh HLC 可能由 router 注入或改写，不能证明源端发布时间和跨机器时钟可比。
因此 `delay` 返回 `unavailable` 及原因，不生成单向延迟数值。
这不影响 `hz`、`bw` 的本地接收测量。

## Doctor 和查询总时限

```sh
aos doctor report --request - <<'JSON'
{"python":"/absolute/path/to/venv/bin/python","timeout":5}
JSON
```

省略 `python` 时，CLI 明确返回 `python_native_module.status:"not_checked"`。
提供后，CLI 启动隔离子进程，检查该解释器安装的 SDK、原生模块、配置及通信初始化；
缺 SDK、加载失败或超时会保留在报告中，不安装包，不发送业务请求。
这里的退出码 0 只表示成功生成诊断报告；自动化必须继续检查 `data.status` 和子项状态。
认证、ACL、加密没有得到证明时保持 `unknown`。

Graph/NodeCard 的 `timeout` 是打开 session 后的共享查询预算，不按 Node 数量相乘。
`node list` 在预算耗尽后保留已发现的 Node，并给未完成的描述项附错误；
`descriptions_complete` 表示该次描述是否完整，不代表网络中所有 Node 都已发现。
连接打开、清理和输出 I/O 不属于该查询预算。

## 日志与基础指标

CLI 在线请求可显式开启：

```json
{"logging":{"stderr":true,"capacity":1024}}
```

该对象应与命令业务字段合并。业务 stdout 保持原有 JSON/JSONL；
诊断写到 stderr，最后输出一条 `telemetry_summary`。
每条记录包含进程/Context、事件、有限 outcome，以及可获得的 Node、端点、请求和 Goal ID。
业务 payload、配置内容和任意远端错误文本不会写入遥测。
所有关联 ID 都是未经认证的诊断信息，不能用于授权。

Python 可直接读取当前 Context：

```python
context.enable_log_capture(256)
# 运行应用操作
records = context.drain_log_records()
snapshot = context.telemetry_snapshot()
context.flush_logs_to_stderr()
```

日志默认关闭，计数默认开启。捕获容量为 1–4096 条，指标最多 64 种计数键；
淘汰、写入失败和拒绝记录都有计数。Context 关闭后仍可读取累计值。
`SingleThreadedExecutor` / `MultiThreadedExecutor` 可显式设置 `flush_logs=True`，
由 spin 调用线程刷新日志，默认不刷新。

记录回调只更新有界内存；flush 的 stderr I/O 可能阻塞调用线程。
启用日志后，stderr 写失败会以 `LOG_OUTPUT_ERROR`、退出码 1 终止本地操作。
单次结果可能尚未写到 stdout；重复 Service 调用会停止本地等待，Action 不因此自动取消。
这类输出失败不能证明远端业务未执行。SDK Executor 则记录日志丢失并继续派发回调。
基础指标和活动资源反映当前进程的 AOS 所有权，不代表操作系统 FD/RSS，
也不代表远端聚合指标或业务成功率。
详细关联与计数边界见 [Core 业务遥测](core-telemetry.md)和
[Zenoh 传输遥测](zenoh-telemetry.md)。

## 结束原因和退出码

| 情况 | 输出与退出码 |
| --- | --- |
| 达到次数、hook 命中、测量窗口结束 | `summary`，退出 0 |
| echo 总时限、任意观察空闲时限到期 | `summary` 说明原因，退出 4 |
| hook 脚本出错 | `hook_error` 与 `summary`，退出 1 |
| hook 队列丢样 | `hook_overflow`，退出 3 |
| 无效请求 | 在打开 session 前拒绝，退出 2 |
| 收到不符合 Interface 的远端消息 | `INVALID_ARGUMENT`，退出 2；错误阶段为 `execute` |
| Ctrl-C | `interrupted`，退出 130 |

错误输出还包括命令、当前阶段、本地 operation ID，以及已解析的端点或 Goal ID。
请求/结果里的 Interface 描述仍是业务数据，不能当作 Agent 指令执行。
