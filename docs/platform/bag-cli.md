# 录制、检查、恢复与回放

`aos bag` 与 Python SDK 共用 Rust `aos-bag`。文件是 MCAP `aos-json-v1`，
保存完整 Interface、type hash、QoS 和每条消息的接收/来源时间证据。
回放只发布观察 Topic；Action feedback/status、参数事件和 lifecycle 事件的回放
不会执行原始调用、发送或取消 goal、修改参数或推进 lifecycle。

本页描述当前 bag CLI。各平台实际原生验收及制品来源见[发行说明](../releases/v0.2.0.md)；
容器与运行契约见 [bag-runtime.md](bag-runtime.md)。

## CLI

先用 `aos schema bag record` 或 `aos bag play --help` 查看输入与输出协议。
业务输入只通过 `--request PATH|-` 提供。`info` 和 `recover` 不创建网络 Context，
即使 provider 已离线也可读取。`record` 和 `play` 提供 macOS、Linux 和 Windows 实现；
使用与本机匹配的发行包，具体原生验证范围见[支持范围](../support.md)。

录制到一个不存在的新目录：

```json
{
  "directory": "/tmp/demo-recording",
  "topics": ["/demo/status"],
  "duration": 30,
  "status_interval": 1,
  "qos": {"depth": 256, "reliability": "best_effort", "durability": "volatile"}
}
```

将内容存为 `record.json` 后运行 `aos bag record --request record.json`。
选定名称使用绝对 Topic 名，不支持通配符。可用 `all_topics: true` 代替 `topics`，
并通过 `exclude` 排除具体名称。默认录制 30 秒；`follow: true` 持续录制直到信号或
运行错误，与 `duration` 互斥。Unix 上 SIGINT/SIGTERM/SIGHUP/SIGQUIT 请求正常收尾；
Windows 上使用控制台 Ctrl-C/Ctrl-Break。
信号处理器在创建录制或回放句柄前安装；更早的初始网络连接期间保持 OS 默认信号行为。
CLI 读取 `AOS_PROCESS_CONFIG`，按共同配置规则合成 Context 和 Bag 自有 Node 的部署输入。

`recording_started` 只说明句柄已经创建。看 `recording_status.active_channel_count`
确认实际订阅的类型通道，并检查 `unresolved_count` 和 `unresolved_sample`。
同名不同 type hash 分别录制；描述暂不可得时明确保留 unresolved。
状态包含 adapter 与 writer queue 的独立丢样计数，`end_to_end_lost_total: null`
表示没有端到端丢样证据。

队列、单条消息、chunk、分片和总容量都有显式限制。`storage` 的默认值为：
4 MiB chunk、1 秒刷新、256 MiB 或 10 分钟分片、总计 4 GiB。
达到总容量后停止，保留已有文件。磁盘满或写失败会给出失败状态与非零退出码。
正常关闭并不承诺 fsync 级断电零丢失。
录制 `qos` 省略或设为 `null` 时均使用 best_effort / volatile / depth 256。

离线检查：

```json
{"directory": "/tmp/demo-recording"}
```

将内容存为 `info.json`，运行 `aos bag info --request info.json`。
此操作检查所有分片的 SHA、CRC、Interface/hash、消息和配套 metadata；
输出包含完整通道描述。`allow_incomplete: true` 只允许已明确标记不完整的文件，
不跳过校验。

恢复到新目录：

```json
{
  "directory": "/tmp/demo-recording",
  "destination": "/tmp/demo-recovered"
}
```

运行 `aos bag recover --request recover.json`。也可指定单个 `.mcap.partial` 为源。
恢复只接受完整、CRC 校验通过的 chunk，不修改源文件，不跳过损坏位置继续拼接。
结果始终标为 incomplete。目标写入或资源上限失败会报错，
不会把目标错误伪装成源文件损坏后返回正常结束。

## 回放与控制

回放必须显式选择独立 namespace，或指定 `original_names: true`：

```json
{
  "directory": "/tmp/demo-recording",
  "namespace": "/replay",
  "rate": 2,
  "subscriber_wait_ms": 1000,
  "clock": {"topic": "/clock", "period_ms": 20}
}
```

运行 `aos bag play --request play.json`。此例将 `/demo/status` 发布到
`/replay/demo/status`，可选 clock 发布到 `/replay/clock`。
消费者通过现有 remap 和 `use_sim_time` 订阅这个时钟。省略 `clock` 就不会生成它；
回放调度和控制超时始终使用 steady clock。默认不等订阅者。`subscriber_wait_ms`
设为正值时，所有业务和可选 clock 端点都必须匹配才能开始；超时保持 Ready，
错误中列出未匹配端点。start 总预算是订阅者等待时间加 `control_timeout_ms`。
等待期间不发布消息；匹配不代表消费者已经处理消息。
等待订阅者期间仍会检查信号和 stdin 的停止/EOF。检查沿用同一次 start
请求及原截止时间，不重复提交控制操作。
使用原名回放 Action feedback/status 时，在线 ActionClient 也可能收到这些消息；
若 goal_id 重合，客户端观察到的状态可能变化。独立 namespace 可隔离这些观察消息。

`playback_ready` 提供发布前的端点映射、type hash 和有效 QoS。`topic_qos` 或
`channel_qos` 可覆盖 QoS；同名多个类型保留独立 publisher。
namespace 先加前缀，再执行 core remap。结果越出该 namespace、目标碰撞或
生成 clock 与录制 clock 冲突时，在发布前拒绝。

需要交互控制时，在请求中加入 `control_input: "stdin"`，
可同时设置 `start_paused: true`。请求必须来自文件，不能使用 `--request -`。
stdin 每行发送一个 JSON 对象：

```json
{"id":"pause-1","command":"pause"}
{"id":"seek-1","command":"seek","offset_ns":0}
{"id":"rate-1","command":"rate","rate":0.5}
{"id":"resume-1","command":"resume"}
{"id":"inspect-1","command":"status"}
{"id":"stop-1","command":"stop"}
```

每行最多 64 KiB，输入队列最多 32 条。`stop` 和 EOF 优先请求停止回放，
即使 stdout 正在背压也会处理；尚未执行的命令记录在摘要的 `unapplied_controls`。
需要按顺序执行多条控制时，等待每条响应，再发送下一条，并保持输入流打开。
停止命令的响应也保存在 `terminal_control_result`，`input_stop_reason` 说明停止输入。
超过行长、队列满或输入读取失败会停止回放并返回 `CONTROL_INPUT_ERROR`。
通过格式校验的命令，其 `control_result` 保留 `id`、`ok`、状态和原生错误；
其他格式不合法的输入返回 `id: null`。单条格式错误或原生控制失败不终止回放，也不会自动重试。
seek 使用相对于录制开始的**整数纳秒**；不接受小数、布尔值或负数。
暂停冻结位置，暂停中 seek 后仍暂停；Ready 中 seek 后仍保持 Ready，且不发布 clock。
成功的 pause/seek 确认本地 cursor、timeline 和 epoch 已提交；
clock 随后发送，失败会通过状态和错误报告。超时不会留下稍后才执行的控制请求。
已经提交到传输或进入消费者队列的数据无法撤回。

## 输出和收尾

`record`/`play` 输出 JSONL，最后给出 `summary`。进度输出在背压时合并，
`coalesced_status_snapshots` 记录合并次数；控制响应保留顺序。
stdout 队列限制 4 MiB，队列满且连续 30 秒没有写入进展会停止运行。
最后的摘要/错误共享 2 秒输出期限；永久不读输出可能导致最终报告无法送达，
原本成功的命令此时退出 1；已有的运行错误或信号退出码优先保留。
停止检查和原生 worker 收尾不依赖 stdout 写入完成。

`summary.resources_closed: true` 且 `cleanup_error: null` 证明原生 worker、文件和自身 Node 已收尾。
它不代表业务成功；仍须检查 `state`、`error`、`stop_reason`、`incomplete` 和丢样计数。
CLI Context 有独立的关闭预算；超时仍报错。正常完成退出 0，输入或损坏文件 2，
传输问题 3，控制/关闭超时 4，磁盘/运行/输出失败 1；
Unix 上运行期 SIGHUP/SIGINT/SIGQUIT/SIGTERM 正常收尾后分别退出 129/130/131/143；
Windows 控制台中断正常收尾后退出 130。
`shutdown_timeout` 默认 5 秒。大队列排空和分片收尾可能超过预算；
超时退出不确认资源已关闭，进程退出后可能留下 `.partial`，需要离线恢复。

## Python

```python
import aos_sdk as aos

context = aos.Context()
with aos.BagRecorder(context, "/tmp/sdk-recording", topics=["/demo/status"]) as recorder:
    # 由程序决定录制时长，并检查 active_channels / unresolved / counters。
    observe_application(recorder)

info = aos.inspect_bag("/tmp/sdk-recording")
with aos.BagPlayer(context, "/tmp/sdk-recording",
                   namespace="/replay", start_paused=True) as player:
    player.start()
    player.seek(0)
    player.set_rate(2.0)
    player.resume()
    report = player.wait(timeout=30)
context.shutdown()
```

构造函数接受 `storage`、`node`、队列限制和 QoS 等关键字选项，由 Rust 校验。
`stop()` 只请求停止；`wait(timeout)` 返回 worker 结束后的报告，其中仍可能含运行或清理失败。
用 `status.resources_closed` 和 `status.cleanup_error` 核对清理结果。
`close(timeout)` 请求停止并等待，最终失败时抛出 `BagError`；`with` 退出调用 `close()`。
`BagError.kind` 与 `file_offset` 保留诊断；`kind == "timeout"` 不代表关闭已经完成。
输入不合法统一使用 `InvalidArgumentError`。`BagError.report` 在关闭报告失败时保留完整报告，
其他原生错误为 `None`。若 `with` 业务代码和关闭同时失败，原业务异常保持主异常，
关闭错误保存在该异常的 `bag_cleanup_error` 和异常注记中。
Bag 对象只关闭自己拥有的 Node，调用方的 Context 保持可用。
所有可能等待文件、网络或派发屏障的绑定都释放 GIL。
录制/回放的 `wait()` 及 Player 控制等待会定期检查 Python 信号，保留原
`KeyboardInterrupt` 或自定义信号异常。中断本地等待不自动关闭句柄或 Context；
尚未提交的控制会取消，若与提交同时发生，应读取 `status` 确认最终状态。
`inspect_bag`、`recover_bag` 和 Player 构造中的离线文件扫描不提供逐次信号检查；
释放 GIL 不代表底层文件系统调用可以取消。
