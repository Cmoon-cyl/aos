# Core 请求诊断的导出、录制与只读回放

本页描述可选的 Python 诊断适配器、安装接口和验证条件。各次真实 Core → Topic → MCAP、跨机回放及制品验证只对其记录的来源有效；当前发行的范围见[发行说明](../releases/v0.2.0.md)。离线测试只验证规则与资源，不能单独证明实时链路或回放期间没有业务调用。

`examples/memory_server/request_observer.py` 提供 `RequestTelemetryExporter` 和只读 observer 入口。provider 与 consumer 在各自进程内调用公共 `Context.enable_log_capture`、`drain_log_records`、`telemetry_snapshot`，导出真实 Service/Action metadata。它不从 MemoryStore 日志重构事件，不读取便笺正文，也不推断或改写 request UUID、Goal ID。

## 安装和显式启用

使用已安装候选 wheel 的 Python，把包安装到新前缀。安装动作不打开 Zenoh 会话或业务目录。

```bash
MEMORY_PYTHON=/absolute/installed-env/bin/python
MEMORY_PREFIX=/absolute/new-memory-prefix
MEMORY_CONFIG=/absolute/session.json5

"$MEMORY_PYTHON" -I /absolute/aos/scripts/install_memory_package.py \
  --prefix "$MEMORY_PREFIX" --work-dir /absolute/owned-synthetic-data \
  --config "$MEMORY_CONFIG" --domain-id 2021 --namespace /memory \
  --export-request-telemetry
```

省略 `--export-request-telemetry` 时 provider/consumer 都不捕获、不导出。安装包始终包含以下资源，便于同一 observer 消费实时或回放 Topic。

| 安装位置 / 名称 | 用途 |
|---|---|
| `lib/aos_memory/request_observer.py` | provider/consumer 使用的适配器；同文件提供只读 CLI |
| 入口 `aos_memory/request_observer` | Unix 使用 `lib/aos_memory/request_observer`；Windows 在 manifest 中显式绑定绝对 Python 与 `.py`，均以 `-I -u` 启动 |
| `share/aos_memory/interfaces/RequestTelemetry.json`，资源 `request_telemetry` | 固定 Message 类型 `aos_observation/msg/RequestTelemetryV1` |
| `share/aos_memory/installation.json` | 保存启用状态、实际导出 Node/Topic、文件与 native 哈希 |

安装参数生成的 provider、consumer run/launch 请求都带各自 `--telemetry-topic`。默认对应关系如下；修改安装 namespace 后同步使用实际名称。

| 进程 | 独立导出 Node | 诊断 Topic |
|---|---|---|
| provider | `/memory/server_request_exporter` | `/memory/server/request_telemetry` |
| consumer | `/memory/consumer_request_exporter` | `/memory/consumer/request_telemetry` |

源程序直接启动时也可显式传入 `--telemetry-topic /absolute/topic`。consumer 的全局参数放在 `put/get/list/export/observe/demo` 子命令前。源开发通过 `--interfaces` 定位接口目录；安装入口通过 package index 读取资源，不依赖 cwd。

导出 Context 使用与源程序相同的显式 Context 参数和进程环境，重新打开独立 session；它不共享源 Context 的 capture。TLS 部署沿用对应 provider/consumer profile。必须为新 Node、精确诊断 Topic 及必要的发现/描述资源配置 ACL；独立 observer、bag recorder/player 的身份和权限由部署显式选择。不要为了诊断扩大整个 namespace 的业务 query 权限。安装器只保存接线，不证明证书、ACL 或路由已验证。

## 记录、边界和状态

每个批次包含 `schema_version=1`、独立 `stream_id`、递增 `batch_sequence`、源 Context/process ID、`source_before/source_after`、累计 `statistics` 和原始 `records`。空 records 的心跳约每秒一次；它证明某次本地发布尝试，不证明接收完整。

record 原样保留 Core 的 `schema_version/process_id/context_id/sequence/system_time_unix_ns/steady_time_ns/identity_trust/component/event/outcome`，以及实际存在的 `node_id/endpoint/request_id/goal_id/correlation_source/bytes/duration_ns`。原字段缺失仍然缺失，wall time 为 null 时仍为 null。Core request UUID 和 Memory Put 的业务幂等键不是同一概念；业务 case ID 应另存在验收记录中。`identity_trust` 保持 `unauthenticated`，TLS 不会把原 metadata 变成远端身份认证证明。steady time 只在原 Context 内可比较。

| 资源 | 默认 / 硬上限 |
|---|---|
| Core capture ring | 默认 256 条；API 最大 4096 条，Core KEEP_LAST 溢出另计 |
| 一次 drain 的暂存列表 | 至多本次 Core ring 容量；不持有 Core 的记录锁做发布 I/O |
| 适配器待发布数据 | 默认 512 条及 256 KiB；API 最大 4096 条及 1 MiB，超限丢新记录 |
| 单个诊断批次 | 最多 32 条、128 KiB JSON 字节，另有固定字段/字符串/整数限制 |
| 导出 Topic | 显式 best_effort、volatile、depth 8 |
| observer | 1..4096 批次、最多 64 个 stream；默认 4 MiB、最大 16 MiB 批次输出，另有一条固定大小结束报告 |

worker 默认每 50 ms drain，发布在该 worker 中执行，不进入业务 callback 或 telemetry 锁。发布失败的该批记录不重发，业务调用也不重试。没有 subscriber 时 best_effort/volatile 不保留历史；recorders/observers 应先建立订阅，并在证据中保留实际覆盖窗口。不能把晚加入之前的数据缺口猜成零。

`source_before/source_after` 保存 drain 前后两次独立读取的 Core counters，包含 capture drop、日志写失败、拒绝事件、metric key 拒绝、排队数；它们不是原子快照。`filtered_records_total` 记录非 Service/Action 元数据的过滤数，原始 sequence 的过滤空隙不是丢样。适配器分别报告非法记录、队列丢弃的条数/字节、单条超限、发布尝试、submitted、发布异常和失败批次记录数。`source_errors_total` 表示收集/导出 worker 未正常继续；独立 Context 被意外打开 capture 另计 `export_capture_errors_total`。

批次里的发布计数取自本次提交前，`publish_attempts_total` 已包含当前尝试；本次成功或失败要在后续批次或最终本地报告查看。`publish_submitted_total` 只表示本地 `publish` 返回，`end_to_end_delivery` 始终 unknown。中间件、observer、bag adapter/writer 的计数分别核对，不能用 submitted 推导 delivered。

## 独占 drain 和关闭

正式程序在创建业务 Node 前启用 exporter，实际 executor 明确使用 `flush_logs=False`，并交给 exporter 检查。一个进程内的同一个 Context 只能登记一个 owner；已启用 capture、存在未 drain 的旧记录、已配置自动 flush、未知 executor 类型或第二个 exporter 都拒绝组合。业务执行器与 Node 先停止/关闭，再停 capture、收集尾部记录并关闭独立导出 session。

SDK 目前没有全局读游标或阻止任意第三方直接 drain 的锁。因此调用方仍须保证没有其他手工 `drain_log_records`、`flush_logs_to_stderr` 或后来启用的自动 flush；这里不会宣称能拦截未知第三方消费者。需要两种输出时，应由同一个 owner 分发已经取出的同一批记录。

`close(timeout=2)` 有界等待 worker；若发布或关闭阻塞，返回 `worker_stopped=false` / `cleanup_complete=false`，不会声称清理完成，也不会提前把仍运行的 worker 的 ownership 交给第二个 exporter。worker 为 daemon；进程退出可能丢弃尚未发布的记录。`request_telemetry_exporter_stopped` 保存最终状态、累计计数、剩余队列和另一次 `source_current` 快照；后者可看到 worker 失败后还留在源 ring 的记录，读取失败则明确为 null 并报告 `source_current_read_failed`。运行中的诊断失败不会重写业务成功/失败结果；验收必须单独检查这个报告。

## 同一个只读 observer

实时订阅示例：

```bash
AOS_PREFIX_PATH="$MEMORY_PREFIX" "$MEMORY_PREFIX/lib/aos_memory/request_observer" \
  --config "$MEMORY_CONFIG" --domain-id 2021 \
  --topic /memory/server/request_telemetry --count 10 --timeout 20
```

Windows 使用同一个 `.py` 入口：设置 `$env:AOS_PREFIX_PATH` 后，以安装 wheel 的 Python 执行 `-I -u <prefix>/lib/aos_memory/request_observer.py`，其余参数保持相同。

TLS 时以 `--security-profile /absolute/observer-profile.json` 替换 `--config`。回放时使用同一入口，只把 Topic 改成明确隔离的路径，例如 `/replay_case01/memory/server/request_telemetry`。observer 仅创建普通 Node 和 subscription，关闭参数服务，不创建业务 ServiceClient/ActionClient；记录中的 endpoint、event 和 Goal ID 只作为数据输出。

每条 `request_telemetry_batch` 输出经过相同的有界解析路径。结束报告分别给出实际收到的批次/记录、重复或乱序批次、首次观察之后的序号缺口、输出字节和停止原因。首次批次之前的交付状态明确 unknown。收到指定批次数退出 0，超时或预算到达退出 1；格式、SDK 或输出错误失败退出。接收超时使用本地 monotonic；阻塞的 stdout 接收端不受这个接收预算控制。

bag 按普通 Message 保存该固定 Interface、hash、QoS 与原批次。未开启 capture、尚未发布或实际丢失的数据仍不可得。录制/回放路径不为补全 metadata 发起业务 Service、send_goal、get_result 或 cancel。回放使用独立 namespace，默认 remap 不可逃逸；具体命令与策略见 [bag CLI](bag-cli.md)。

## 真实验收清单

最终验收必须绑定相同源码、平台制品、安装文件和适配器哈希，使用真实进程、真实 Core 事件和正式安装入口；测试手写的 metadata、fake publisher 和文件存在都不能替代它。

1. provider 和 consumer 显式启用 capture。用受控业务请求覆盖真实 Service 成功/失败、Action 接受/拒绝、结果和取消；以原 Context/sequence、transport request UUID、Goal ID 核对实际 Topic 与 MCAP 中的记录，不从业务日志补造记录。先确认 observer/recorder 已订阅，保存覆盖窗口和所有层的 drop counters。
2. 在另一台机器生产并录制数据。producer 离线后，用同一个已安装 observer 解析隔离 replay Topic，确认 MCAP 的完整 Schema 足以解释记录。该步骤证明离线可解释和回放，不单独证明零请求。
3. 另保持真实业务 provider 或 spy 在线。回放前后执行合法正例，证明观测仍工作；回放窗口中以可核验 Core/adapter 观察确认业务 Service、send_goal、get_result、cancel 请求计数增量分别为 0，并要求 capture/export 无丢样、无 worker/格式/发布错误，源 ring 与待发布队列已排空。Graph/NodeCard 发现和描述查询单列。provider 离线且数据库没变化，不能排除曾错误发出但无人接收的请求。
4. 对照真实业务 ledger、SQLite 内容/commit_seq、导出文件清单没有因回放改变。制造诊断发布失败或资源溢出，核实业务继续、loss 分层记录、关闭预算及未完成清理状态。重启后 Context/stream ID 改变，旧记录不冒充新业务。

离线规则验证：`python3.11 python/tests/test_request_observer_offline.py -v`。归档 fixture：`python3.11 scripts/test_package_mvp.py -v`。这两组命令不执行 native、连接 Router 或完成上述真实验收。
