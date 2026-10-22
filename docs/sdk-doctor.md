# Python SDK 安装自检

`doctor_report()` 检查当前 Python 环境、原生扩展、所选配置和一次有界的
Context / Graph 操作。适合在 Linux 等目标系统安装 wheel 后运行。
本功能的本机测试结果不能替代 Linux 或 Windows 的原生安装验收。

## 运行

先使用安装 wheel 的同一个解释器。SDK 可以导入时：

```sh
python -I -m aos_sdk.doctor --help
python -I -m aos_sdk.doctor --config /path/to/zenoh-session.json5 --domain-id 0 --timeout 5
```

标准输出只有一个 JSON 对象。退出码 `0` 表示本次执行的检查通过，
`1` 表示检查失败或超时，参数解析错误为 `2`。
`router.status` 和安全字段仍可能为 `unknown`，不能把退出码 `0` 当作业务验收。

也可从 Python 调用：

```python
import json
from aos_sdk import doctor_report

report = doctor_report(config="/path/to/zenoh-session.json5", domain_id=0, timeout=5)
print(json.dumps(report, indent=2))
```

参数均为关键字参数，返回普通字典，可直接 JSON 序列化。
`timeout` 默认为 5 秒，允许有限数值 `0 < timeout <= 86400`；无效值抛出 `ValueError`。
`config` 接受字符串路径或返回字符串的 `os.PathLike`。

配置选择顺序为 `config` 参数 → `AOS_ZENOH_CONFIG` → 原生模块内置配置。
Doctor 会把所选路径作为最终覆盖传给 Context。相对路径按调用时的工作目录解释；
不展开 `~`。空的配置环境变量与未设置相同。
domain 选择顺序为 `domain_id` 参数 → 非空 `AOS_DOMAIN_ID` → 安全 profile 的 domain → `0`；
没有安全 profile 时才使用最后的 `0`。domain 必须在 `0..4294967295` 内。

安全选择先用显式 `security_profile`，否则保留 Python `os.environ` 或原生进程环境中的
`AOS_SECURITY_PROFILE`。两个来源都有值但不一致时拒绝打开 Context；空值也作为已配置的
无效选择处理。Doctor 在独立子进程中核对两份来源，然后移除部署身份，使用选定的显式
profile 做诊断。它按字符串比较环境值，因此应为两个来源使用相同的路径拼写。

配置和 domain 的环境值仍取自父进程的 Python `os.environ` 快照。直接 `Context()` 则读取
原生进程环境里的 `AOS_ZENOH_CONFIG` 和 `AOS_DOMAIN_ID`。如果通过 `os.putenv` 或嵌入宿主
只修改原生环境，这两项的诊断结果可能与业务 Context 不同；需要比较时传入明确参数。

## 原生模块无法导入时

`aos_sdk` 包初始化仍要求 `_native` 能导入。因此，缺少原生模块或共享库时，
`python -m aos_sdk.doctor` 和公共 SDK 函数都无法越过包初始化。
使用同一解释器直接运行安装目录内的 `doctor.py`，即可获得结构化诊断：

```sh
python -I /path/to/site-packages/aos_sdk/doctor.py --timeout 5
```

在 Linux 上，安装目录通常位于虚拟环境的
`lib/python3.11/site-packages/aos_sdk/`。以实际安装目录为准。
直接入口使用该文件旁边的 SDK 包，能够报告 `NATIVE_IMPORT_FAILED`，
以及可定位到的扩展文件路径。它不会修复安装，也不会自动下载依赖。

## 读取报告

| 字段 | 能说明什么 |
| --- | --- |
| `schema_version` | 当前报告结构版本为 `1` |
| `status` | `ok`、`error` 或 `timeout`，只概括本次实际执行的检查 |
| `runtime` | Python 版本、解释器路径、操作系统、CPU；Linux 还尝试报告 libc 名称和版本 |
| `sdk` | 安装元数据版本和原生模块版本；不一致时报告 `VERSION_MISMATCH` |
| `native` | 原生扩展是否加载成功及加载路径；缺少模块时路径可能为 `null` |
| `config` | 来源、绝对路径、存在情况、读取字节数和 SHA256；不输出配置内容 |
| `domain` | 所选逻辑域和来源；不是认证身份 |
| `context` | 创建是否成功，以及 `ok()` 返回的本地关闭状态 |
| `router` | 当前为 `unknown`；Python Context 尚未提供连接中的 router ID |
| `graph` | 一次发现快照是否完成、观察到的实体数量和查询等待时间；不输出实体名称或业务元数据 |
| `shutdown` | 本次诊断 Context 是否正常关闭 |
| `security` | 认证、授权、加密均为 `unknown`；本功能未验证这些能力 |
| `probe` / `cleanup` | 最后执行阶段、子进程退出码、是否强制结束及是否已回收 |
| `failures` | 失败阶段、稳定错误码、排查提示和异常类型；不包含原始异常文本 |

内置配置没有外部路径，因此 `exists`、`size_bytes`、`sha256` 为 `null`。
文件摘要只描述检查时读到的字节，不能证明文件在随后 Context 创建时没有变化。
检查期间请保持配置不变。

常见错误码包括：

| 错误码 | 下一步 |
| --- | --- |
| `CONFIG_NOT_FOUND` / `CONFIG_NOT_FILE` / `CONFIG_UNREADABLE` | 核对路径、文件类型和读取权限 |
| `CONFIG_INVALID` | 用同版本 Zenoh 支持的 session 配置修正内容 |
| `INVALID_DOMAIN_ID` | 检查参数或 `AOS_DOMAIN_ID` |
| `NATIVE_IMPORT_FAILED` / `NOT_NATIVE_EXTENSION` / `VERSION_MISMATCH` | 核对 wheel 的 Python ABI、系统、CPU 架构及共享库依赖，并重新安装匹配版本 |
| `COMMUNICATION_FAILED` | 检查所选配置、可达的 peer/router 和网络；单凭该错误无法断言根因是 router |
| `PROBE_TIMEOUT` | 从 `probe.stage` 判断卡在配置读取、模块导入、Context、Graph 还是关闭；检查对应环境后重试 |
| `SHUTDOWN_FAILED` / `CLEANUP_TIMEOUT` | 保留报告，检查原生关闭失败或操作系统是否仍未确认子进程退出 |
| `PROBE_START_FAILED` / `PROBE_EXITED` / `PROBE_IO_FAILED` | 检查解释器执行权限、临时文件访问，或原生模块异常退出 |

配置解析和网络异常可能带出密码、证书内容或连接凭据，因此报告只保留异常类型，
不会转发原始异常、子进程标准错误或原生模块的额外标准输出。
路径、版本、系统信息和文件摘要会出现在报告中，分享前按实际接收方检查这些信息。

## 时限和探测范围

Doctor 启动独立 Python 子进程，使用当前安装的 SDK 创建一个临时 Context，
调用 `ok()`、执行 `graph(timeout=...)`，最后调用 `shutdown()`。
Graph 等待最多 0.2 秒，并受剩余总预算限制。
Doctor 不创建 Node，不发布 Topic，不调用 Service 或 Action，也不查询 NodeCard。
建立通信会话仍会按所选配置打开监听端点并连接配置中的 peer/router。

`timeout` 覆盖子进程启动、配置读取和摘要、模块导入、Context 创建、
Graph、关闭和回收。总预算的最后 20%（最多 0.2 秒）留给强制结束和回收；
超时后不会额外无限等待。实现不以后台线程包裹原生调用，
也不使用会遗留管道读取线程的通信方式。
操作系统调度或不可中断的系统调用不能由 Python 提供实时保证；
若截止时操作系统尚未确认退出，报告会明确给出 `cleanup.reaped=false`
和 `CLEANUP_TIMEOUT`，不会宣称清理成功。

Context 打开成功并不证明连接了 router，`ok()` 只表示本地 Context 尚未关闭。
Graph 为空也不能证明没有其他节点；发现可能尚未传播，或逻辑域不同。
验证业务可用性仍需另外运行实际的 Topic、Service 或 Action 场景。
