# 安装后通信与重启探针

`scripts/platform_probe.py` 使用已安装的 Python SDK，在独立进程间验证 Topic、Service、Action 与 provider 重启。脚本只依赖 Python 标准库和 `aos_sdk`；Interface 定义已内嵌，可以单独复制到另一台机器，从任意目录启动。

它不构建或安装制品，也不启动 router。原生构建与 wheel 安装仍由 `scripts/native_acceptance.py` 或平台构建流程负责。单机探针通过，只证明这台机器的进程间通信；跨机结果须分别运行 provider/client，并保留两端证据。

## 前提与身份

两端使用 CPython 3.11 或更高版本，并在各自独立环境中安装对应平台、同一源码版本的 wheel。使用该环境的 Python，加 `-I` 运行；探针会核对 SDK 与 native extension 来自该解释器的 `site-packages`。

启动可达的 Zenoh 1.10.1 router，并提供明确的 session 配置路径。下例是可同时供两台机器使用的 JSON 配置；把 `<router-ip>` 换成可达地址，保存为绝对路径：

```json
{
  "mode": "client",
  "connect": {
    "endpoints": ["tcp/<router-ip>:7447"],
    "timeout_ms": 3000,
    "exit_on_failure": true
  },
  "listen": {"endpoints": []},
  "scouting": {
    "multicast": {"enabled": false},
    "gossip": {"enabled": false, "autoconnect": []}
  },
  "timestamping": {"enabled": true}
}
```

使用安全部署时，将下列命令的 `--config /absolute/session.json` 替换为 `--security-profile /absolute/peer.json`；两者互斥。省略 `--domain-id` 时沿用安全 profile 的 domain，使用 `--config` 时则默认 72。每台机器使用自己的安全 profile 和凭据，参见[每机 Router 与 TLS peer 配置](platform/topology.md)。探针会把同一 profile 路径传给自己的子进程，Context 仍通过 SDK 的安全配置校验。Windows 使用 `venv/Scripts/python.exe`，其他参数相同。

同一轮两端使用相同的 `--run-id`、生效 domain 和 `--source-sha`。domain 可由安全 profile 继承，也可通过 `--domain-id` 显式给出。继承时父进程尚未打开 Context，早期证据的 domain 为 null；以 SDK worker 的 `sdk_ready` 及之后记录为准。`run-id` 每次换新，首字符为字母，之后只用字母、数字或下划线，最多 64 个字符；它同时隔离端点命名空间和业务数据。探针的控制 Service 只负责当前测试 provider 的退出，请在为测试选定的网络/domain 中运行。

`--source-sha` 是构建记录中声明的完整 Git SHA，不能通过它反推 wheel 内容。实际加载的 native extension 路径与 SHA256、SDK 版本、脚本 SHA256 会单独记录；`--artifact-sha256` 可补上构建清单中的 wheel 摘要。旧制品初验应加 `--build-label baseline`；默认标签为 `candidate`。

## 单机运行

把下面的占位路径与 SHA 换成实际值，使用一个不存在或为空的证据目录：

```sh
/absolute/installed-env/bin/python -I /absolute/platform_probe.py local \
  --config /absolute/session.json \
  --run-id c02_local_001 \
  --source-sha <40-character-source-sha> \
  --artifact-sha256 <64-character-wheel-sha256> \
  --output /absolute/evidence/local_001 \
  --network-label 'local client sessions through tcp/127.0.0.1:7447'
```

`local` 启动 provider 监督进程与 client 监督进程；二者各自启动实际 SDK worker。provider 第一轮 worker 正常退出后，监督进程启动第二个 worker。client 的 SDK worker 与 Context 从第一轮保持到第二轮，核对旧 Node ID 消失、新 PID/instance ID/Node ID 出现，并再次调用全部能力。

## 跨机运行

先在 Linux 启动 provider：

```sh
/absolute/installed-env/bin/python -I /absolute/platform_probe.py provider \
  --config /absolute/session.json --run-id c02_linux_to_mac_001 \
  --source-sha <40-character-source-sha> \
  --output /absolute/evidence/provider_001 \
  --overall-timeout 180 --network-label 'Mac and Linux through the shared router'
```

再在 Mac 启动 client，使用相同的 `run-id` 与源码 SHA：

```sh
/absolute/installed-env/bin/python -I /absolute/platform_probe.py client \
  --config /absolute/session.json --run-id c02_linux_to_mac_001 \
  --source-sha <40-character-source-sha> \
  --output /absolute/evidence/client_001 \
  --overall-timeout 180 --network-label 'Mac and Linux through the shared router'
```

client 完成第一轮后，通过测试专用 `finish_round` Service 请求 provider 正常退出。provider 监督进程等待 `--restart-delay`（默认 1 秒），再启动第二轮。client 完成第二轮并观察 provider 消失后退出。两端命令都必须返回 0，并核对两端 Action 的 goal ID、结果与终态。交换两台机器的 provider/client 角色，换一个 `run-id` 再运行，才能记录双向验收。

每个业务等待默认最多 10 秒，使用 `--stage-timeout` 调整。整体工作期限默认 120 秒，使用 `--overall-timeout` 调整。期限届满或中断后另有清理时间：监督进程终止自己的 worker，3 秒内未退出则强制终止，再最多等待 3 秒。`local` 对自己启动的两个进程组逐一清理。它不会停止外部 router。

## 通过判据与证据

每轮都必须同时满足以下条件：

| 检查 | 通过判据 |
|---|---|
| Topic | client 真实收到本轮 provider 的 tick，核对 run ID、instance ID 与确定性 payload |
| Service | 加法请求只发送一次，返回同一 request ID、provider 身份和正确的计算结果 |
| Action 成功 | 收到匹配 goal 的 feedback；最终状态为 `SUCCEEDED`（4），完成步数和总和正确 |
| Action 取消 | `goals_canceling` 包含目标 goal ID；随后结果为 `CANCELED`（5），部分计算结果正确 |
| 重启 | 同一 client 观察旧 Node ID 消失，发现新 PID/instance ID/Node ID，并完成第二轮全部调用 |
| 清理 | provider 第二轮退出，client 观察节点消失，SDK/Executor 正常关闭，所有自有子进程已回收 |

示例数据只做加法和有界逐步累加，Action 的状态转换、feedback、cancel 与 result 都经过真实 SDK、Executor 和 Rust ActionServer。探针没有另建 goal 状态机，不用新 provider 承接旧 goal，也不自动重试业务请求。Graph 与 readiness 只用于等待能力可见，业务结果另行校验。

成功短任务先收到并核对 feedback，再请求 result。AOS 与固定版本 rclpy 都会在成功取得 result 后释放该 goal 的反馈回调，独立通道上的迟到反馈不保证继续交付；先请求 result 再要求至少一条 feedback 不是稳定的验收顺序。取消任务仍在等待 feedback 前挂起 GetResult，随后发取消并验证 CANCELED，从而覆盖挂起结果查询被取消终态唤醒。成功路径可能取到已缓存的终态，不单独证明 GetResult 在 SUCCEEDED 前已挂起。

Action 证据记录 client 本地 monotonic 的提交、接受、首条 feedback 和结果观察时刻，以及反馈计数、原生 status 缓存和 result future 状态。首条 feedback 可能早于 goal 接受响应，时间差允许为负。反馈超时时先写 `action_feedback_timeout` 再报错；已有 future 若完成，只读取其本地结果，不增加网络请求。这些字段用于区分反馈时序、状态到达和结果完成，不能用多机绝对时钟差推断单向网络延迟。探针脚本独立于已安装制品变化时，另行记录 probe commit 和脚本摘要，不能把 wheel 的源码 SHA 当成脚本来源。

证据文件使用 JSON/JSONL。每条事件含协议版本、run ID、PID/PPID、hostname、OS/架构、Python、源码声明、配置摘要和脚本摘要；SDK worker 的业务阶段还含实际 native extension 摘要。配置完整文本不会复制进证据；JSON 配置会提取 mode/connect/listen，通用 JSON5 仅记录文件摘要，可用 `--network-label` 补充人工声明的网络路径。

| 文件 | 内容 |
|---|---|
| `local-0.json` | 单机父进程结果 |
| `provider-0.json` / `client-0.json` | 各端监督进程结果 |
| `_provider-1.jsonl` / `_provider-2.jsonl` | 两个真实 provider worker 的请求、结果、终态与清理 |
| `_client-0.jsonl` | 同一个 client worker 的两轮验收及发现变化 |
| 对应 `.stdout` | worker 的标准输出与错误，失败时可定位异常 |

在 `local` 模式下，两端证据分别放在 `provider/`、`client/` 子目录。每个进程的 `.json` 是其最终结果，`.jsonl` 是阶段记录。只看到 `provider_ready`、`cancel_accepted` 或其中一端通过，都不能计为完整跨机通过。

这份探针覆盖有序 provider 退出和重新启动。router 重启、网络中断、进程崩溃后的任务恢复、长稳负载与认证/ACL 需要对应的后续验收，不能从这次结果推断。
