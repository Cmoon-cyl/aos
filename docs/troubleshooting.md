# 故障排查

先记录平台包的 `manifest.json`、CLI 实际路径、Python 实际路径和完整错误。
使用同一个发行包的 CLI/wheel；不要仅凭 `0.2.0` 版本字符串判断候选是否相同。

## Python 无法导入

在已安装 wheel 的 venv 中运行：

```sh
python -I -c 'import sys,aos_sdk; import aos_sdk._native; print(sys.executable); print(aos_sdk.__version__); print(aos_sdk._native.__file__)'
```

Windows PowerShell 可把外层单引号改为双引号，并直接使用
`.\.venv\Scripts\python.exe`。确认解释器与 wheel 的 CPU/平台一致、原生扩展来自该 venv，
而不是源码树。安装 wheel 不需要 Rust，但系统仍要满足原生运行库要求。

Linux 的 `GLIBC_* not found` 表示系统基线低于所用原生产物；当前 wheel 没有 manylinux 或 musl 承诺。
Windows 的 DLL 加载错误需要保留具体缺失模块信息；Router 应保持完整 `router/` 目录，
不能只复制 `zenohd.exe`。实际平台基线见[支持范围](support.md)。

## Context 打开了，但看不到 Node

```sh
aos doctor report
aos node list
aos service list
```

默认 peer 可以先于 Router 打开。依次确认 Router 正在运行、应用的实际 endpoint/domain 一致、
provider 已注册所需端点，并给发现留出有界等待。`node list` 是有限窗口快照；
一次空结果不证明网络中永久没有 Node。

Windows 使用 `config/zenoh-router-windows.json5`。`[::]` listener 在已测 Windows 环境中
只接受 IPv6，不能据此推断 IPv4 的 `127.0.0.1` 可连接。
跨机还要核对 Router 间的实际连接，见[部署指南](deployment.md)。

## doctor 退出 0，报告却有错误

退出 0 表示报告已生成。继续检查 `data.status` 与每个子项。
不指定解释器时，CLI 的 `python_native_module` 为 `not_checked`；
用请求显式指定要诊断的 Python：

```json
{"python":"/absolute/venv/bin/python","timeout":5}
```

将 JSON 保存为 `doctor.json` 后运行 `aos doctor report --request doctor.json`。
Windows 路径使用实际绝对路径，可在 JSON 中使用 `/`。
doctor 不安装依赖，也不发业务请求；认证或授权没有独立证据时保持 `unknown`。

## Service 超时或 Action 结果未返回

先查 provider 是否存在、Executor 是否持续 spin、类型 hash 是否一致，以及 lifecycle 是否允许业务。
长同步回调可能占满线程；需要响应控制请求时保留工作线程并检查 CallbackGroup。

超时不能证明 provider 未执行，特别是写入操作。不要立即重发。
Service 使用业务 request ID 查询或按幂等契约确认；Action 使用原 goal ID 读取结果，
并考虑结果保留期和 provider 是否重启。`UNKNOWN` 不代表该任务从未执行。

## 取消已受理，任务还在运行

CancelGoal 回复的 `goals_canceling` 只证明接受取消。
provider 必须在执行回调里检查取消意图，在安全位置结束并调用 `canceled()`。
最终以 GetResult 的 `CANCELED` 为准。取消不会强行打断 Python 回调；
任务先完成时也可能返回 `SUCCEEDED`。

## mTLS 失败或疑似 ACL 拒绝

核对 profile 的绝对路径、CA、证书有效期、服务端 SAN、证书 CN 与策略主体，
以及实际 domain、名称、类型 hash 和 Router relay 转发集合。
安全 profile 被选中后不会通过开发连接补结果。

Zenoh 1.10.1 的 ACL 丢弃可能表现为超时。拒绝结论需要 Router 的拒绝记录、
业务状态未变化和获准正例相互印证；不能只凭 timeout 认定授权机制工作正常。
不要在故障材料中附带私钥。

## launch 看到了 Node，但依赖没有启动

readiness 核对 launch/process/generation/PID、NodeCard、端点和 lifecycle 目标。
同名外部 Node、旧进程的发现残留或包装脚本改变了实际进程身份，都可能不满足条件。
查看 `process_started`、`discovered`、`ready` 和终端 summary，
按[进程配置](platform/process-bootstrap.md)固定入口及部署参数。

## 进程退出慢或输出管道堵塞

持续读取 stdout 和 stderr；将 JSONL 解析与业务处理解耦，避免消费者停读导致反压。
使用终端中断请求有界退出，并检查 summary 的清理结果。
操作系统强制终止只能证明进程被回收，不能当作 SDK 正常关闭或业务回滚。

bag 恢复写入新目录，不修改源目录；先保留原始 bag 和 provenance，再运行
`aos bag recover`。详细边界见[Bag CLI](platform/bag-cli.md)。

## 提供可复现问题

附上版本、平台、制品 SHA256、最小请求与接口、退出码和分离的 stdout/stderr。
同时说明预期业务结果、实际结果、是否重复提交、是否发生 Router/provider 重启。
业务数据可使用最小合成样本，证书只保留非秘密身份与有效期信息。
