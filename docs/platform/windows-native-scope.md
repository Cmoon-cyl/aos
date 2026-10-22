# Windows 平台支持

0.2.0 发行分支已加入 Windows 本机进程管理、脚本 hook、CLI bag 和 Memory 示例。
本页说明实现与检查契约；各平台实际通过的源码、测试和制品以[发行说明](../releases/v0.2.0.md)为准。

| 能力 | Windows 实现与检查范围 |
| --- | --- |
| SDK | Context、Graph、Topic、Service、Action、参数、lifecycle、Executor、时间与资源管理；安装后执行适用测试 |
| run / launch | 每代进程使用独立 Job Object；在创建进程时加入 Job；检查依赖就绪、有限重启、停止与后代清理 |
| 进程身份 | NodeCard 的 launch/process/generation 必须匹配；实际 PID 必须仍属于该代 Job，兼容 Python venv launcher 与 worker |
| 退出 | 控制台 Ctrl-C/Ctrl-Break 请求停止；向自有子进程组发送 Ctrl-Break，宽限结束终止 Job；不扫描 PID 杀进程 |
| 脚本 hook / doctor | 自有 Job 和非阻塞 named pipe；限制输入、输出、超时、取消及后代保留管道 |
| CLI bag | record/play、离线 info/recover、控制输入及显式停止；观察消息回放不重放原业务调用 |
| Memory 示例 | 显式绝对 Python 入口、Windows 文件锁、SQLite 事务、Put/Get/List、Export 与生命周期 |
| 安全 Router | 原生 mTLS、ACL 和进程退出；用匹配的 Windows Router 配置与完整上游依赖 |

## 平台差异

Windows 不提供 POSIX 的 SIGHUP/SIGQUIT、Unix 权限位和共享文件描述符标志。
依赖这些机制的检查逐项登记 N/A；通用能力需要 Windows 等价检查，不能仅删除平台过滤就算通过。
明确清单位于 [`WINDOWS_EXPECTED_SKIPS`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/scripts/native_acceptance.py)。
缺失测试、额外 skip、原因漂移、失败或错误均阻断原生验收；N/A 不计入通过数量。

安装入口使用 `executable` 指向包内 `.py`，`interpreter` 指向安装时的绝对 Python，
`interpreter_args` 为 `-I -u`。不依赖 shell、文件关联、当前目录或 PATH 猜测解释器。
复制安装前缀前必须重新安装并检查解释器路径。

无显式解释器的入口必须是可执行映像；`.bat/.cmd/.py` 不能依赖文件关联启动。
脚本 hook 的 `executable` 与 doctor 的 `python` 在 Windows 上使用绝对可执行路径。

托管 Python 节点在主线程创建有效 bootstrap Context 时，若 SIGBREAK 仍为默认处理，
SDK 将其转换为正常的 KeyboardInterrupt，使 executor 和 `finally` 得以清理；
应用自定义处理器、非托管进程和工作线程不被覆盖。
业务代码中的 `Event.wait()`、`join()` 等等待也应设置有限超时并检查停止条件，
否则只能在宽限期结束后终止 Job。交互式 Ctrl-Break 可能同时送达同一控制台的子进程，
不能把它当作严格逆依赖逐个关闭的证明。

Job Object 负责本次启动的进程树；readiness 的 Job 成员检查不是远端身份认证。
控制台事件也不是 POSIX 信号的完全等价物。Windows 服务注册、开机自启、
任意 OS 版本及无开发工具环境的运行库兼容性，均不由本次原生功能测试推导。

文件路径检查保留绝对路径、规范化和目录包含关系；Windows junction、大小写和
verbatim 路径按本机规则处理。Bag 文件替换检测的具体边界见[运行契约](bag-runtime.md)。
