# 在 Linux 上把 AOS launch 安装为用户服务

`scripts/system_service.py` 把一个已经准备好的 `aos launch start` 请求安装为
systemd 用户服务，提供安装、启动、状态、日志、停止和卸载入口。它不需要 sudo，
不修改 linger，不把应用配置或环境变量复制进 unit。

## 三层职责

| 层 | 负责 | 失败后的行为 |
|---|---|---|
| systemd 用户管理器 | 启动和保活 `aos` supervisor，拥有它的整个 cgroup | supervisor 异常崩溃后先清理 cgroup，再按窗口限流重启 |
| AOS launch supervisor | 本次计划内的 child、依赖、就绪、有限重启和关闭顺序 | 按每个 process 的 `restart` 策略重启；额度耗尽后关闭整组 |
| Node lifecycle | Node 的配置、激活、业务状态与转换 | 使用标准生命周期服务；不承担 OS 进程重启 |

同一 child 只有 supervisor 一个重启责任方。unit 的
`RestartPreventExitStatus=1 2 3 4 129 130 131 143` 阻止 systemd 因 CLI 已报告的失败重新启动
整份计划。这样，child 耗尽重启额度后不会被 OS 自动重置额度。
`129/130/131/143` 对应 CLI 处理 SIGHUP/SIGINT/SIGQUIT/SIGTERM 后的退出，
也列为正常停止状态。用户显式再次启动是一次新部署尝试。

## 前提与登录会话

需要 Linux、Python 3.11+、systemd 用户管理器、`systemctl`、`systemd-analyze`、
`loginctl` 和 `journalctl`。当前实际验证环境为 systemd 252、cgroup v1。
脚本也识别 cgroup v2 的 `cgroup.procs`，但不能据此宣称已经在 v2 主机验收。

先用脚本审计当前用户的条件：

```bash
SERVICE_TOOL=/absolute/path/to/aos/scripts/system_service.py
python3 "$SERVICE_TOOL" audit
```

输出包括用户管理器状态、cgroup 版本、`Linger`、`Sessions` 和 `RuntimePath`。
`Linger=yes` 表示用户管理器可以在退出登录后继续运行，并在开机时启动；
unit 还需 `--enable` 才会加入该管理器的 `default.target`。
未开启 linger 时，不承诺最后一个登录会话退出后服务仍然存活。
是否允许修改 linger 由主机管理员决定；本脚本不会执行该修改。

关闭一次 SSH 连接与退出用户全部会话是两种不同验证。如果主机上仍有其他用户会话，
关闭一个 SSH 连接不能证明最后一个会话退出后服务仍会保活；应同时检查 linger 设置。

## 准备固定的启动输入

安装前，先通过已安装 CLI 验证包、入口和 launch 计划。服务请求必须包含非空的显式
`prefixes`；每个前缀必须是现存绝对目录。脚本不会依赖交互式 shell 的
`AOS_PREFIX_PATH`、cwd 或源码目录来发现包。

请求支持 CLI 的三种 launch 来源，只能选择一种：

| 来源 | 服务请求内容 |
|---|---|
| 内嵌计划 | `{"prefixes":[...],"plan":{...}}` |
| 已保存计划 | `{"prefixes":[...],"path":"/absolute/path/launch.json"}` |
| 已安装资源 | `{"prefixes":[...],"package":"...","resource":"...", "package_version":"..."}` |

`package_version` 可省略。资源来源通过只读 `aos package resource` 解析，必须是
`kind=launch`。`overrides` 沿用 CLI 格式。业务计划、接口和 bootstrap 仍由已有 CLI、
runtime、core 校验；本脚本只校验服务路径、输入格式和关闭预算，不另实现业务状态机。

例如，先保存一个只引用现有文件的请求；路径需换成实际安装位置：

```json
{
  "prefixes": ["/opt/aos/releases/example"],
  "path": "/opt/aos/config/managed-pair.json"
}
```

请求与计划使用严格 UTF-8 JSON，最大各 1 MiB，拒绝重复键和非有限数字。
请求 `path` 必须是绝对路径。默认 `WorkingDirectory` 是请求文件所在目录；
内嵌计划中的相对配置路径以此为基准。文件/资源计划内部的相对路径仍按 CLI 的
“计划所在目录”规则解析。也可以用 `--working-directory` 指定现存绝对目录。

systemd 参数不会经过 shell；`$` 不会展开，`%` 会转义为字面量。请求路径支持空格和引号。
systemd 252 拒绝可执行路径中的引号和反斜杠，因此 `--aos` 路径不能含这些字符；
工作目录也不能以空格或反斜杠结尾。

## 安装、查看与停止

先生成可审阅的 unit。`render` 的标准输出是 JSON；`--output` 新建文本 unit 文件，
目标已存在时拒绝覆盖。

```bash
python3 "$SERVICE_TOOL" render \
  --name example \
  --aos /opt/aos/releases/example/bin/aos \
  --request /opt/aos/config/service-request.json \
  --output /tmp/aos-example.service

systemd-analyze --user verify /tmp/aos-example.service

python3 "$SERVICE_TOOL" install \
  --name example \
  --aos /opt/aos/releases/example/bin/aos \
  --request /opt/aos/config/service-request.json \
  --enable --start
```

安装器再次执行 `systemd-analyze --user verify`，通过后才创建：

- `${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/aos-example.service`
- `${XDG_STATE_HOME:-$HOME/.local/state}/aos/system-services/aos-example.service/owner.json`

新文件权限为 `0600`，每个服务的 ownership 目录为 `0700`。owner 只保存路径、摘要、
UID 和服务选项，不保存请求正文、应用环境或认证信息。unit 的 stdout/stderr 直接进入
journal；应用本身不要输出秘密。journal 的保留周期和访问规则由主机管理。

`--enable` 与 `--start` 都是显式选项，省略时只安装。
`Type=exec` 或 `ActiveState=active` 只说明 supervisor 已执行，不能证明 Node 就绪。
业务就绪需核对 CLI 的真实 `ready` 事件，以及对应 launch/process/generation/PID 身份。

```bash
python3 "$SERVICE_TOOL" status --name example
python3 "$SERVICE_TOOL" logs --name example --lines 100
python3 "$SERVICE_TOOL" stop --name example
python3 "$SERVICE_TOOL" start --name example
python3 "$SERVICE_TOOL" uninstall --name example
```

`status` 返回 systemd 原始关键属性、当前 cgroup PID、输入是否变化和关闭预算。
`stop` 等待 systemd 完成，并核对此前 cgroup 已无进程；不能检查 cgroup 时返回
`CLEANUP_UNVERIFIED`，不会把“stop 命令返回”当成完整清理证明。
`uninstall` 先停止和核对，再禁用并删除本脚本拥有的文件，最后 reload。
停止或卸载均不删除应用文件、请求或 journal。

## 关闭预算与 supervisor 重启限制

unit 使用 `KillMode=mixed`：先只向 supervisor 发送 SIGTERM，让它按依赖反序组织
child 的 SIGINT → SIGTERM → SIGKILL。main 退出或 OS 停止期限到达后，systemd 对整个
cgroup 发送最终 SIGKILL，覆盖另建 session/process group 的后代。
这修正了早期设计稿中 `control-group` 同时向所有 child 发初始信号的建议。

默认 `TimeoutStopSec` 根据实际计划计算：

```text
shutdown.sigint_timeout（默认 5 秒）
+ shutdown.sigterm_timeout（默认 5 秒）
+ runtime 统一回收宽限 2 秒
+ 停止后的绝对输出排空预算 30 秒
+ 探针关闭主线程上限 1 秒
+ 终态 summary/error 写出上限 2 秒
+ OS 管理余量 3 秒
```

runtime 的 INT/TERM 时间是整组共享预算，因此这里不按进程数相乘。默认计划得到
48 秒；不是对任意计划固定 48 秒。输出排空与进程组清理可能重叠，脚本仍保守地
加总各自上限，为 CLI 自身的关闭期限保留余量。停止后的 30 秒输出预算是绝对期限，
不因 journal 缓慢但持续接收而延长。`--stop-timeout` 可以增加 OS 预算，但不会增加 CLI
内部的输出期限。
选择更短值必须同时指定 `--allow-forced-stop`，状态会标记 `forced_stop=true`，
其含义是允许 OS 提前强制终止，不再保证应用完成全部优雅关闭步骤。

supervisor 的默认重启策略为 `on-failure`，延迟 10 秒，300 秒内最多启动 3 次。
可调整 `--restart-delay`、`--start-limit-interval`、`--start-limit-burst`；脚本要求
延迟至少 5 秒、burst 为 1–5，且窗口大于 `delay × burst`，不能禁用限流。
这是窗口内启动次数限制，包含首次启动和手动启动，不是整个服务生命周期的总重启次数。
达到限流后不会自行定时恢复；排查后可以明确执行：

```bash
python3 "$SERVICE_TOOL" reset-failed --name example
python3 "$SERVICE_TOOL" start --name example
```

`reset-failed` 会清空 systemd 的启动限流计数，不能把它放入自动重试循环。
systemd 252 可能在启动限流后仍保留 `Result=signal`；结合 `ActiveState`、
`NRestarts`、实际启动次数及 journal 的限流记录判断，不能只匹配 `Result=start-limit-hit`。

stdout 使用 4 MiB 有限队列。下一帧因队列已满无法入队后，连续 30 秒没有任何
写入进展才触发 `OUTPUT_ERROR`；每次写入都会重置期限。journal 的短暂停顿不会
因此停止整组，拥塞期间停止信号和进程回收仍继续。若持续堵塞超过上限，CLI 会
清理 child 后以 1 退出；这个已报告的失败被 `RestartPreventExitStatus` 拦截，
需要排查 journal/磁盘/消费者后再明确启动，OS 不自动重置这次运行。

运行中的 30 秒只覆盖满队列且无进展的停滞，持续慢速输出可以继续运行。收到停止请求后，
runtime 另用上面的绝对输出预算；到期仍有待交付输出时返回 `OUTPUT_ERROR`，可能没有
完整终态摘要。leader 回收时，runtime 用每个流的 `FIONREAD` 快照记录内核及本地残留；
这份有限残留可按配额逐步排空，后来写入的数据不能补充它。每个流各有原始字节额度，
每个进程在残留排完并检查尾部后有一个 2 秒管道关闭窗口。超额或超时报 `CLEANUP_FAILED`。
新字节不会延长关闭窗口；sink 背压或待交付事件可以延后检查，但均不能延长停止后的绝对期限。
这些错误不会授权 supervisor 向组外进程发信号，最终清理由
systemd cgroup 覆盖。

supervisor 崩溃后由 systemd 发起的重启会重新执行整份计划。这是新的部署运行：
一次性消费者也会再次提交其批次。同一 supervisor 内的 provider 重启不会隐式重放
消费者的业务请求，但这个保证不涵盖整份计划重新运行。涉及外部副作用的消费者
应使用业务幂等键或持久完成记录。

## 修改配置和所有权边界

部署输入应使用固定版本的文件。安装器保存请求和解析后 launch 文件的摘要；
手动 `start` 会在输入变化时返回 `INPUT_CHANGED`，要求重新安装以重算关闭预算。
systemd 自行重启直接执行 native CLI，不经过 Python 脚本；因此运行中不能原地修改这些
文件。需要更新时先停止、卸载，再使用新版本输入安装。其他被引用的资源与二进制也应按
发行版本管理，本脚本不锁定或复制它们。

已有同名 unit、symlink 或 owner 记录一律拒绝覆盖。启动、停止、日志与卸载前都会核对
unit 摘要、文件归属、systemd 的 `FragmentPath` 和 drop-in；用户修改过的 unit 会报
`UNIT_MODIFIED` 并保留。此时应先审阅差异，明确接管，不能删除用户改动后强行重装。

安装后若用户管理器操作失败，会保留已创建的专属文件供检查；不会自动删除其他服务。
脚本只针对自己命名的 unit 调用 systemctl，不修改 user manager 全局环境、
用户会话、linger、系统服务或其他仓库。

## 验证部署

本地脚本测试：

```sh
python3.11 -m unittest discover -s deployment/systemd -p test_system_service.py -v
```

在 Linux 上验证服务管理行为时，使用全新的目录与服务名：

```sh
python3.11 deployment/systemd/verify_systemd.py fixtures \
  --root /absolute/scratch/systemd-fixtures \
  --name aos-systemd-check
```

这个入口使用测试程序检查参数引用、同名保护、进程组清理、重启限流、停止和卸载。
实际应用还需要检查依赖就绪、child 重启额度、业务数据保留和日志背压。

会话保活可通过两次独立 SSH 调用执行 `persistence-start`、`persistence-check`，
使用同一个新 `--root` 和 `--name`；第二次核对同一 MainPID 并清理。
最后一个用户会话退出和主机重启需要分别验证，不能由单个 SSH 连接结束推导。

系统管理依据固定在 systemd v252：
[kill](https://github.com/systemd/systemd/blob/v252/man/systemd.kill.xml)、
[service](https://github.com/systemd/systemd/blob/v252/man/systemd.service.xml)、
[unit 限流](https://github.com/systemd/systemd/blob/v252/man/systemd.unit.xml)、
[linger](https://github.com/systemd/systemd/blob/v252/man/loginctl.xml)。
