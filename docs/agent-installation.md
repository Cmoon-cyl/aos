# 一键安装与维护

一键安装支持 macOS arm64、GNU/Linux x86_64 和 Windows x64。运行时版本为
AOS 0.2.0，安装管理器和插件版本为 0.1.0，两者分别报告版本。
网络下载使用公开 GitHub 发行包；不要求 GitHub 登录。

## 安装

macOS / Linux：

```sh
curl -fsSL https://raw.githubusercontent.com/Cmoon-cyl/aos/main/install.sh | sh -s -- --non-interactive --json
```

Windows PowerShell：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -Command "& ([scriptblock]::Create((irm 'https://raw.githubusercontent.com/Cmoon-cyl/aos/main/install.ps1'))) --non-interactive --json"
```

安装器下载并校验固定版本 uv，使用它准备独立的 CPython 3.11.15，
再安装已验证的平台包；不编译原生代码，不替换系统 Python。
macOS / Linux 需要系统提供 curl、tar 和 SHA256 工具。
Windows 使用系统 PowerShell；缺少原生 DLL 时按错误安装 Microsoft Visual C++
v14 x64 Runtime 后重试。不会自动申请管理员权限或更改系统级执行策略。

参数可以追加到安装命令末尾：

| 参数 | 用途 |
| --- | --- |
| `--prefix PATH` | 指定独立安装目录，支持含空格的路径 |
| `--agent codex` | 同时安装 Skill 到用户的 `~/.agents/skills/aos` |
| `--agent claude` | 同时安装 Skill 到用户的 `~/.claude/skills/aos` |
| `--agent trae` | 同时安装 Skill 到用户的 `~/.trae/skills/aos` |
| `--non-interactive --json` | 无交互执行；安装器始终返回结构化 JSON |

已有同名且不由 AOS 管理的 Skill 会被保留，并返回冲突错误。
插件及 Skill 的发现方式见[插件说明](../plugins/aos/README.md)。

默认目录优先取 `AOS_HOME`；否则 macOS / Linux 使用
`$XDG_DATA_HOME/aos` 或 `~/.local/share/aos`，Windows 使用
`%LOCALAPPDATA%\AOS`。安装输出的 `data.cli` 和 `data.python` 可直接执行，
无需激活虚拟环境或重新打开终端。安装器不会修改 PATH。

成功需同时满足 `ok: true`、`data.verification.status: "passed"`，
以及 `data.verification.service_response.sum: 42`。验证会临时启动独立的
loopback Router 与 provider，最后停止它们。安装成功不代表后台服务已常驻。

## 本机环境

下文的 `aos` 指安装结果中的绝对 CLI 路径。

```sh
aos --version
aos env status
aos env ensure
aos env test
aos env stop
```

`env ensure` 启动或复用本安装拥有的 Router，默认只监听
`127.0.0.1:7447`，关闭 multicast 和 gossip。端口被其他进程使用时会失败，
不会停止占用者。`env stop` 通过带随机令牌的本机控制连接停止自己的 Router，
不按保存的 PID 杀进程。`env test` 在临时端口验证真实 Service 调用。

配置位于安装目录的 `config/router.json5` 和 `config/session.json5`。
已有配置不会被安装或更新覆盖。修改 Router 配置后，需要显式执行
`env stop` 再 `env ensure`；这会中断依赖该 Router 的应用。

连接已有部署时，显式设置 `AOS_ZENOH_CONFIG` 和 `AOS_DOMAIN_ID`。
跨机 Router 连接与安全配置见[部署指南](deployment.md)。

## 检查更新、升级和回滚

```sh
aos update check
aos schema update apply
aos update apply
aos update rollback
```

`update check` 只读取公开的稳定发行目录，不修改安装。返回已安装管理器版本、
可用版本、是否有更新，以及当前和目标原生运行时版本。
稳定目录位于仓库 `distributions/stable.json`，指向
`aos-agent-vX.Y.Z` 标签及其带 SHA256 的发行附件，不需要 GitHub API 登录。

`update apply` 下载新版管理器，由新版安装引导程序准备其固定的 Python/uv
和原生运行时。候选版本安装到独立目录，通过文件校验和真实 Service 测试后，
才原子替换 `current.json`。失败的候选版本不替换当前版本；安装日志保留在本机。
重复安装会重新检查已有文件和服务调用；同一目录的安装由锁串行执行。

指定一个已发行管理器版本时，将下列 JSON 保存为文件，再传入
`aos update apply --request 文件路径`：

```json
{"version":"0.1.0"}
```

相同版本无需更新；降级使用 `update rollback`，切回上一个通过重新验证的安装。
首次安装没有可回滚版本。

更新和回滚不会重启已有应用或 Router。`env status` 分别显示当前安装和
正在运行的 Router；需要切换运行中的程序时，显式停止后重新启动。
插件目录中的技能说明由各客户端的插件更新机制维护。

## 手动安装与诊断

需要自己管理 CPython 或离线转移平台包时，使用[手动安装指南](install.md)。
插件 setup 支持 `--archive 本地平台包路径`，仍严格校验固定 SHA256；
首次准备 uv 和 Python 仍需要网络，不能把该参数当作完全离线安装。

安装目录中的 `releases/` 保存候选和已用版本，`current.json` 选择当前版本，
`previous.json` 保存回滚目标，`config/` 保存配置，
`logs/` 和候选版本内的 `verification/` 保存本机诊断。
不要在仍有应用使用旧 Python 或 Router 时删除其版本目录。

下载、SHA256 校验、依赖或测试失败时，保留错误码和本机日志定位问题。
未看到 `ok: true` 及真实调用通过，不能视为安装完成。
