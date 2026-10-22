# 快速开始：从加法服务到可取消任务

目标是在本机完成一次真实 Service 调用，再运行一个带反馈的 Action。
先完成[安装](install.md)，在解压后的平台包根目录执行。需要三个终端；
Router 和 provider 运行期间，终端保持打开。

## macOS / Linux

终端 1 启动包内 Router：

```sh
./router/zenohd --config config/zenoh-router.json5
```

终端 2 进入同一个包根目录，启动 provider：

```sh
.venv/bin/python -m examples.add_two_ints_server --config config/zenoh-session.json5
```

等待 provider 输出 `"ready": true`。终端 3 进入包根目录，发现并调用：

```sh
export AOS_ZENOH_CONFIG="$PWD/config/zenoh-session.json5"
export AOS_DOMAIN_ID=0
./bin/aos node list
./bin/aos service list
./bin/aos schema service call
./bin/aos service call --request - <<'JSON'
{"service_name":"/add_two_ints","service_type":"aos_examples/srv/AddTwoInts","values":{"a":17,"b":25},"timeout":5}
JSON
```

响应中的 `sum` 应为 `42`。这一步同时验证发现、接口描述、请求与 provider 的实际计算。

## Windows PowerShell

终端 1 启动 Windows IPv4 Router 配置：

```powershell
.\router\zenohd.exe --config config\zenoh-router-windows.json5
```

终端 2 进入同一个包根目录：

```powershell
.\.venv\Scripts\python.exe -m examples.add_two_ints_server --config config\zenoh-session.json5
```

等待 `"ready": true`。终端 3 进入包根目录：

```powershell
$env:AOS_ZENOH_CONFIG = (Resolve-Path '.\config\zenoh-session.json5').Path
$env:AOS_DOMAIN_ID = '0'
.\bin\aos.exe node list
.\bin\aos.exe service list
.\bin\aos.exe schema service call
'{"service_name":"/add_two_ints","service_type":"aos_examples/srv/AddTwoInts","values":{"a":17,"b":25},"timeout":5}' |
    .\bin\aos.exe service call --request -
```

响应中的 `sum` 同样应为 `42`。PowerShell 中使用绝对 venv 解释器即可，无需修改脚本执行策略。

## 运行 Action

保持 Router 运行，在终端 2 按 Ctrl-C 关闭加法 provider。
用当前平台的 venv Python 启动 Fibonacci provider：

```sh
# macOS / Linux
.venv/bin/python -m examples.fibonacci_server --config config/zenoh-session.json5
```

```powershell
# Windows
.\.venv\Scripts\python.exe -m examples.fibonacci_server --config config\zenoh-session.json5
```

在终端 3 使用同一平台的 venv 解释器运行客户端：

```sh
# macOS / Linux
.venv/bin/python -m examples.fibonacci_client 8 --config config/zenoh-session.json5
.venv/bin/python -m examples.fibonacci_client 30 --cancel-after 0.2 --config config/zenoh-session.json5
```

```powershell
# Windows
.\.venv\Scripts\python.exe -m examples.fibonacci_client 8 --config config\zenoh-session.json5
.\.venv\Scripts\python.exe -m examples.fibonacci_client 30 --cancel-after 0.2 --config config\zenoh-session.json5
```

第一条会输出受理、反馈和结果；成功终态 `status: 4` 的序列应为
`[0,1,1,2,3,5,8,13]`。第二条显式请求协作取消，应检查取消回复及最终 `status`。
任务可能先完成，不能仅凭发送取消请求判定已取消。

## 停止与下一步

先在 provider 终端按 Ctrl-C，等待进程结束，再停止 Router。
停止 Router 只改变连接，不会替应用撤销已经执行的写操作。

- 要写自己的程序：读 [Python SDK](python-sdk.md)。
- 要让 Agent 调用：读 [CLI 的发现与 schema 流程](cli.md)。
- 要试完整持久化应用：运行 [MemoryServer](platform/memory-server.md)。
- 要连接另一台机器：按[部署指南](deployment.md)配置每机 Router。
- 没有得到预期结果：从[故障排查](troubleshooting.md)确认实际解释器、连接与接口。
