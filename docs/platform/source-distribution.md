# 源码包验收

发行候选除原生 SDK/CLI 验收外，还需检查源码包能独立构建。使用 CPython
3.11、仓库固定的 Rust 工具链和 `maturin==1.15.0`，先准备 Cargo 依赖缓存，
再从干净提交运行：

```sh
python -I scripts/check_sdist.py --output /absolute/new/sdist-evidence
```

脚本保存提交 SHA、全部 Git 跟踪文件的摘要、源码包、解包结果、构建日志、
wheel 和隔离安装结果。它核对源码包保留每个跟踪文件的原始字节及可执行标志，
拒绝缺失、额外、重复或链接条目；`PKG-INFO` 是唯一允许的生成文件。
固定版本 maturin 的 Git 打包路径可能丢失可执行标志。脚本先校验归档路径、
文件集合及全部源文件字节，再按干净源码的标志恢复归档权限，保留生成器原包、
修改前后摘要和逐项权限记录；最终包仍须通过严格权限校验才能解包和重建。
解包后执行 `maturin build --release --locked --offline`，再从新的虚拟环境以
`python -I` 导入原生扩展，并核对其字节与 wheel 一致。wheel 中声明的许可文件
也必须与源码相同。

默认构建目录为证据目录下的 `build-target`。可用 `--build-target-dir` 传入
本次任务独占的 Cargo 缓存；不要与另一个正在构建的工作树共用。
离线构建依赖本机已有的 Cargo 缓存，不表示已随源码包附带所有第三方依赖。
Zenoh 原始归档可通过 `AOS_ZENOH_CRATE_ARCHIVE` 指定；脚本仍会校验固定摘要。

退出码 0 只证明该提交的源码打包、离线重建、许可字节和原生导入通过。
功能、多机、安全和长稳结果仍需引用同一候选对应的验收记录。
