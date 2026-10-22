# aos-bag

Rust 共用 Bag 运行层：录制经 Interface 校验的原始 Topic 字节，写入 `aos-json-v1` MCAP，离线检查/恢复，并沿现有 core 名称、QoS 和 Clock 规则回放。

公共入口为 `Recorder::start`、`Player::open`、`inspect` 和 `recover`。配置与结果结构见 [`types.rs`](src/types.rs)；完整契约、默认资源边界与验收范围见 [bag-runtime.md](../../docs/platform/bag-runtime.md)。

```rust,no_run
use aos_bag::{PlaybackOptions, Player, RecordOptions, Recorder, RecordStopReason};
use aos_core::Context;
use std::{path::Path, time::Duration};

fn record_and_replay(context: Context, directory: &Path) -> aos_bag::BagResult<()> {
    let recorder = Recorder::start(context.clone(), RecordOptions {
        directory: directory.into(),
        all_topics: true,
        ..Default::default()
    })?;
    // 应用在这里运行；status() 显示发现情况与丢样来源。
    recorder.stop(RecordStopReason::Requested)?;
    let recorded = recorder.wait(Duration::from_secs(5))?;
    assert!(!recorded.manifest.incomplete);
    let player = Player::open(context, directory, PlaybackOptions {
        namespace: Some("/replay".into()),
        ..Default::default()
    })?;
    player.start()?;
    player.wait(Duration::from_secs(5))?;
    Ok(())
}
```

`stop()` 只提交关闭请求，`wait()` 成功才确认线程和资源已关闭。Recorder/Player 不 shutdown 调用方传入的 Context。pause/seek 成功是后续本地 dispatch 的边界；已经交给传输或消费者队列的消息无法撤回。控制超时应读取 status，不能当成已经暂停或跳转。

回放必须显式给独立 namespace，或设置 `original_names=true`。只发布观察消息，不调用 Action 的 goal/result/cancel，不执行参数设置或 lifecycle 转换。默认拒绝 incomplete；恢复结果始终保留 incomplete 标记，需要显式允许。

存储使用固定 `mcap 0.26.0` 上游 Writer/reader，启用 CRC，不启用压缩。业务字节及同 chunk 配套 metadata 成对提交；内部 metadata 永不回放到网络。到总容量后停止，不清理旧 shard。`clean_close` 表示容器正确关闭，不表示断电零丢失。

运行检查：`cargo test -p aos-bag`、`cargo clippy -p aos-bag --all-targets -- -D warnings`。网络测试只监听 `127.0.0.1` 的临时端口。它们证明本机 Rust 行为；CLI/Python、Mac/Linux 与跨机验收需要集成后另行完成。
