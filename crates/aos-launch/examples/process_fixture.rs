//! Native subprocess fixture. It performs no work unless its test-specific
//! marker path and mode are explicitly provided by the acceptance harness.
use std::{
    io::{Read, Write},
    time::Duration,
};
#[expect(
    clippy::zombie_processes,
    reason = "the orphan fixture deliberately exits first so the supervisor must clean its descendant"
)]
fn main() {
    let Some(marker) = std::env::var_os("AOS_PROCESS_FIXTURE_MARKER") else {
        return;
    };
    let mode = std::env::var("AOS_PROCESS_FIXTURE_MODE").unwrap_or_default();
    if mode == "child" {
        std::fs::write(&marker, std::process::id().to_string()).unwrap();
    } else if mode == "tree" || mode == "tree_exit" {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child.env("AOS_PROCESS_FIXTURE_MODE", "child");
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            child.creation_flags(0x200);
        }
        let mut child = child.spawn().unwrap();
        if mode == "tree_exit" {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while !std::path::Path::new(&marker).is_file() {
                assert!(std::time::Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(5));
            }
            return;
        }
        let _ = child.wait();
        return;
    } else {
        std::fs::write(&marker, std::process::id().to_string()).unwrap();
    }
    match mode.as_str() {
        "echo" => {
            let mut bytes = Vec::new();
            std::io::stdin().read_to_end(&mut bytes).unwrap();
            println!(
                "{}",
                serde_json::json!({"decision":"matched", "reason":bytes.len().to_string()})
            );
            return;
        }
        "flood" => loop {
            if std::io::stdout().write_all(&[b'x'; 8192]).is_err() {
                return;
            }
        },
        _ => {}
    }
    std::thread::sleep(Duration::from_secs(60));
}
