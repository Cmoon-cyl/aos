#![cfg(windows)]

use std::io::Write;
use std::process::{Command, Stdio};

fn invalid_request(arguments: &[&str]) -> Vec<serde_json::Value> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_aos"))
        .args(arguments)
        .args(["--request", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // An invalid request may be rejected before Windows consumes the pipe.
    // The observable error frame, not this write, is the contract under test.
    let _ = child.stdin.take().unwrap().write_all(b"{}");
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn native_streaming_commands_share_the_versioned_bounded_output_protocol() {
    for arguments in [
        vec!["run"],
        vec!["launch"],
        vec!["bag", "record"],
        vec!["bag", "play"],
        vec!["security", "serve"],
    ] {
        let events = invalid_request(&arguments);
        assert!(!events.is_empty(), "{arguments:?}");
        assert!(
            events.iter().all(|event| event["schema_version"] == 1),
            "{events:?}"
        );
        assert_eq!(events.last().unwrap()["event"], "error");
    }
}
