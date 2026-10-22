use serde_json::{json, Map, Value};

fn integer(minimum: u64, default: u64) -> Value {
    json!({"type":"integer","minimum":minimum,"default":default})
}

fn names() -> Value {
    json!({"type":"array","items":{"type":"string","minLength":1},
        "uniqueItems":true,"description":"Exact absolute Topic names; no glob or regex."})
}

fn limits() -> Value {
    json!({"type":"object","additionalProperties":false,"properties":{
        "record_bytes":integer(1,8*1024*1024),
        "schema_bytes":integer(1,1024*1024),
        "descriptor_bytes":integer(1,16*1024*1024),
        "index_bytes":integer(1,256*1024*1024),
        "channels":integer(1,4096),"shards":integer(1,256),
        "manifest_bytes":integer(1,4*1024*1024),
        "messages_per_shard":integer(1,1_000_000),
        "chunk_records":integer(1,65_536)
    },"description":"Native limits also validate relationships and maximum values before reading."})
}

fn storage() -> Value {
    json!({"type":"object","additionalProperties":false,"properties":{
        "chunk_bytes":integer(4096,4*1024*1024),
        "chunk_interval_ms":integer(1,1000),
        "shard_bytes":integer(1,256*1024*1024),
        "shard_interval_ms":integer(1,600_000),
        "total_bytes":integer(1,4*1024*1024*1024),
        "messages_per_shard":integer(1,65_536),
        "chunks_per_shard":integer(1,1024),"limits":limits()
    },"description":"Uncompressed CRC chunks. Capacity includes container/index/manifest reservations; reaching it stops without deleting older shards."})
}

fn qos_map() -> Value {
    json!({"type":"object","additionalProperties":crate::schema::qos_schema(false)})
}

pub fn request_schema(verb: &str) -> Option<Value> {
    if !matches!(verb, "record" | "info" | "recover" | "play") {
        return None;
    }
    let mut fields = Map::new();
    fields.insert("directory".into(), json!({"type":"string","minLength":1}));
    let mut required = vec!["directory"];
    let mut extra = Map::new();
    if matches!(verb, "record" | "play") {
        fields.insert("security_profile".into(), json!({"type":"string","minLength":1,
            "description":"Absolute client TLS profile path; otherwise AOS_SECURITY_PROFILE. Invalid secure configuration never falls back."}));
        fields.insert(
            "context_config".into(),
            crate::schema::deployment_schema(true),
        );
        fields.insert("node".into(), crate::schema::deployment_schema(false));
        fields.insert("topics".into(), names());
        fields.insert("exclude".into(), names());
        fields.insert("topic_qos".into(), qos_map());
        fields.insert("control_timeout_ms".into(), integer(1, 2000));
        fields.insert(
            "status_interval".into(),
            json!({"type":"number","minimum":0.02,"default":1,
                "description":"Steady seconds between compact progress snapshots."}),
        );
        fields.insert("shutdown_timeout".into(),
            json!({"type":"number","exclusiveMinimum":0,"default":5,
                "description":"Steady seconds to wait for native resources to close after stop; timeout is not completion."}));
        fields.insert("duration".into(), json!({"type":"number","exclusiveMinimum":0,
            "description":"Maximum live steady seconds after start. Record defaults to 30; play defaults to file completion."}));
    }
    match verb {
        "record" => {
            fields.insert(
                "all_topics".into(),
                json!({"type":"boolean","default":false}),
            );
            fields.insert("follow".into(), json!({"type":"boolean","default":false,
                "description":"Disable the default 30-second recording limit; conflicts with duration."}));
            fields.insert("qos".into(), crate::schema::qos_schema(false));
            fields.get_mut("qos").unwrap()["description"] = json!(
                "Omitted or null uses the recorder default: best_effort, volatile, depth 256."
            );
            for (name, default) in [
                ("discovery_interval_ms", 100),
                ("description_timeout_ms", 200),
                ("queue_messages", 1024),
                ("queue_bytes", 32 * 1024 * 1024),
                ("adapter_queue_bytes", 256 * 1024 * 1024),
                ("message_bytes", 1024 * 1024),
                ("metadata_bytes", 64 * 1024),
            ] {
                fields.insert(name.into(), integer(1, default));
            }
            fields.get_mut("adapter_queue_bytes").unwrap()["maximum"] = json!(1024 * 1024 * 1024);
            fields.get_mut("adapter_queue_bytes").unwrap()["description"] = json!(
                "Per-endpoint queued payload plus in-progress-copy budget; depth * message_bytes must fit."
            );
            fields.insert("storage".into(), storage());
            extra.insert("oneOf".into(), json!([
                {"required":["topics"],"properties":{"topics":{"minItems":1},"all_topics":{"const":false}}},
                {"required":["all_topics"],"properties":{"all_topics":{"const":true},"topics":{"maxItems":0}}}
            ]));
        }
        "play" => {
            fields.insert(
                "namespace".into(),
                json!({"type":"string","minLength":2,
                "description":"Explicit normalized non-root replay namespace, e.g. /replay."}),
            );
            fields.insert(
                "original_names".into(),
                json!({"type":"boolean","default":false}),
            );
            fields.insert(
                "channel_ids".into(),
                json!({"type":"array","uniqueItems":true,
                "items":{"type":"string","pattern":"^[0-9a-f]{64}$"}}),
            );
            fields.insert("channel_qos".into(), qos_map());
            fields.insert(
                "rate".into(),
                json!({"type":"number","exclusiveMinimum":0,"default":1}),
            );
            fields.insert(
                "start_paused".into(),
                json!({"type":"boolean","default":false,
                "description":"Requires control_input=stdin, so a caller can resume."}),
            );
            fields.insert("start_offset_ns".into(), integer(0, 0));
            fields.insert("subscriber_wait_ms".into(), integer(0, 0));
            fields.insert("control_input".into(), json!({"enum":["none","stdin"],"default":"none",
                "description":"stdin consumes bounded JSONL control commands; use a request file, not --request -. EOF requests stop."}));
            fields.insert(
                "allow_incomplete".into(),
                json!({"type":"boolean","default":false}),
            );
            fields.insert("limits".into(), limits());
            fields.insert("clock".into(), json!({"oneOf":[{"type":"null"},
                {"type":"object","additionalProperties":false,"properties":{
                    "topic":{"type":"string","minLength":1,"default":"/clock"},
                    "period_ms":integer(1,20)}}],
                "description":"Explicitly generate replay clock under the same namespace/remap rules; omit to disable."}));
            extra.insert(
                "oneOf".into(),
                json!([
                    {"required":["namespace"],"properties":{"original_names":{"const":false}}},
                    {"required":["original_names"],"properties":{"original_names":{"const":true}},
                        "not":{"required":["namespace"]}}
                ]),
            );
        }
        "info" => {
            fields.insert(
                "allow_incomplete".into(),
                json!({"type":"boolean","default":false}),
            );
            fields.insert("limits".into(), limits());
        }
        "recover" => {
            fields.insert("destination".into(), json!({"type":"string","minLength":1,
                "description":"A new destination outside the source recording; source stays unchanged."}));
            fields.insert("storage".into(), storage());
            required.push("destination");
        }
        _ => unreachable!(),
    }
    let mut schema = json!({"$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object","additionalProperties":false,"properties":fields,"required":required});
    schema.as_object_mut().unwrap().extend(extra);
    Some(schema)
}

pub fn control_schema() -> Value {
    json!({"type":"object","additionalProperties":false,
        "required":["id","command"],"properties":{
            "id":{"type":"string","minLength":1,"maxLength":256},
            "command":{"enum":["pause","resume","seek","rate","stop","status"]},
            "offset_ns":{"type":"integer","minimum":0},
            "rate":{"type":"number","exclusiveMinimum":0}
        },"description":"seek requires only offset_ns; rate requires only rate. Other commands accept neither. Timeout leaves the barrier unconfirmed; inspect the returned status."})
}

pub fn describe(verb: &str) -> Option<Value> {
    let request = request_schema(verb)?;
    let live = matches!(verb, "record" | "play");
    let mut result = json!({
        "command":format!("aos bag {verb}"),
        "input":"--request PATH|-",
        "request_schema":request,
        "output_format":if live {"jsonl"} else {"json"},
        "output_schema":if live {
            json!({"type":"object","required":["schema_version","event","data"],
                "properties":{"schema_version":{"const":1},"event":{"type":"string"},"data":{}}})
        } else {
            json!({"type":"object","required":["ok"],
                "properties":{"ok":{"type":"boolean"},"data":{},"error":{"type":"object"}}})
        },
        "scope":"MCAP aos-json-v1. Recorder preserves validated serialized bytes and source/receive metadata. Playback publishes observation Topics only; it never calls Services, sends/cancels Action goals, sets parameters or transitions lifecycle.",
        "integer_representation":"Counts, byte sizes and nanoseconds use unsigned JSON integers, not 3.0 or bool.",
        "completion":"stop requests closure. wait returns the worker report. A summary with resources_closed=true and cleanup_error=null confirms native worker/file cleanup; inspect state, error, incomplete and stop_reason separately. Published counts are local submissions, not consumer acknowledgements.",
        "subscriber_wait":"Zero skips waiting. A positive subscriber_wait_ms requires every selected data publisher and optional clock publisher to match before start commits; timeout leaves Ready and names unmatched endpoints. Start uses subscriber_wait_ms plus control_timeout_ms. Matching is not consumer processing.",
        "control_commit":"Successful controls acknowledge local cursor/timeline/epoch commit. Clock publication follows the acknowledgement; a later failure is visible in state/error. A timed out control is cancelled and cannot execute later. Ready seek remains Ready and publishes no clock.",
        "exit_codes":{"0":"operation completed; inspect manifest completeness and local loss counters",
            "1":"storage/runtime/output failure","2":"invalid input, corrupt bag or disallowed incomplete source",
            "3":"transport or closed context","4":"control/closure timeout",
            "129":"SIGHUP after closure","130":"SIGINT after closure",
            "131":"SIGQUIT after closure","143":"SIGTERM after closure"}
    });
    if live {
        result["platforms"] = json!("record/play CLI currently requires Unix; offline info/recover and SDK use the shared native runtime.");
        result["output"] = json!({
            "progress":"Compact snapshots with coalesced_status_snapshots; full channel Interface definitions are available through bag info after closure.",
            "limits":"4 MiB bounded JSONL output queue; full with no write progress for 30 seconds fails and requests stop. Terminal summary/error drain has a shared 2-second budget.",
            "signals":"SIGINT/SIGTERM/SIGHUP/SIGQUIT handlers are installed before creating the bag handle; default OS behavior remains during the earlier network connection.",
        });
    }
    if verb == "record" {
        result["defaults"] = json!({"duration_seconds":30,"qos":{"depth":256,
            "reliability":"best_effort","durability":"volatile"}});
        result["discovery"] = json!("Construction does not mean a channel was discovered. Inspect active_channel_count and unresolved_count. Exact names/type hashes remain independent.");
    }
    if verb == "play" {
        result["controls"] = json!({
            "input":"One JSON object per UTF-8 line; 64 KiB maximum line, 32-command input queue. Request must come from a file.",
            "schema":control_schema(),
            "reply":"control_result preserves id, ok, native status and any error; a failed command does not silently change state.",
            "eof":"Stop and EOF request closure independently of output backpressure. Unapplied queued commands appear in summary.unapplied_controls; stop's response is also in terminal_control_result. Await each control reply before sending the next when order matters.",
            "input_failure":"An overlong line, a full 32-command queue, or an input read failure stops playback with CONTROL_INPUT_ERROR. Other malformed lines produce a control_result with id=null and leave playback running.",
            "barrier":"Successful pause stops new submissions; successful seek changes epoch before new submissions. Already submitted transport/consumer messages cannot be recalled.",
            "examples":[{"id":"p1","command":"pause"},{"id":"s1","command":"seek","offset_ns":0},
                {"id":"r1","command":"rate","rate":2},{"id":"go1","command":"resume"}]
        });
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_accepts_explicit_adapter_budget_and_matches_core_limits() {
        let schema = request_schema("record").unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let request = json!({
            "directory":"/unused/recording","topics":["/large"],
            "message_bytes":2 * 1024 * 1024,
            "adapter_queue_bytes":512 * 1024 * 1024
        });
        assert!(validator.is_valid(&request));
        let options: aos_bag::RecordOptions = serde_json::from_value(request.clone()).unwrap();
        options.validate().unwrap();
        let mut invalid = request;
        invalid["adapter_queue_bytes"] = json!(1024_u64 * 1024 * 1024 + 1);
        assert!(!validator.is_valid(&invalid));
        let options: aos_bag::RecordOptions = serde_json::from_value(invalid).unwrap();
        assert!(options.validate().is_err());
    }

    #[test]
    fn every_reader_entry_accepts_an_explicit_index_budget() {
        for verb in ["record", "info", "recover", "play"] {
            let schema = request_schema(verb).unwrap();
            let validator = jsonschema::validator_for(&schema).unwrap();
            let budget = json!({"index_bytes": 256 * 1024 * 1024});
            let mut request = json!({"directory":"/unused/bag"});
            if matches!(verb, "record" | "recover") {
                request["storage"] = json!({"limits":budget});
            } else {
                request["limits"] = budget;
            }
            if verb == "recover" {
                request["destination"] = json!("/unused/recovered");
            }
            if verb == "record" {
                request["topics"] = json!(["/source"]);
            }
            if verb == "play" {
                request["namespace"] = json!("/replay");
            }
            assert!(validator.is_valid(&request), "{verb}: {request}");
        }
    }
}
