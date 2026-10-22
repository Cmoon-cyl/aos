use std::{sync::Arc, time::Duration};

use aos_core::{
    transport::{PublisherBackend, SubscriptionBackend, Transport},
    Context, Endpoint, Error, Interface, Result, TypeDescriptionLimits,
    MAX_INTERFACE_DEFINITION_BYTES, MAX_INTERFACE_REGEXES, MAX_INTERFACE_SCHEMA_BYTES,
    MAX_INTERFACE_SCHEMA_NODES,
};
use serde_json::{json, Value};

fn message(schema: Value) -> Value {
    json!({"name":"test/msg/Bounded","kind":"msg","message":schema})
}

fn reference_dag(levels: usize) -> Value {
    let mut definitions = serde_json::Map::new();
    definitions.insert("n0".into(), json!({"type":"integer"}));
    for level in 1..=levels {
        let target = format!("#/$defs/n{}", level - 1);
        definitions.insert(
            format!("n{level}"),
            json!({"allOf":[{"$ref":target},{"$ref":target}]}),
        );
    }
    json!({"$defs":definitions,"$ref":format!("#/$defs/n{levels}")})
}

#[test]
fn expanded_reference_cost_charges_repeated_edges_and_keeps_local_scopes() {
    // Root plus expanded n8 costs 1,022 visits; n9 costs 2,046, despite
    // containing only 19 reference keywords and a small source definition.
    Interface::from_json(message(reference_dag(8)))
        .unwrap()
        .validate("message", &json!(0))
        .unwrap();
    for levels in [9, 24] {
        let error = Interface::from_json(message(reference_dag(levels))).unwrap_err();
        assert!(
            error.to_string().contains("expanded schema visits"),
            "{error}"
        );
    }
    let schema = json!({
        "$defs":{
            "Scoped":{"$id":"urn:aos:scoped","$defs":{"Inner":{"type":"integer"}},"$ref":"#/$defs/Inner"},
            "Space Name":{"type":"integer"},
            "Anchored":{"$anchor":"number","type":"integer"}
        },
        "allOf":[{"$ref":"#/$defs/Scoped"},{"$ref":"#/$defs/Space%20Name"},{"$ref":"#number"}]
    });
    let interface = Interface::from_json(message(schema)).unwrap();
    interface.validate("message", &json!(7)).unwrap();
    assert!(interface.validate("message", &json!("bad")).is_err());
    let mut unused = reference_dag(24);
    unused.as_object_mut().unwrap().remove("$ref");
    Interface::from_json(message(unused))
        .unwrap()
        .validate("message", &json!(null))
        .unwrap();
}

#[test]
fn recursive_references_are_explicitly_rejected_in_all_schema_positions() {
    for schema in [
        json!({"$ref":"#"}),
        json!({"$dynamicAnchor":"loop","$dynamicRef":"#loop"}),
        json!({"$defs":{"unused":{"$ref":"#/$defs/unused"}},"type":"integer"}),
        json!({"$defs":{"a":{"$ref":"#/$defs/b"},"b":{"$ref":"#/$defs/a"}},"$ref":"#/$defs/a"}),
    ] {
        let error = Interface::from_json(message(schema)).unwrap_err();
        assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
        assert!(
            error.to_string().contains("recursive schema references"),
            "{error}"
        );
    }
}

#[test]
fn referenced_data_objects_enter_the_same_schema_resource_budget() {
    let heavy: Vec<Value> = (0..MAX_INTERFACE_REGEXES + 1)
        .map(|i| json!({"pattern":format!("^p{i}$")}))
        .collect();
    // Unreferenced business defaults remain data, including schema-like keys.
    let mut schema = json!({"default":{"allOf":heavy},"type":"string"});
    Interface::from_json(message(schema.clone())).unwrap();
    schema["$ref"] = json!("#/default");
    let error = Interface::from_json(message(schema)).unwrap_err();
    assert!(error.to_string().contains("distinct regexes"), "{error}");
    for target in [
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"$defs":{"unused":{"$ref":"#/default/$defs/unused"}}}),
    ] {
        let error = Interface::from_json(message(json!({"default":target,"$ref":"#/default"})))
            .unwrap_err();
        assert!(
            error.to_string().contains("external schema references")
                || error.to_string().contains("recursive schema references")
                || error
                    .to_string()
                    .contains("Default retriever does not fetch resources"),
            "{error}"
        );
    }
    // The same referenced object is counted once structurally. Its unused
    // definitions still count, while each of ten reference edges has work cost.
    let definitions: serde_json::Map<String, Value> = (0..MAX_INTERFACE_SCHEMA_NODES - 12)
        .map(|i| (format!("n{i}"), json!(true)))
        .collect();
    let mut schema =
        json!({"default":{"$defs":definitions},"allOf":vec![json!({"$ref":"#/default"});10]});
    Interface::from_json(message(schema.clone())).unwrap();
    schema["default"]["$defs"]["extra"] = json!(true);
    let error = Interface::from_json(message(schema)).unwrap_err();
    assert!(error.to_string().contains("schema nodes"), "{error}");
}

#[test]
fn payload_work_admission_rejects_the_old_heavy_sample_and_accepts_the_exact_budget() {
    let branches: Vec<Value> = (0..MAX_INTERFACE_SCHEMA_NODES - 2)
        .map(|i| json!({"type":"integer","minimum":-(i as i64)}))
        .collect();
    let interface =
        Interface::from_json(message(json!({"type":"array","items":{"allOf":branches}}))).unwrap();
    // Expanded schema cost 1,024, payload units 8,192: exactly 8 Mi units.
    interface
        .validate("message", &json!(vec![0; 8191]))
        .unwrap();
    for items in [8192, 500_000] {
        let error = interface
            .validate("message", &json!(vec![0; items]))
            .unwrap_err();
        assert!(error.to_string().contains("validation work"), "{error}");
    }
    // C20-sized payload and a simple array remain within the shared profile.
    let simple = Interface::from_json(message(json!({"type":"object","properties":{
        "sequence":{"type":"integer"},"payload":{"type":"string"}},"required":["sequence","payload"]}))).unwrap();
    simple
        .validate(
            "message",
            &json!({"sequence":1,"payload":"x".repeat(256 * 1024)}),
        )
        .unwrap();
    Interface::from_json(message(json!({"type":"array","items":{"type":"integer"}})))
        .unwrap()
        .validate("message", &json!(vec![0; 500_000]))
        .unwrap();
}

#[test]
fn local_file_size_is_checked_before_json_parsing() {
    let path =
        std::env::temp_dir().join(format!("aos-interface-size-{}.json", uuid::Uuid::new_v4()));
    std::fs::write(&path, vec![b'!'; MAX_INTERFACE_DEFINITION_BYTES + 1]).unwrap();
    let result = Interface::load(&path);
    std::fs::remove_file(path).unwrap();
    let error = result.unwrap_err();
    assert!(matches!(error, Error::ResourceLimit(_)));
    assert!(error.to_string().contains("file exceeds"));
}

#[test]
fn schema_bytes_are_checked_before_compiling_an_unsupported_pattern() {
    let schema =
        json!({"type":"string","pattern":"(?=unsupported)","description":"x".repeat(1024)});
    let size = serde_json::to_vec(&schema).unwrap().len();
    let error = Interface::from_json_bounded(message(schema.clone()), size - 1).unwrap_err();
    assert!(error.to_string().contains("schema_bytes"), "{error}");
    let error = Interface::from_json_bounded(message(schema), size).unwrap_err();
    assert!(
        error.to_string().contains("invalid message schema"),
        "{error}"
    );
}

#[test]
fn regex_syntax_errors_keep_the_original_schema_part_diagnostic() {
    for (kind, fields) in [
        ("msg", &["message"][..]),
        ("srv", &["request", "response"][..]),
        ("action", &["goal", "result", "feedback"][..]),
    ] {
        for field in fields {
            for pattern in ["[", "(?=unsupported)"] {
                let mut definition = json!({"name":format!("test/{kind}/RegexError"),"kind":kind});
                for part in fields {
                    definition[*part] = json!({"type":"null","default":null});
                }
                definition[*field] = json!({"type":"string","pattern":pattern});
                let error = Interface::from_json(definition).unwrap_err();
                assert!(matches!(error, Error::InvalidArgument(_)), "{error:?}");
                assert!(
                    error
                        .to_string()
                        .contains(&format!("invalid {field} schema")),
                    "{kind}/{field}/{pattern}: {error}"
                );
            }
        }
    }
}

#[test]
fn draft_2020_12_rejects_legacy_tuple_items_before_any_payload_validation() {
    let definition = message(json!({"items":[{"type":"string","pattern":"[a-z]{2000}"}]}));
    let error = Interface::from_json(definition).unwrap_err();
    assert!(matches!(error, Error::InvalidArgument(_)), "{error:?}");
    assert!(
        error.to_string().contains("invalid message schema"),
        "{error}"
    );
}

#[test]
fn schema_node_boundary_and_pattern_cache_domains_are_exact() {
    let accepted = message(json!({"allOf":vec![true; MAX_INTERFACE_SCHEMA_NODES - 1]}));
    Interface::from_json(accepted)
        .unwrap()
        .validate("message", &json!(0))
        .unwrap();
    let error = Interface::from_json(message(
        json!({"allOf":vec![true; MAX_INTERFACE_SCHEMA_NODES]}),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("schema nodes"));

    let patterns = |count: usize, same: bool| -> Vec<Value> {
        (0..count)
            .map(|i| json!({"pattern":format!("^p{}$", if same { 0 } else { i })}))
            .collect()
    };
    Interface::from_json(message(
        json!({"allOf":patterns(MAX_INTERFACE_REGEXES + 1, true)}),
    ))
    .unwrap();
    Interface::from_json(message(
        json!({"allOf":patterns(MAX_INTERFACE_REGEXES, false)}),
    ))
    .unwrap();
    let error = Interface::from_json(message(
        json!({"allOf":patterns(MAX_INTERFACE_REGEXES + 1, false)}),
    ))
    .unwrap_err();
    assert!(error.to_string().contains("distinct regexes"));
    // Eight regex strings occur in both roots, but each build owns a separate cache.
    let error = Interface::from_json(json!({"name":"test/srv/Bounded","kind":"srv",
        "request":{"allOf":patterns(8, false)}, "response":{"allOf":patterns(9, false)}}))
    .unwrap_err();
    assert!(error.to_string().contains("distinct regexes"));
}

#[test]
fn pattern_properties_count_but_business_data_does_not() {
    let patterns: serde_json::Map<String, Value> = (0..MAX_INTERFACE_REGEXES + 1)
        .map(|i| (format!("^p{i}$"), json!(true)))
        .collect();
    let error = Interface::from_json(message(json!({"patternProperties":patterns}))).unwrap_err();
    assert!(error.to_string().contains("distinct regexes"));
    Interface::from_json(message(json!({
        "type":"object", "properties":{"$ref":{"type":"string"},"pattern":{"type":"string"}},
        "examples":[{"$ref":"https://example.invalid","pattern":"(?=data)"}],
        "default":{"$ref":"https://example.invalid","pattern":"(?=data)"}
    })))
    .unwrap();
}

#[test]
fn resource_limits_only_allow_further_restriction() {
    for limits in [
        TypeDescriptionLimits {
            response_bytes: 0,
            ..TypeDescriptionLimits::default()
        },
        TypeDescriptionLimits {
            response_bytes: MAX_INTERFACE_DEFINITION_BYTES + 1,
            ..TypeDescriptionLimits::default()
        },
        TypeDescriptionLimits {
            schema_bytes: 0,
            ..TypeDescriptionLimits::default()
        },
        TypeDescriptionLimits {
            schema_bytes: MAX_INTERFACE_SCHEMA_BYTES + 1,
            ..TypeDescriptionLimits::default()
        },
    ] {
        assert!(limits.validate().is_err());
    }
    TypeDescriptionLimits {
        response_bytes: 1,
        schema_bytes: 1,
    }
    .validate()
    .unwrap();
    assert!(
        Interface::from_json_bounded(message(json!(true)), MAX_INTERFACE_SCHEMA_BYTES + 1).is_err()
    );
}

#[derive(Clone)]
struct LegacyReply(Arc<Vec<u8>>);
impl Transport for LegacyReply {
    fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("fixture".into()))
    }
    fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
        Err(Error::Unsupported("fixture".into()))
    }
    fn call(&self, _: &Endpoint, _: &[u8], _: Duration) -> Result<Vec<u8>> {
        Ok((*self.0).clone())
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
struct IgnoresLimit(LegacyReply);
impl Transport for IgnoresLimit {
    fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("fixture".into()))
    }
    fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
        Err(Error::Unsupported("fixture".into()))
    }
    fn call_bounded(&self, _: &Endpoint, _: &[u8], _: Duration, _: usize) -> Result<Vec<u8>> {
        Ok((*self.0 .0).clone())
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn legacy_and_noncompliant_transports_cannot_decode_oversized_descriptions() {
    // Invalid JSON distinguishes byte admission from a later decoder failure.
    let reply = LegacyReply(Arc::new(vec![b'!'; 65]));
    let contexts = [
        Context::new(reply.clone()),
        Context::new(IgnoresLimit(reply)),
    ];
    for context in contexts {
        let error = context
            .get_type_description_bounded(
                "/source",
                "test/msg/Bounded",
                None,
                Duration::from_secs(1),
                TypeDescriptionLimits {
                    response_bytes: 64,
                    ..TypeDescriptionLimits::default()
                },
            )
            .unwrap_err();
        assert!(matches!(error, Error::QueueOverflow(_)), "{error}");
    }
}

#[test]
fn remote_schema_limit_precedes_build_and_exact_limits_keep_the_hash() {
    let schema = json!({"type":"string","pattern":"(?=unsupported)","description":"x".repeat(256)});
    let size = serde_json::to_vec(&schema).unwrap().len();
    let reply = serde_json::to_vec(&message(schema)).unwrap();
    let context = Context::new(LegacyReply(Arc::new(reply)));
    let error = context
        .get_type_description_bounded(
            "/source",
            "test/msg/Bounded",
            None,
            Duration::from_secs(1),
            TypeDescriptionLimits {
                schema_bytes: size - 1,
                ..TypeDescriptionLimits::default()
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("schema_bytes"), "{error}");
    let error = context
        .get_type_description("/source", "test/msg/Bounded", None, Duration::from_secs(1))
        .unwrap_err();
    assert!(
        error.to_string().contains("invalid message schema"),
        "{error}"
    );

    let definition = message(json!({"type":"integer"}));
    let expected = Interface::from_json(definition.clone()).unwrap();
    let reply = serde_json::to_vec(&definition).unwrap();
    let limits = TypeDescriptionLimits {
        response_bytes: reply.len(),
        schema_bytes: serde_json::to_vec(&definition["message"]).unwrap().len(),
    };
    let context = Context::new(LegacyReply(Arc::new(reply)));
    let actual = context
        .get_type_description_bounded(
            "/source",
            expected.type_name(),
            Some(expected.type_hash()),
            Duration::from_secs(1),
            limits,
        )
        .unwrap();
    assert_eq!(actual.type_hash(), expected.type_hash());
    actual.validate("message", &json!(7)).unwrap();
    assert!(matches!(
        actual.validate("message", &json!("invalid")),
        Err(Error::Validation(_))
    ));
}

#[test]
fn resource_refusal_is_distinct_from_invalid_argument_and_schema_mismatch() {
    let over_budget = Interface::from_json(message(
        json!({"allOf":vec![true; MAX_INTERFACE_SCHEMA_NODES]}),
    ))
    .unwrap_err();
    assert!(matches!(over_budget, Error::ResourceLimit(_)));
    assert_eq!(
        aos_core::TelemetryOutcome::from(&over_budget),
        aos_core::TelemetryOutcome::ResourceLimit
    );
    assert!(matches!(
        Interface::from_json_bounded(message(json!(true)), 0),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        Interface::from_json(message(json!({"type":43}))),
        Err(Error::InvalidArgument(_))
    ));
    let interface = Interface::from_json(message(json!({"type":"integer"}))).unwrap();
    assert!(matches!(
        interface.validate("message", &json!("wrong")),
        Err(Error::Validation(_))
    ));
    let work = Interface::from_json(message(json!({"type":"array", "items":{
        "allOf":vec![json!({"minimum":0}); MAX_INTERFACE_SCHEMA_NODES - 2]
    }})))
    .unwrap();
    assert!(matches!(
        work.validate("message", &json!(vec![0; 8192])),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn action_default_resource_refusal_keeps_its_category() {
    let definition = json!({
        "name":"test/action/DefaultBudget", "kind":"action",
        "goal":true, "feedback":true,
        "result":{"type":"array", "items":{
            "allOf":vec![json!({"minimum":0}); MAX_INTERFACE_SCHEMA_NODES - 4]
        }, "default":vec![0; 9000]}
    });
    assert!(matches!(
        Interface::from_json(definition),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn enum_work_matches_integer_fast_path_numeric_fallback_and_small_enum_compatibility() {
    let options: Vec<Value> = (0..60_000).map(|n| json!(n)).collect();
    let interface =
        Interface::from_json(message(json!({"type":"array","items":{"enum":options}}))).unwrap();
    // The R4 review's i64 example uses binary search in pinned jsonschema 0.59.
    interface
        .validate("message", &json!(vec![59_999; 174_762]))
        .unwrap();
    // The numerically equal float must fall back to 60,000 comparisons/value.
    interface.validate("message", &json!([59_999.0])).unwrap();
    assert!(matches!(
        interface.validate("message", &json!(vec![59_999.0; 140])),
        Err(Error::ResourceLimit(_))
    ));
    for options in [
        (0..250).map(|n| json!(n)).collect::<Vec<_>>(),
        (0..250)
            .map(|n| json!(format!("state-{n}")))
            .collect::<Vec<_>>(),
    ] {
        let payload = json!(vec![options[249].clone(); 10_000]);
        let interface =
            Interface::from_json(message(json!({"type":"array","items":{"enum":options}})))
                .unwrap();
        interface.validate("message", &payload).unwrap();
    }
}

#[test]
fn compound_enum_and_const_comparisons_charge_nested_values_and_text() {
    let options: Vec<Value> = (0..1000)
        .map(|n| {
            let mut row = vec![0; 16];
            row[15] = n;
            json!(row)
        })
        .collect();
    let last = options[999].clone();
    let interface =
        Interface::from_json(message(json!({"type":"array","items":{"enum":options}}))).unwrap();
    interface
        .validate("message", &json!([last.clone()]))
        .unwrap();
    assert!(matches!(
        interface.validate("message", &json!(vec![last; 512])),
        Err(Error::ResourceLimit(_))
    ));

    let text = "x".repeat(600_000);
    let interface = Interface::from_json(message(json!({
        "$defs":{"same":{"const":{"text":text}}},
        "allOf":vec![json!({"$ref":"#/$defs/same"}); 16]
    })))
    .unwrap();
    assert!(matches!(
        interface.validate("message", &json!({"text":text})),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn plain_image_and_large_strings_are_not_charged_per_schema_node_byte() {
    let image = Interface::from_json(message(json!({"type":"object","properties":{
        "header":{"type":"object","properties":{
            "stamp":{"type":"object","properties":{"sec":{"type":"integer"},"nanosec":{"type":"integer"}}},
            "frame_id":{"type":"string"}}},
        "height":{"type":"integer"}, "width":{"type":"integer"},
        "encoding":{"type":"string"}, "step":{"type":"integer"}, "data":{"type":"string"}
    }}))).unwrap();
    image
        .validate(
            "message",
            &json!({
                "header":{"stamp":{"sec":1,"nanosec":0},"frame_id":"camera"},
                "height":480,"width":640,"encoding":"mono16","step":1280,"data":"A".repeat(819_200)
            }),
        )
        .unwrap();
    Interface::from_json(message(json!({"type":"string"})))
        .unwrap()
        .validate(
            "message",
            &json!("x".repeat(aos_core::MAX_INTERFACE_VALIDATION_WORK + 1)),
        )
        .unwrap();
}

#[test]
fn regex_work_charges_each_pattern_and_property_key() {
    let patterns: Vec<Value> = (0..16)
        .map(|n| json!({"pattern":format!("^x{{{n},}}$")}))
        .collect();
    let interface = Interface::from_json(message(json!({"allOf":patterns}))).unwrap();
    interface
        .validate("message", &json!("x".repeat(500_000)))
        .unwrap();
    assert!(matches!(
        interface.validate("message", &json!("x".repeat(524_288))),
        Err(Error::ResourceLimit(_))
    ));
    let properties: serde_json::Map<String, Value> = (0..16)
        .map(|n| (format!("^x{{{n},}}$"), json!(true)))
        .collect();
    let interface = Interface::from_json(message(json!({"patternProperties":properties}))).unwrap();
    let mut payload = serde_json::Map::new();
    payload.insert("x".repeat(524_288), json!(0));
    assert!(matches!(
        interface.validate("message", &Value::Object(payload)),
        Err(Error::ResourceLimit(_))
    ));
}

#[test]
fn compiled_regex_size_refusal_is_not_an_invalid_schema_error() {
    for schema in [
        json!({"pattern":"^[0-9a-zA-Z]{1000000}$"}),
        json!({"patternProperties":{"^[0-9a-zA-Z]{1000000}$":true}}),
    ] {
        let error = Interface::from_json(message(schema)).unwrap_err();
        assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
        assert!(error.to_string().contains("regex compiled size"), "{error}");
    }
    let invalid = Interface::from_json(message(json!({"pattern":"(?=unsupported)"}))).unwrap_err();
    assert!(matches!(invalid, Error::InvalidArgument(_)), "{invalid:?}");
    // A literal fast path must not be forced through an unnecessary regex compile.
    let literal = "x".repeat(aos_core::MAX_INTERFACE_REGEX_SOURCE_BYTES - 2);
    Interface::from_json(message(json!({"pattern":format!("^{literal}$")}))).unwrap();
}

#[test]
fn deprecated_dependencies_use_the_same_schema_and_pattern_budgets() {
    let patterns: Vec<Value> = (0..17)
        .map(|n| json!({"pattern":format!("^p{n}$")}))
        .collect();
    let error = Interface::from_json(message(json!({"dependencies":{"a":{"allOf":patterns}}})))
        .unwrap_err();
    assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
    for dependency in [
        json!({"$ref":"https://example.invalid/schema"}),
        json!({"$schema":"http://json-schema.org/draft-07/schema#"}),
    ] {
        assert!(Interface::from_json(message(json!({"dependencies":{"a":dependency}}))).is_err());
    }
    let interface = Interface::from_json(message(json!({"dependencies":{"a":["b"]}}))).unwrap();
    interface
        .validate("message", &json!({"a":1,"b":2}))
        .unwrap();
    assert!(matches!(
        interface.validate("message", &json!({"a":1})),
        Err(Error::Validation(_))
    ));
}

#[test]
fn action_parent_checks_real_wrapper_node_and_byte_boundaries() {
    let action = |count: usize| {
        json!({
            "name":"test/action/WrapperBudget","kind":"action", "goal":{"allOf":vec![true; count]},
            "result":{"type":"object","default":{}},"feedback":{"type":"object"}
        })
    };
    let accepted = Interface::from_json(action(1013)).unwrap();
    aos_core::ActionInterfaces::new(&accepted).unwrap();
    for count in [1014, 1020] {
        let error = Interface::from_json(action(count)).unwrap_err();
        assert!(
            matches!(error, Error::ResourceLimit(_)),
            "count={count} {error:?}"
        );
    }
    for (count, accepted) in [(15, true), (16, false)] {
        let mut definition = action(1);
        let patterns: Vec<Value> = (0..count)
            .map(|n| json!({"pattern":format!("^p{n}$")}))
            .collect();
        definition["goal"] = json!({"allOf":patterns});
        let result = Interface::from_json(definition);
        if accepted {
            aos_core::ActionInterfaces::new(&result.unwrap()).unwrap();
        } else {
            assert!(matches!(result, Err(Error::ResourceLimit(_))));
        }
    }
    let mut goal = json!({"type":"object","description":""});
    let padding = aos_core::MAX_INTERFACE_SCHEMA_BYTES - serde_json::to_vec(&goal).unwrap().len();
    goal["description"] = json!("x".repeat(padding));
    assert_eq!(
        serde_json::to_vec(&goal).unwrap().len(),
        aos_core::MAX_INTERFACE_SCHEMA_BYTES
    );
    let mut definition = action(1);
    definition["goal"] = goal;
    let error = Interface::from_json(definition).unwrap_err();
    assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
}

#[test]
fn regex_program_legacy_recursive_reference_is_rejected_only_in_schema_positions() {
    let literal = format!("^{}$", "x".repeat(1024));
    for schema in [
        json!({"$recursiveRef":"#"}),
        json!({"$recursiveRef":null}),
        json!({"$defs":{"unused":{"$recursiveRef":"#"}}}),
        json!({"patternProperties":{literal:true},"properties":{
            "nested":{"$recursiveRef":"#","unevaluatedProperties":false}
        }}),
        json!({"default":{"$recursiveRef":"#"},"$ref":"#/default"}),
    ] {
        // Construction only. The legacy property-evaluation path was identified
        // in pinned upstream source; this test does not run a recursive payload.
        let error = Interface::from_json(message(schema)).unwrap_err();
        assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
        assert!(error.to_string().contains("legacy $recursiveRef"));
    }
    for schema in [
        json!({"default":{"$recursiveRef":"#"},"examples":[{"$recursiveRef":"#"}],
            "custom":{"$recursiveRef":"#"}}),
        json!({"type":"object","properties":{"$recursiveRef":{"type":"string"}}}),
    ] {
        Interface::from_json(message(schema)).unwrap();
    }
}

#[test]
fn regex_program_construction_rejects_expansion_before_any_payload() {
    let accepted = json!({"pattern":"[a-z]{763}"}); // 768 forward NFA states.
    Interface::from_json(message(accepted)).unwrap();
    for pattern in ["[a-z]{764}", "[a-z]{2000}"] {
        for schema in [
            json!({"pattern":pattern}),
            json!({"patternProperties":{pattern:true}}),
            json!({"patternProperties":{pattern:true},"additionalProperties":false}),
            json!({"patternProperties":{pattern:true},"additionalProperties":{"type":"integer"}}),
            json!({"patternProperties":{pattern:true},"unevaluatedProperties":false}),
        ] {
            // Establish the previous compiler's acceptance without ever
            // constructing or matching a large input string or property key.
            jsonschema::options()
                .with_draft(jsonschema::Draft::Draft202012)
                .with_pattern_options(
                    jsonschema::PatternOptions::regex()
                        .size_limit(aos_core::MAX_INTERFACE_REGEX_COMPILED_BYTES)
                        .dfa_size_limit(aos_core::MAX_INTERFACE_REGEX_DFA_BYTES),
                )
                .build(&schema)
                .unwrap();
            let error = Interface::from_json(message(schema)).unwrap_err();
            assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
            assert!(error.to_string().contains("regex NFA states exceed 768"));
        }
    }
}

#[test]
fn regex_program_literal_group_selection_matches_pinned_backend() {
    let literal = "x".repeat(1024);
    let exact = format!("^{literal}$");
    for pattern in [
        exact.clone(),
        format!("^{literal}"),
        format!("^({literal}|short)$"),
        r"^\S*$".into(),
    ] {
        Interface::from_json(message(json!({"pattern":pattern}))).unwrap();
        Interface::from_json(message(json!({"patternProperties":{pattern:true}}))).unwrap();
    }
    let mixed = json!({"patternProperties":{exact.clone():true,"^[a-z]+$":true}});
    // A mixed group uses full regex for every entry unless additionalProperties
    // owns the validation and can optimize each literal separately.
    for additional in [None, Some(json!(true))] {
        let mut schema = mixed.clone();
        if let Some(value) = additional {
            schema["additionalProperties"] = value;
        }
        let error = Interface::from_json(message(schema)).unwrap_err();
        assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
    }
    for additional in [json!(false), json!({"type":"integer"})] {
        let mut schema = mixed.clone();
        schema["additionalProperties"] = additional;
        Interface::from_json(message(schema)).unwrap();
    }
    Interface::from_json(message(json!({
        "patternProperties":{exact:true},"unevaluatedProperties":true
    })))
    .unwrap();
}

#[test]
fn regex_program_unevaluated_property_graph_checks_literals_and_references() {
    let pattern = format!("^{}$", "x".repeat(1024));
    let properties = json!({"patternProperties":{pattern:true}});
    for schema in [
        json!({"patternProperties":properties["patternProperties"],"unevaluatedProperties":false}),
        json!({"allOf":[properties.clone()],"unevaluatedProperties":false}),
        json!({"anyOf":[properties.clone()],"unevaluatedProperties":false}),
        json!({"oneOf":[properties.clone()],"unevaluatedProperties":false}),
        json!({"if":properties.clone(),"then":true,"unevaluatedProperties":false}),
        json!({"if":true,"then":properties.clone(),"unevaluatedProperties":false}),
        json!({"if":false,"else":properties.clone(),"unevaluatedProperties":false}),
        json!({"dependentSchemas":{"trigger":properties.clone()},"unevaluatedProperties":false}),
        json!({"$defs":{"target":properties.clone()},"$ref":"#/$defs/target","unevaluatedProperties":false}),
        json!({"$defs":{"target":{"$dynamicAnchor":"target","patternProperties":properties["patternProperties"]}},"$dynamicRef":"#target","unevaluatedProperties":false}),
        json!({"allOf":[properties.clone()],"unevaluatedProperties":{"type":"integer"}}),
    ] {
        let error = Interface::from_json(message(schema)).unwrap_err();
        assert!(matches!(error, Error::ResourceLimit(_)), "{error:?}");
        assert!(error.to_string().contains("regex NFA states exceed 768"));
    }
    // A parent's evaluation graph does not follow property values or unused
    // definitions: these keep their own literal optimization.
    for schema in [
        json!({"properties":{"nested":properties.clone()},"unevaluatedProperties":false}),
        json!({"$defs":{"unused":properties},"unevaluatedProperties":false}),
    ] {
        Interface::from_json(message(schema)).unwrap();
    }
}

#[test]
fn regex_program_small_unicode_and_property_payloads_keep_correctness() {
    for (pattern, valid, invalid) in [
        (
            r"^[0-9a-f]{32}$",
            "0123456789abcdef0123456789abcdef",
            "invalid",
        ),
        (r"^\p{L}+$", "张三Élodie", "123"),
        (r"^[\p{L}\p{N}_-]+$", "名字_42", "bad!"),
        (
            r"^[\p{L}\p{N}]+(?:[ '-][\p{L}\p{N}]+)*$",
            "Élodie 张三-42",
            "bad!",
        ),
        (r"^[\u4E00-\u9FFF]+$", "张三", "alice"),
        (r"^\S*$", "no-whitespace", "with\u{feff}space"),
    ] {
        let interface = Interface::from_json(message(json!({"pattern":pattern}))).unwrap();
        assert!(serde_json::to_vec(&json!(valid)).unwrap().len() < 4096);
        assert!(serde_json::to_vec(&json!(invalid)).unwrap().len() < 4096);
        interface.validate("message", &json!(valid)).unwrap();
        assert!(matches!(
            interface.validate("message", &json!(invalid)),
            Err(Error::Validation(_))
        ));
    }
    for schema in [
        json!({"patternProperties":{"^name$":{"type":"string"},"^[a-z]+$":{"type":"string"}},"additionalProperties":false}),
        json!({"patternProperties":{"^name$":{"type":"string"}},"additionalProperties":{"type":"integer"}}),
        json!({"allOf":[{"patternProperties":{"^name$":{"type":"string"}}}],"unevaluatedProperties":false}),
    ] {
        let interface = Interface::from_json(message(schema)).unwrap();
        let valid = json!({"name":"张三"});
        let invalid = json!({"name":3});
        assert!(serde_json::to_vec(&valid).unwrap().len() < 4096);
        assert!(serde_json::to_vec(&invalid).unwrap().len() < 4096);
        interface.validate("message", &valid).unwrap();
        assert!(matches!(
            interface.validate("message", &invalid),
            Err(Error::Validation(_))
        ));
    }
}

#[test]
fn regex_program_small_image_uuid_and_action_wrappers_remain_accepted() {
    let schema = json!({"type":"object","properties":{
        "id":{"type":"string","pattern":"^[0-9a-f]{32}$"},
        "data":{"type":"string"},"width":{"type":"integer"},"height":{"type":"integer"}
    }});
    let image = Interface::from_json(message(schema.clone())).unwrap();
    let payload = json!({"id":"0123456789abcdef0123456789abcdef","data":"A".repeat(128),"width":8,"height":8});
    assert!(serde_json::to_vec(&payload).unwrap().len() < 4096);
    image.validate("message", &payload).unwrap();
    let action = Interface::from_json(json!({
        "name":"test/action/RegexImage","kind":"action","goal":schema,
        "result":{"type":"object","default":{}},"feedback":{"type":"object"}
    }))
    .unwrap();
    aos_core::ActionInterfaces::new(&action).unwrap();
    action.validate("goal", &payload).unwrap();
    let patterns: Vec<Value> = (0..16)
        .map(|n| json!({"pattern":format!("^x{{{n},}}$")}))
        .collect();
    let interface = Interface::from_json(message(json!({"allOf":patterns}))).unwrap();
    interface
        .validate("message", &json!("x".repeat(32)))
        .unwrap();
}
