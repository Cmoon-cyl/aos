use std::time::Duration;

use aos_core::{
    IntegerRange, Interface, Parameter, ParameterDescriptor, ParameterError, ParameterOverride,
    ParameterServiceKind, ParameterStore, ParameterType, ParameterValue,
};
use serde_json::json;

fn restore_store() -> ParameterStore {
    let store = ParameterStore::new("/restore", "restore");
    for (name, value, descriptor) in [
        (
            "dynamic",
            ParameterValue::NotSet,
            ParameterDescriptor {
                dynamic_typing: true,
                ..Default::default()
            },
        ),
        (
            "static",
            ParameterValue::NotSet,
            ParameterDescriptor {
                parameter_type: Some(ParameterType::Integer),
                ..Default::default()
            },
        ),
        (
            "constant",
            ParameterValue::Integer(3),
            ParameterDescriptor {
                read_only: true,
                ..Default::default()
            },
        ),
        (
            "changed",
            ParameterValue::Integer(1),
            ParameterDescriptor {
                integer_range: Some(IntegerRange {
                    from_value: 1,
                    to_value: 5,
                    step: 1,
                }),
                ..Default::default()
            },
        ),
    ] {
        store.declare(name, value, descriptor, false).unwrap();
    }
    store
}

fn unchanged(store: &ParameterStore) -> Vec<Parameter> {
    store
        .snapshot()
        .unwrap()
        .parameters
        .into_iter()
        .map(|p| Parameter::new(p.name, p.value))
        .collect()
}

#[test]
fn stage3_r2_not_set_override_cannot_erase_a_concrete_static_default() {
    let store = || {
        ParameterStore::with_overrides(
            "/restore",
            "restore",
            [(
                "x".into(),
                ParameterOverride {
                    value: ParameterValue::NotSet,
                    source: "old_snapshot".into(),
                },
            )]
            .into(),
        )
    };
    let concrete = store();
    let before = json!(concrete.snapshot().unwrap());
    assert!(matches!(
        concrete.prepare_declare(
            "x",
            ParameterValue::Integer(5),
            ParameterDescriptor::default(),
            false,
        ),
        Err(ParameterError::WrongType(message))
            if message.contains("NotSet startup override")
    ));
    assert_eq!(json!(concrete.snapshot().unwrap()), before);
    concrete
        .declare(
            "x",
            ParameterValue::Integer(5),
            ParameterDescriptor::default(),
            true,
        )
        .unwrap();
    assert_eq!(concrete.get("x").unwrap(), ParameterValue::Integer(5));

    let uninitialized = store();
    uninitialized
        .declare(
            "x",
            ParameterValue::NotSet,
            ParameterDescriptor {
                parameter_type: Some(ParameterType::Integer),
                ..Default::default()
            },
            false,
        )
        .unwrap();
    assert!(matches!(
        uninitialized.get("x"),
        Err(ParameterError::Uninitialized(_))
    ));
    let record = uninitialized.describe(&["x".into()]).unwrap().remove(0);
    assert_eq!(record.default_value, ParameterValue::NotSet);
    assert_eq!(record.source, "old_snapshot");

    let dynamic = store();
    dynamic
        .declare(
            "x",
            ParameterValue::Integer(5),
            ParameterDescriptor {
                dynamic_typing: true,
                ..Default::default()
            },
            false,
        )
        .unwrap();
    assert_eq!(dynamic.get("x").unwrap(), ParameterValue::NotSet);
}

#[cfg(unix)]
#[test]
fn stage3_r2_snapshot_write_uses_private_atomic_replacement() {
    use std::os::unix::fs::PermissionsExt;
    let directory = std::env::temp_dir().join(format!(
        "aos-snapshot-review-{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir(&directory).unwrap();
    let output = directory.join("snapshot.json");
    let previous = directory.join("previous_inode");
    let store = restore_store();
    let before = store.snapshot().unwrap();
    before.write(&output).unwrap();
    assert_eq!(
        std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
        0o600
    );
    std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o640)).unwrap();
    std::fs::hard_link(&output, &previous).unwrap();
    store
        .set_atomically(vec![Parameter::new("changed", ParameterValue::Integer(2))])
        .unwrap();
    let after = store.snapshot().unwrap();
    after.write(&output).unwrap();
    assert_eq!(
        json!(aos_core::ParameterSnapshot::read(&output).unwrap()),
        json!(after)
    );
    assert_eq!(
        json!(aos_core::ParameterSnapshot::read(&previous).unwrap()),
        json!(before)
    );
    assert_eq!(
        std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
        0o640
    );
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 2);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn snapshot_load_preserves_a_declared_dynamic_not_set_parameter() {
    let store = ParameterStore::new("/restore", "restore");
    store
        .declare(
            "pending",
            ParameterValue::NotSet,
            ParameterDescriptor {
                dynamic_typing: true,
                ..Default::default()
            },
            false,
        )
        .unwrap();
    let snapshot = store.snapshot().unwrap();
    store.prepare_load(&snapshot).unwrap().commit().unwrap();
    assert_eq!(
        store.describe(&["pending".into()]).unwrap()[0].value,
        ParameterValue::NotSet
    );
}

#[test]
fn snapshot_load_preserves_a_declared_static_not_set_parameter() {
    let store = ParameterStore::new("/restore", "restore");
    store
        .declare(
            "pending",
            ParameterValue::NotSet,
            ParameterDescriptor {
                parameter_type: Some(ParameterType::Integer),
                ..Default::default()
            },
            false,
        )
        .unwrap();
    let snapshot = store.snapshot().unwrap();
    store.prepare_load(&snapshot).unwrap().commit().unwrap();
    assert_eq!(
        store.describe(&["pending".into()]).unwrap()[0].value,
        ParameterValue::NotSet
    );
}

#[test]
fn snapshot_load_accepts_an_unchanged_read_only_parameter() {
    let store = ParameterStore::new("/restore", "restore");
    store
        .declare(
            "constant",
            ParameterValue::Integer(3),
            ParameterDescriptor {
                read_only: true,
                ..Default::default()
            },
            false,
        )
        .unwrap();
    let snapshot = store.snapshot().unwrap();
    store.prepare_load(&snapshot).unwrap().commit().unwrap();
    assert_eq!(store.get("constant").unwrap(), ParameterValue::Integer(3));
}

#[test]
fn whole_no_op_load_preserves_all_records_versions_and_empty_event() {
    let store = restore_store();
    let before = store.snapshot().unwrap();
    let transaction = store.prepare_load(&before).unwrap();
    assert_eq!(transaction.candidate().len(), 4);
    let commit = transaction.commit().unwrap();
    assert_eq!(commit.version, before.version);
    assert_eq!(commit.event.version, before.version);
    assert!(commit.event.new_parameters.is_empty());
    assert!(commit.event.changed_parameters.is_empty());
    assert!(commit.event.deleted_parameters.is_empty());
    assert!(!commit.event_published);
    assert!(commit.event_publish_error.is_none());
    assert_eq!(json!(store.snapshot().unwrap()), json!(before));

    let empty = store
        .prepare_load_values(Vec::new())
        .unwrap()
        .commit()
        .unwrap();
    assert_eq!(empty.version, before.version);
    assert!(!empty.event_published);
    assert!(matches!(
        store.prepare_set(Vec::new()),
        Err(ParameterError::InvalidArgument(_))
    ));
}

#[test]
fn mixed_load_changes_only_values_and_uses_the_targets_descriptor() {
    let store = restore_store();
    let before = store.snapshot().unwrap();
    let mut restored = before.clone();
    restored.node = "/different_node".into();
    restored.node_id = "old_instance".into();
    restored.version = u64::MAX;
    for record in &mut restored.parameters {
        record.version = u64::MAX;
        record.source = "untrusted snapshot source".into();
        record.descriptor = ParameterDescriptor::default();
        if record.name == "changed" {
            record.value = ParameterValue::Integer(4);
        }
    }
    let commit = store.prepare_load(&restored).unwrap().commit().unwrap();
    assert_eq!(commit.version, before.version + 1);
    assert_eq!(
        commit.event.changed_parameters,
        [Parameter::new("changed", ParameterValue::Integer(4))]
    );
    assert!(commit.event.new_parameters.is_empty());
    assert!(commit.event.deleted_parameters.is_empty());
    for record in store.snapshot().unwrap().parameters {
        let original = before
            .parameters
            .iter()
            .find(|old| old.name == record.name)
            .unwrap();
        if record.name == "changed" {
            assert_eq!(record.descriptor, original.descriptor);
            assert_eq!(record.source, "runtime");
            assert_eq!(record.version, before.version + 1);
        } else {
            assert_eq!(&record, original);
        }
    }
    restored
        .parameters
        .iter_mut()
        .find(|p| p.name == "changed")
        .unwrap()
        .value = ParameterValue::Integer(6);
    let before_rejection = json!(store.snapshot().unwrap());
    assert!(matches!(
        store.prepare_load(&restored),
        Err(ParameterError::OutOfRange(_))
    ));
    assert_eq!(json!(store.snapshot().unwrap()), before_rejection);
}

#[test]
fn load_failures_are_atomic_and_do_not_undeclare_or_overwrite_read_only_values() {
    let store = restore_store();
    for (name, value, code) in [
        ("constant", ParameterValue::Integer(4), "read_only"),
        ("missing", ParameterValue::NotSet, "not_declared"),
        ("changed", ParameterValue::NotSet, "conflict"),
    ] {
        let before = json!(store.snapshot().unwrap());
        let mut candidate = vec![Parameter::new("dynamic", ParameterValue::Integer(9))];
        candidate.push(Parameter::new(name, value));
        let error = match store.prepare_load_values(candidate) {
            Err(error) => error,
            Ok(_) => panic!("invalid load unexpectedly prepared"),
        };
        assert_eq!(error.code(), code);
        assert_eq!(json!(store.snapshot().unwrap()), before);
    }
    let mut repeated = unchanged(&store);
    repeated.push(repeated[0].clone());
    assert!(matches!(
        store.prepare_load_values(repeated),
        Err(ParameterError::InvalidArgument(_))
    ));
}

#[test]
fn no_op_load_rechecks_version_and_expiry_after_validation() {
    let store = restore_store();
    let candidate = store.prepare_load_values(unchanged(&store)).unwrap();
    store
        .set_atomically(vec![Parameter::new("changed", ParameterValue::Integer(2))])
        .unwrap();
    assert!(matches!(
        candidate.commit(),
        Err(ParameterError::Conflict(_))
    ));
    let expired = store
        .prepare_load_values_with_timeout(unchanged(&store), Duration::ZERO)
        .unwrap();
    assert!(matches!(expired.commit(), Err(ParameterError::Expired)));
}

#[test]
fn atomic_service_load_validates_every_candidate_and_preserves_default_set_semantics() {
    let store = restore_store();
    let candidate = unchanged(&store);
    let before = json!(store.snapshot().unwrap());
    let mut validations = 0;
    let response = store
        .handle_request(
            ParameterServiceKind::SetParametersAtomically,
            &json!({"parameters":candidate,"mode":"load"}),
            |parameters| {
                validations += 1;
                assert_eq!(parameters, candidate);
                Err(ParameterError::Validation(
                    "application rejected no-op".into(),
                ))
            },
        )
        .unwrap();
    assert_eq!(validations, 1);
    assert_eq!(response["result"]["successful"], false);
    assert_eq!(json!(store.snapshot().unwrap()), before);

    let response = store
        .handle_request(
            ParameterServiceKind::SetParametersAtomically,
            &json!({"parameters":candidate,"mode":"load"}),
            |parameters| {
                assert_eq!(parameters, candidate);
                store
                    .set_atomically(vec![Parameter::new("changed", ParameterValue::Integer(2))])?;
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(response["result"]["code"], "conflict");
    assert!(store
        .describe(&["dynamic".into(), "static".into(), "constant".into()])
        .is_ok());

    for mode in [None, Some("set")] {
        let store = restore_store();
        let mut request = json!({"parameters":[Parameter::new("dynamic", ParameterValue::NotSet)]});
        if let Some(mode) = mode {
            request["mode"] = json!(mode);
        }
        let reply = store
            .handle_request(
                ParameterServiceKind::SetParametersAtomically,
                &request,
                |_| Ok(()),
            )
            .unwrap();
        assert_eq!(reply["result"]["successful"], true);
        assert!(matches!(
            store.get("dynamic"),
            Err(ParameterError::NotDeclared(_))
        ));
        assert!(matches!(
            store.prepare_set(vec![Parameter::new("constant", ParameterValue::Integer(3))]),
            Err(ParameterError::ReadOnly(_))
        ));
    }
    assert!(store
        .handle_request(
            ParameterServiceKind::SetParameters,
            &json!({"parameters":[],"mode":"load"}),
            |_| Ok(())
        )
        .is_err());
}

#[test]
fn atomic_load_changes_only_the_atomic_service_hash() {
    for (kind, hash) in [
        (
            ParameterServiceKind::DescribeParameters,
            "7d480d9323c4ad24f0a7dd70f3301843d7f3836bcd68b9dbb6ce9b2b4dfa1018",
        ),
        (
            ParameterServiceKind::GetParameterTypes,
            "f2855c5f4b646dff56951b10376a7f141405ed9cbbdf62f65cd0b03a7d28f384",
        ),
        (
            ParameterServiceKind::GetParameters,
            "f09b98dd6f0bfe1c327c8b3782d21ea29d1a47a233e738301f5c582905f49f77",
        ),
        (
            ParameterServiceKind::ListParameters,
            "635123bc9a8c73f7a8e017b10905ae4014507391c7049b3f9bab86b493906d28",
        ),
        (
            ParameterServiceKind::SetParameters,
            "42c16c1fe14ef4306a9fe25bf2043e2b9c11a1c52bf0d41664e86502227cf9fe",
        ),
        (
            ParameterServiceKind::SetParametersAtomically,
            "b04ae3615764725db87d3af97c119f46c5fbe626c421c2fe4a7072c801d2a1a7",
        ),
    ] {
        assert_eq!(kind.interface().type_hash(), hash);
    }
    let mut old_atomic = ParameterServiceKind::SetParametersAtomically
        .interface()
        .definition()
        .clone();
    old_atomic["request"]["properties"]
        .as_object_mut()
        .unwrap()
        .remove("mode");
    assert_eq!(
        Interface::from_json(old_atomic).unwrap().type_hash(),
        "8abee1265429b75d5c959d2d8504738552476438c4502661bafbf714bf984d03"
    );
}
