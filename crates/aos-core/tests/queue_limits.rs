use aos_core::{transport::*, Error, QueueLimits, Result};

struct Legacy;
impl Transport for Legacy {
    fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
        unreachable!()
    }
    fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
        unreachable!()
    }
    fn create_typed_subscription(
        &self,
        _: &Endpoint,
        _: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        panic!("an explicit byte budget must not fall back to the old declaration")
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn byte_limits_validate_extremes_without_overflow() {
    assert_eq!(QueueLimits::default().message_bytes, 16 * 1024 * 1024);
    assert_eq!(QueueLimits::default().queue_bytes, 256 * 1024 * 1024);
    for limits in [
        QueueLimits {
            message_bytes: 0,
            queue_bytes: 1,
        },
        QueueLimits {
            message_bytes: 2,
            queue_bytes: 1,
        },
        QueueLimits {
            message_bytes: 1,
            queue_bytes: QueueLimits::MAX_QUEUE_BYTES + 1,
        },
        QueueLimits {
            message_bytes: usize::MAX,
            queue_bytes: usize::MAX,
        },
    ] {
        assert!(matches!(limits.validate(), Err(Error::InvalidArgument(_))));
    }
    QueueLimits {
        message_bytes: QueueLimits::MAX_QUEUE_BYTES,
        queue_bytes: QueueLimits::MAX_QUEUE_BYTES,
    }
    .validate()
    .unwrap();
}

#[test]
fn old_queue_observations_decode_as_unknown_bytes_and_custom_bounded_declarations_fail() {
    let stats: QueueStats = serde_json::from_str(
        r#"{"depth":10,"queued":2,"received_total":3,"dropped_total":1,"closed":false}"#,
    )
    .unwrap();
    assert_eq!(stats.bytes, None);
    let endpoint = Endpoint {
        name: "/bounded".into(),
        type_name: "test".into(),
        type_hash: "hash".into(),
    };
    assert!(matches!(
        Legacy.create_typed_subscription_with_limits(
            &endpoint,
            &QoS::default(),
            QueueLimits::default()
        ),
        Err(Error::Unsupported(_))
    ));
}
