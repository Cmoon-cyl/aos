use aos_core::{
    transport::{PublisherBackend, SubscriptionBackend, Transport},
    Context, Endpoint, Error, Result,
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

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
    fn call(&self, endpoint: &Endpoint, payload: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        self.0.call(endpoint, payload, timeout)
    }
    fn call_bounded(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        timeout: Duration,
        _: usize,
    ) -> Result<Vec<u8>> {
        self.call(endpoint, payload, timeout)
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn default_node_card_gate_rejects_two_mib_before_json_decode_for_legacy_and_custom_adapters() {
    // Invalid JSON proves rejection happened before decoding, including when
    // an override ignores call_bounded. These are post-Vec core gates.
    let reply = LegacyReply(Arc::new(vec![b'!'; 2 * 1024 * 1024]));
    let contexts = [
        Context::new(reply.clone()),
        Context::new(IgnoresLimit(reply)),
    ];
    for context in contexts {
        let error = context
            .get_node_card("/source", Duration::from_secs(1))
            .unwrap_err();
        assert!(matches!(error, Error::QueueOverflow(_)), "{error}");
    }
}

struct PrecopyReply {
    bytes: usize,
    received_limit: Arc<AtomicUsize>,
    copies: Arc<AtomicUsize>,
}
impl Transport for PrecopyReply {
    fn create_publisher(&self, _: &str) -> Result<Box<dyn PublisherBackend>> {
        Err(Error::Unsupported("fixture".into()))
    }
    fn create_subscription(&self, _: &str) -> Result<Box<dyn SubscriptionBackend>> {
        Err(Error::Unsupported("fixture".into()))
    }
    fn call_bounded(&self, _: &Endpoint, _: &[u8], _: Duration, limit: usize) -> Result<Vec<u8>> {
        self.received_limit.store(limit, Ordering::Release);
        if self.bytes > limit {
            return Err(Error::QueueOverflow(
                "adapter rejected before copying".into(),
            ));
        }
        self.copies.fetch_add(1, Ordering::AcqRel);
        Ok(vec![b' '; self.bytes])
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn node_card_limit_reaches_adapter_before_copy_and_can_only_be_tightened() {
    let received_limit = Arc::new(AtomicUsize::new(0));
    let copies = Arc::new(AtomicUsize::new(0));
    let context = Context::new(PrecopyReply {
        bytes: 2 * 1024 * 1024,
        received_limit: received_limit.clone(),
        copies: copies.clone(),
    });
    assert!(matches!(
        context.get_node_card("/source", Duration::from_secs(1)),
        Err(Error::QueueOverflow(_))
    ));
    assert_eq!(received_limit.load(Ordering::Acquire), 1024 * 1024);
    assert_eq!(copies.load(Ordering::Acquire), 0);
    for limit in [0, Context::MAX_NODE_CARD_RESPONSE_BYTES + 1] {
        assert!(matches!(
            context.get_node_card_bounded("/source", Duration::from_secs(1), limit),
            Err(Error::InvalidArgument(_))
        ));
        assert_eq!(received_limit.load(Ordering::Acquire), 1024 * 1024);
    }
    assert!(matches!(
        context.get_node_card_bounded("/source", Duration::from_secs(1), 64),
        Err(Error::QueueOverflow(_))
    ));
    assert_eq!(received_limit.load(Ordering::Acquire), 64);
    assert_eq!(copies.load(Ordering::Acquire), 0);
    let card = json!({"tags":["ordinary_provider"]});
    let encoded = serde_json::to_vec(&card).unwrap();
    let context = Context::new(LegacyReply(Arc::new(encoded.clone())));
    assert_eq!(
        context
            .get_node_card_bounded("/source", Duration::from_secs(1), encoded.len())
            .unwrap(),
        card
    );
    assert!(matches!(
        context.get_node_card_bounded("/source", Duration::from_secs(1), encoded.len() - 1),
        Err(Error::QueueOverflow(_))
    ));
}
