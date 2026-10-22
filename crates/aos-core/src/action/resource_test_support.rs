//! Deterministic transport fixture for ownership and request-retention tests.
//! No worker waits indefinitely and no protocol state is simulated by clients.
use crate::{transport::*, *};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{atomic::AtomicBool, Arc, Mutex},
    time::Duration,
};

type SendHook = dyn Fn(&str) -> Result<()> + Send + Sync;

#[derive(Default)]
pub(super) struct State {
    queues: Mutex<BTreeMap<String, VecDeque<Vec<u8>>>>,
    goal_requests: Mutex<VecDeque<Box<dyn RequestBackend>>>,
    pub send_hook: Mutex<Option<Arc<SendHook>>>,
    pub send_error: Mutex<Option<Error>>,
}
impl State {
    pub fn goal_request(&self, id: &str, goal: Value) -> Arc<ReplyCapture> {
        let (request, capture) = request(json!({"goal_id":id,"goal":goal}));
        self.goal_requests.lock().unwrap().push_back(request);
        capture
    }
    pub fn publish(&self, topic: &str, value: &Value) {
        if let Some(queue) = self.queues.lock().unwrap().get_mut(topic) {
            queue.push_back(serde_json::to_vec(value).unwrap());
        }
    }
    pub fn status(&self, items: &[(&str, u8)]) {
        self.publish(
            "/resources/_action/status",
            &json!({"status_list":items.iter().map(|(id,status)|
                json!({"goal_info":{"goal_id":id,"stamp":{"sec":1,"nanosec":0}},
                    "status":status})).collect::<Vec<_>>()}),
        );
    }
}
struct TestTransport(Arc<State>);
struct Publisher(Arc<State>, String);
struct Subscription(Arc<State>, String);
struct QuietService;
struct GoalService(Arc<State>);
struct Guard;
impl PublisherBackend for Publisher {
    fn publish(&self, payload: &[u8]) -> Result<()> {
        if let Some(queue) = self.0.queues.lock().unwrap().get_mut(&self.1) {
            queue.push_back(payload.to_vec());
        }
        Ok(())
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
impl SubscriptionBackend for Subscription {
    fn recv_timeout(&self, _: Duration) -> Result<Vec<u8>> {
        self.0
            .queues
            .lock()
            .unwrap()
            .get_mut(&self.1)
            .and_then(VecDeque::pop_front)
            .ok_or(Error::Timeout)
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
impl ServiceBackend for QuietService {
    fn recv_timeout(&self, _: Duration) -> Result<Box<dyn RequestBackend>> {
        Err(Error::Closed)
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
impl ServiceBackend for GoalService {
    fn recv_timeout(&self, _: Duration) -> Result<Box<dyn RequestBackend>> {
        self.0
            .goal_requests
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(Error::Timeout)
    }
    fn close(&self) -> Result<()> {
        self.0.goal_requests.lock().unwrap().clear();
        Ok(())
    }
}
impl GraphGuard for Guard {
    fn close(&self) -> Result<()> {
        Ok(())
    }
}
impl Transport for TestTransport {
    fn create_publisher(&self, topic: &str) -> Result<Box<dyn PublisherBackend>> {
        Ok(Box::new(Publisher(self.0.clone(), topic.into())))
    }
    fn create_subscription(&self, topic: &str) -> Result<Box<dyn SubscriptionBackend>> {
        self.0
            .queues
            .lock()
            .unwrap()
            .entry(topic.into())
            .or_default();
        Ok(Box::new(Subscription(self.0.clone(), topic.into())))
    }
    fn create_typed_publisher(
        &self,
        endpoint: &Endpoint,
        _: &QoS,
    ) -> Result<Box<dyn PublisherBackend>> {
        self.create_publisher(&endpoint.name)
    }
    fn create_typed_subscription(
        &self,
        endpoint: &Endpoint,
        _: &QoS,
    ) -> Result<Box<dyn SubscriptionBackend>> {
        self.create_subscription(&endpoint.name)
    }
    fn create_service(&self, endpoint: &Endpoint) -> Result<Box<dyn ServiceBackend>> {
        if endpoint.name.ends_with("/send_goal") {
            Ok(Box::new(GoalService(self.0.clone())))
        } else {
            Ok(Box::new(QuietService))
        }
    }
    fn call_cancelable(
        &self,
        endpoint: &Endpoint,
        payload: &[u8],
        _: Duration,
        _: &AtomicBool,
    ) -> Result<Vec<u8>> {
        if endpoint.name.ends_with("/send_goal") {
            let request: Value = serde_json::from_slice(payload).unwrap();
            let hook = self.0.send_hook.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook(request["goal_id"].as_str().unwrap())?;
            }
            if let Some(error) = self.0.send_error.lock().unwrap().clone() {
                return Err(error);
            }
            Ok(
                serde_json::to_vec(&json!({"accepted":true,"stamp":{"sec":1,"nanosec":0}}))
                    .unwrap(),
            )
        } else if endpoint.name.ends_with("/get_result") {
            Ok(serde_json::to_vec(&json!({"status":4,"result":{}})).unwrap())
        } else {
            Err(Error::Unsupported("unused fixture call".into()))
        }
    }
    fn declare_graph_entity(&self, _: &GraphEntity) -> Result<Box<dyn GraphGuard>> {
        Ok(Box::new(Guard))
    }
    fn close(&self) -> Result<()> {
        Ok(())
    }
}

pub(super) fn fixture() -> (Arc<State>, Context, Node, Interface) {
    let state = Arc::new(State::default());
    let context = Context::new(TestTransport(state.clone()));
    let node = Node::new(context.clone(), "resources", "/", NodeOptions::default()).unwrap();
    let interface = Interface::from_json(json!({
        "name":"test/action/Resources","kind":"action",
        "goal":{"type":"object","properties":{},"additionalProperties":false},
        "result":{"type":"object","properties":{},"additionalProperties":false,"default":{}},
        "feedback":{"type":"object","properties":{},"additionalProperties":false}
    }))
    .unwrap();
    (state, context, node, interface)
}

#[derive(Default)]
pub(super) struct ReplyCapture {
    pub replies: Mutex<Vec<Result<Vec<u8>>>>,
    pub failure: Mutex<Option<Error>>,
    pub reply_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl ReplyCapture {
    pub fn result(&self) -> Option<Result<Value>> {
        self.replies.lock().unwrap().first().map(|reply| {
            reply
                .clone()
                .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        })
    }
}
struct Request {
    payload: Vec<u8>,
    capture: Arc<ReplyCapture>,
}
impl RequestBackend for Request {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
    fn reply(&self, reply: Result<Vec<u8>>) -> Result<()> {
        self.capture.replies.lock().unwrap().push(reply);
        let hook = self.capture.reply_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        match self.capture.failure.lock().unwrap().clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
pub(super) fn request(value: Value) -> (Box<dyn RequestBackend>, Arc<ReplyCapture>) {
    let capture = Arc::new(ReplyCapture::default());
    (
        Box::new(Request {
            payload: serde_json::to_vec(&value).unwrap(),
            capture: capture.clone(),
        }),
        capture,
    )
}
