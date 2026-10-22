// Portions of the liveliness token layout are adapted from Hiroz
// d9f979a0404311de12f6884bafaa9b20600eb490 (Apache-2.0 selected).
// Modified for AOS JSON interfaces, ownership and closure. See upstream/source-map.json
// and upstream/licenses/hiroz-LICENSE for source locations and license.

//! Liveliness-backed discovery, with no separate registry process.
//!
//! The token layout and slash mangling follow Hiroz d9f979a0404311de12f6884bafaa9b20600eb490
//! `hiroz-protocol/src/format/rmw_zenoh.rs`. AOS replaces the admin prefix and
//! carries JSON interface identities; action kinds extend the endpoint tags.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use aos_core::transport::{
    Durability, Endpoint, EntityKind, GraphEntity, GraphGuard, GraphWatchBackend, QoS, Reliability,
};
use aos_core::{Error, Result, TelemetryComponent, TelemetryResource};
use zenoh::liveliness::LivelinessToken;
use zenoh::pubsub::Subscriber;
use zenoh::sample::SampleKind;
use zenoh::Wait;

use crate::key::typed_key;
use crate::queue::{FinishOnDrop, Queue};
use crate::session::{failure, Shared};

const PREFIX: &str = "aos/@lv";

pub(crate) struct ZenohGraphGuard {
    token: Mutex<Option<LivelinessToken>>,
    shared: Arc<Shared>,
    resource: Arc<TelemetryResource>,
}

impl ZenohGraphGuard {
    pub(crate) fn declare(
        session: &zenoh::Session,
        domain: u32,
        entity: &GraphEntity,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        if shared.is_closed() {
            return Err(Error::Closed);
        }
        let key = graph_key(domain, &session.zid().to_string(), entity)?;
        let resource = shared.resource(TelemetryComponent::Graph)?;
        let token = session
            .liveliness()
            .declare_token(key)
            .wait()
            .map_err(|error| failure(&shared, "declare graph token", error))?;
        Ok(Self {
            token: Mutex::new(Some(token)),
            shared,
            resource,
        })
    }
}

impl GraphGuard for ZenohGraphGuard {
    fn close(&self) -> Result<()> {
        self.resource.close();
        let token = self
            .token
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or(Error::Closed)?;
        if self.shared.is_closed() {
            return Err(Error::Closed);
        }
        token
            .undeclare()
            .wait()
            .map_err(|error| failure(&self.shared, "undeclare graph token", error))
    }
}

pub(crate) struct ZenohGraphWatch {
    // Drop unregisters the subscriber before releasing its callback state.
    _subscriber: Subscriber<()>,
    entities: Arc<Mutex<BTreeMap<String, GraphEntity>>>,
    changes: Arc<Queue<()>>,
    shared: Arc<Shared>,
    _resource: Arc<TelemetryResource>,
}

impl ZenohGraphWatch {
    pub(crate) fn declare(
        session: &zenoh::Session,
        domain: u32,
        shared: Arc<Shared>,
    ) -> Result<Self> {
        let changes = Arc::new(Queue::new(1));
        shared.register(&changes)?;
        let resource = shared.resource(TelemetryComponent::Graph)?;
        let entities = Arc::new(Mutex::new(BTreeMap::new()));
        let current = entities.clone();
        let changed = changes.clone();
        // Zenoh 1.10.1 history(true) uses a CurrentFuture liveliness interest
        // (api/session.rs, declare_liveliness_subscriber_inner), not a GET with
        // a query timeout. Subscriber Drop undeclares that interest.
        // This callback must only update state and wake the receive queue.
        // Do not synchronously wait for a liveliness query/Declare result on
        // any Session: nested/cross-thread callbacks can form lane wait cycles.
        let subscriber = session
            .liveliness()
            .declare_subscriber(format!("{PREFIX}/{domain}/**"))
            .history(true)
            .callback(move |sample| {
                let key = sample.key_expr().as_str();
                let Some(entity) = parse_graph_key(domain, key) else {
                    return;
                };
                let mut current = current.lock().unwrap_or_else(PoisonError::into_inner);
                match sample.kind() {
                    SampleKind::Put => {
                        current.insert(key.to_owned(), entity);
                    }
                    SampleKind::Delete => {
                        current.remove(key);
                    }
                }
                drop(current);
                // Only a wakeup is coalesced; the map retains the latest state.
                changed.push(());
            })
            .wait()
            .map_err(|error| {
                changes.close();
                failure(&shared, "watch graph", error)
            })?;
        let watch = Self {
            _subscriber: subscriber,
            entities,
            changes,
            shared,
            _resource: resource,
        };
        watch.ensure_open()?;
        Ok(watch)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.shared.is_closed() || self.changes.is_closed() {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
}

impl GraphWatchBackend for ZenohGraphWatch {
    fn snapshot(&self) -> Result<Vec<GraphEntity>> {
        self.ensure_open()?;
        let mut entities: Vec<_> = self
            .entities
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        self.ensure_open()?;
        entities.sort_by(|a, b| a.id.cmp(&b.id));
        entities.dedup_by(|a, b| a.id == b.id);
        Ok(entities)
    }

    fn wait_for_change(&self, timeout: Duration) -> Result<()> {
        self.ensure_open()?;
        let result = self.changes.recv_timeout(timeout);
        self.ensure_open()?;
        result
    }
}

impl Drop for ZenohGraphWatch {
    fn drop(&mut self) {
        self.changes.close();
    }
}

pub(crate) fn snapshot(
    session: &zenoh::Session,
    domain: u32,
    shared: &Arc<Shared>,
    timeout: Duration,
) -> Result<Vec<GraphEntity>> {
    let queue = Arc::new(Queue::new(usize::MAX));
    shared.register(&queue)?;
    let sink = FinishOnDrop(queue.clone());
    let started = Instant::now();
    session
        .liveliness()
        .get(format!("{PREFIX}/{domain}/**"))
        .timeout(timeout)
        .callback(move |reply| match reply.into_result() {
            Ok(sample) => {
                if let Some(entity) = parse_graph_key(domain, sample.key_expr().as_str()) {
                    sink.push(Ok(entity));
                }
            }
            Err(error) if crate::service::is_query_timeout(&error) => {}
            Err(error) => sink.push(Err(Error::Transport(format!("graph query: {error:?}")))),
        })
        .wait()
        .map_err(|error| failure(shared, "query graph", error))?;
    let mut entities = Vec::new();
    loop {
        match queue.recv_timeout(timeout.saturating_sub(started.elapsed())) {
            Ok(entity) => entities.push(entity?),
            Err(Error::Timeout) => break,
            Err(error) => return Err(error),
        }
    }
    queue.close();
    entities.sort_by(|a, b| a.id.cmp(&b.id));
    entities.dedup_by(|a, b| a.id == b.id);
    Ok(entities)
}

fn mangle(value: &str) -> String {
    value.replace('/', "%")
}
fn demangle(value: &str) -> String {
    value.replace('%', "/")
}

fn graph_key(domain: u32, zid: &str, entity: &GraphEntity) -> Result<String> {
    for value in [&entity.id, &entity.node_id, &entity.node_name] {
        if value.is_empty()
            || !value
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        {
            return Err(Error::InvalidArgument(
                "graph identity must be a literal name".into(),
            ));
        }
    }
    if !entity.namespace.starts_with('/')
        || !entity
            .namespace
            .split('/')
            .all(|part| part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return Err(Error::InvalidArgument(
            "graph namespace must be absolute".into(),
        ));
    }
    let kind = match entity.kind {
        EntityKind::Node => "NN",
        EntityKind::Publisher => "MP",
        EntityKind::Subscription => "MS",
        EntityKind::Client => "SC",
        EntityKind::Service => "SS",
        EntityKind::ActionClient => "AC",
        EntityKind::ActionServer => "AS",
    };
    let mut key = format!(
        "{PREFIX}/{domain}/{zid}/{}/{}/{kind}/%/{}/{}",
        entity.node_id,
        entity.id,
        mangle(&entity.namespace),
        entity.node_name
    );
    if let Some(endpoint) = &entity.endpoint {
        typed_key(domain, endpoint)?;
        let qos = match &entity.qos {
            Some(qos) => {
                qos.validate()?;
                encode_qos(qos)
            }
            None => "%".to_string(),
        };
        key.push_str(&format!(
            "/{}/{}/{}/{qos}",
            mangle(&endpoint.name),
            mangle(&endpoint.type_name),
            endpoint.type_hash
        ));
    }
    Ok(key)
}

fn parse_graph_key(domain: u32, key: &str) -> Option<GraphEntity> {
    let chunks: Vec<_> = key.split('/').collect();
    if !matches!(chunks.len(), 10 | 14)
        || chunks[0] != "aos"
        || chunks[1] != "@lv"
        || chunks[2].parse::<u32>().ok()? != domain
    {
        return None;
    }
    let kind = match chunks[6] {
        "NN" => EntityKind::Node,
        "MP" => EntityKind::Publisher,
        "MS" => EntityKind::Subscription,
        "SC" => EntityKind::Client,
        "SS" => EntityKind::Service,
        "AC" => EntityKind::ActionClient,
        "AS" => EntityKind::ActionServer,
        _ => return None,
    };
    let (endpoint, qos) = if chunks.len() == 14 {
        (
            Some(Endpoint {
                name: demangle(chunks[10]),
                type_name: demangle(chunks[11]),
                type_hash: chunks[12].into(),
            }),
            if chunks[13] == "%" {
                None
            } else {
                // Keep the entity visible when a newer/foreign publisher uses
                // an unsupported profile. None means unknown, never default.
                decode_qos(chunks[13])
            },
        )
    } else {
        (None, None)
    };
    Some(GraphEntity {
        id: chunks[5].into(),
        node_id: chunks[4].into(),
        node_name: chunks[9].into(),
        namespace: demangle(chunks[8]),
        kind,
        endpoint,
        qos,
    })
}

// Hiroz/RMW compact QoS layout. Unsupported duration policies retain their
// omitted/default fields; AOS only advertises the supported keep_last profile.
fn encode_qos(qos: &QoS) -> String {
    let reliability = if qos.reliability == Reliability::Reliable {
        1
    } else {
        2
    };
    let durability = if qos.durability == Durability::TransientLocal {
        1
    } else {
        2
    };
    format!("{reliability}:{durability}:1,{}:,:,:,,", qos.depth)
}
fn decode_qos(value: &str) -> Option<QoS> {
    let parts: Vec<_> = value.split(':').collect();
    if parts.len() != 6 || parts[3..] != [",", ",", ",,"] {
        return None;
    }
    let reliability = match parts[0] {
        "1" => Reliability::Reliable,
        "2" => Reliability::BestEffort,
        _ => return None,
    };
    let durability = match parts[1] {
        "1" => Durability::TransientLocal,
        "2" => Durability::Volatile,
        _ => return None,
    };
    let depth = parts[2].strip_prefix("1,")?.parse().ok()?;
    let qos = QoS {
        depth,
        reliability,
        durability,
    };
    qos.validate().ok()?;
    Some(qos)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publisher() -> GraphEntity {
        GraphEntity {
            id: "e1".into(),
            node_id: "n1".into(),
            node_name: "talker".into(),
            namespace: "/demo".into(),
            kind: EntityKind::Publisher,
            endpoint: Some(Endpoint {
                name: "/demo/chatter".into(),
                type_name: "demo/msg/String".into(),
                type_hash: "a".repeat(64),
            }),
            qos: Some(QoS::default()),
        }
    }

    #[test]
    fn token_preserves_identity_names_types_and_qos() {
        let entity = publisher();
        let key = graph_key(42, "z1", &entity).unwrap();
        assert_eq!(key, format!("aos/@lv/42/z1/n1/e1/MP/%/%demo/talker/%demo%chatter/demo%msg%String/{}/1:2:1,10:,:,:,,", "a".repeat(64)));
        assert_eq!(parse_graph_key(42, &key), Some(entity));
        assert_eq!(parse_graph_key(0, &key), None);
    }

    #[test]
    fn token_preserves_every_supported_profile_field() {
        for reliability in [Reliability::Reliable, Reliability::BestEffort] {
            for durability in [Durability::Volatile, Durability::TransientLocal] {
                let mut entity = publisher();
                entity.qos = Some(QoS {
                    depth: 17,
                    reliability,
                    durability,
                });
                let key = graph_key(42, "z1", &entity).unwrap();
                assert_eq!(parse_graph_key(42, &key), Some(entity));
            }
        }
    }

    #[test]
    fn unsupported_remote_policies_keep_entity_visible_with_unknown_qos() {
        let mut entity = publisher();
        let key = graph_key(42, "z1", &entity).unwrap();
        let prefix = key.rsplit_once('/').unwrap().0;
        entity.qos = None;
        for profile in [
            "%",
            "99:2:1,10:,:,:,,",      // Unknown reliability.
            "1:99:1,10:,:,:,,",      // Unknown durability.
            "1:2:2,10:,:,:,,",       // Unsupported history.
            "1:2:1,0:,:,:,,",        // Invalid depth.
            "1:2:1,10:123,0:,:,,",   // Deadline must not be silently ignored.
            "1:2:1,10:,:123,0:,,",   // Lifespan must not be silently ignored.
            "1:2:1,10:,:,:1,123,0",  // Liveliness/lease must not be ignored.
            "future-profile-format", // New format remains unknown.
        ] {
            assert_eq!(
                parse_graph_key(42, &format!("{prefix}/{profile}")),
                Some(entity.clone()),
                "{profile}"
            );
        }
    }
}
