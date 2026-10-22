//! Test-only parsing of the fixed Zenoh 1.10.1 dispatcher trace.
//! A selected route is not a delivery receipt; the caller must separately
//! require Router interception for each selected route.
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct PlaybackRoute {
    pub to_router: bool,
    pub evidence: String,
}

pub fn playback_routes(text: &str, key: &str) -> Result<Vec<PlaybackRoute>, String> {
    let mut pending = BTreeMap::<&str, &str>::new();
    let mut result = Vec::new();
    for line in text.lines() {
        let Some((prefix, rest)) = line.split_once("ThreadId(") else {
            continue;
        };
        if prefix.split_whitespace().last() != Some("aos-bag-playback") {
            continue;
        }
        let Some((thread, rest)) = rest.split_once(')') else {
            continue;
        };
        let Some(message) = rest.strip_prefix(" zenoh::net::routing::dispatcher::pubsub: ") else {
            continue;
        };
        if let Some((origin, current_key)) = message.split_once(" Route data for res ") {
            if pending.remove(thread).is_some() {
                return Err("playback route start has no corresponding route result".into());
            }
            if origin.starts_with("local/") && current_key == key {
                pending.insert(thread, line);
            }
        } else if let Some(route) = message.strip_prefix("route=") {
            let Some(start) = pending.remove(thread) else {
                continue;
            };
            let to_router = if route == "[]" {
                false
            } else if route.starts_with("[Direction { ")
                && route.ends_with(" }]")
                && route.matches("Direction { ").count() == 1
                && route.contains("region: North }")
            {
                true
            } else {
                return Err(format!("unexpected playback route: {route}"));
            };
            result.push(PlaybackRoute {
                to_router,
                evidence: format!("{start}\n{line}"),
            });
        }
    }
    if !pending.is_empty() {
        return Err("playback route trace ended before its route result".into());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "aos/29/replay/denied/type/hash";
    const NORTH: &str =
        "[Direction { dst_face: FaceState { id: 1, zid: router, region: North }, wire_expr: WireExpr { scope: 1, suffix: \"\", mapping: Sender }, node_id: 0 }]";

    fn line(worker: &str, thread: u32, message: &str) -> String {
        format!("TRACE {worker} ThreadId({thread}) zenoh::net::routing::dispatcher::pubsub: {message}\n")
    }

    fn start(worker: &str, thread: u32, key: &str) -> String {
        line(
            worker,
            thread,
            &format!("local/client:0 Route data for res {key}"),
        )
    }

    #[test]
    fn accounts_for_empty_and_north_routes_despite_interleaved_threads() {
        let text = start("aos-bag-playback", 1, KEY)
            + &start("aos-bag-playback", 2, KEY)
            + &line("aos-bag-playback", 2, "route=[]")
            + "DEBUG aos-bag-playback ThreadId(1) compute_data_route: return=()\n"
            + &line("aos-bag-playback", 1, &format!("route={NORTH}"));
        let routes = playback_routes(&text, KEY).unwrap();
        assert_eq!(routes.len(), 2);
        assert!(!routes[0].to_router);
        assert!(routes[1].to_router);
    }

    #[test]
    fn ignores_other_workers_other_keys_and_unpaired_results() {
        let text = start("authorized-control", 1, KEY)
            + &line("authorized-control", 1, &format!("route={NORTH}"))
            + &start("aos-bag-playback", 2, &format!("{KEY}/other"))
            + &line("aos-bag-playback", 2, "route=[]")
            + &line("aos-bag-playback", 3, "route=[]");
        assert!(playback_routes(&text, KEY).unwrap().is_empty());
    }

    #[test]
    fn rejects_missing_cross_thread_and_superseded_route_results() {
        let start = start("aos-bag-playback", 1, KEY);
        for text in [
            start.clone(),
            start.clone() + &line("aos-bag-playback", 2, "route=[]"),
            start.clone() + &start,
        ] {
            assert!(playback_routes(&text, KEY).is_err());
        }
    }

    #[test]
    fn rejects_unknown_local_and_multiple_routes() {
        for route in [
            "?".into(),
            NORTH.replace("North", "Local"),
            format!(
                "[{}, {}]",
                &NORTH[1..NORTH.len() - 1],
                &NORTH[1..NORTH.len() - 1]
            ),
        ] {
            let text = start("aos-bag-playback", 1, KEY)
                + &line("aos-bag-playback", 1, &format!("route={route}"));
            assert!(playback_routes(&text, KEY).is_err());
        }
    }
}
