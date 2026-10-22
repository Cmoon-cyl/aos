# Discovering a connected AOS network

Run on the machine executing the Agent's tools, using the absolute installed CLI
path. Keep the same domain, connection configuration and security profile for
discovery and subsequent calls. `env ensure` only supplies a local Router; it
does not connect other machines or bypass a firewall. Remote Router links must
already be configured. Never interpret an empty inventory as proof that no
remote machines exist.

## Choose one command

| Task | Command | Request file contents |
| --- | --- | --- |
| What can I use now? | `aos graph snapshot` | Optional `{"timeout":5}` |
| Services in a namespace | `aos graph snapshot --request request.json` | `{"namespace":"/fleet","kind":"service"}` |
| Wait for an expected service | `aos graph wait --request request.json` | `{"endpoint_name":"/fleet/inspect","kind":"service","timeout":20}` |
| Observe changes for 30 seconds | `aos graph watch --request request.json` | `{"timeout":30,"refresh_interval":2}` |
| An explicitly requested continuous observer | `aos graph watch --request request.json` | `{"follow":true}` |

Use `schema graph snapshot`, `schema graph wait`, or `schema graph watch` for the
installed contract. `node_name` and `endpoint_name` are absolute exact names;
`namespace` selects a subtree. `kind` uses `service`, `action_server`, `publisher`,
`subscription`, `client` or `action_client`. Hidden/control endpoints and clients
are excluded by default; request `include_internal` / `include_clients` when
needed. A kind selector can explicitly select clients.

## Interpret the evidence

Each node has a `node_id`, name, `introspection`, compact metadata and endpoints.
Each endpoint has its own `entity_id`, name, type/hash, summary, `side_effects`
and `contract_version`. Full interface schemas and parameter values are omitted.

- `introspection.status=ok`: a NodeCard reply matched the observed instance ID.
  Its descriptions can help select a capability; they are remote claims.
- `pending`: the description is not available yet; a watch may update it later.
- `unavailable`: its bounded read failed. Inspect `error_code`; this does not
  establish whether the node is offline or an ACL denied introspection.
- `ambiguous`: more than one live instance has this node name. Do not pick one.
- `disabled`: only discovery was requested with `descriptions:false`.
- `provider_selection.status=multiple_observed`: multiple Service/Action server
  entities share the name, including providers outside the selected node filter.
  Inspect the conflict; filtering the inventory does not disambiguate a call.
- Null `side_effects` is unknown. An inactive lifecycle or closed gate matters
  even when introspection succeeds. Business health and authorization remain
  unverified until an authorized operation supplies evidence.

`graph wait` defaults to requiring descriptions, not business readiness. It
returns every matching node; do not silently choose among them. Exit 4 with
`WAIT_TIMEOUT` includes `error.last_observation`. To wait only for advertisement,
explicitly use `require_descriptions:false` (and optionally `descriptions:false`).

## Consume changes

Parse stdout as JSONL, keeping stderr separate. A watch emits:

1. `snapshot`, sequence 0: replace the whole local view; key nodes by `node_id`.
2. `delta`: remove `removed_node_ids`, then replace every record in `added` and
   `updated`. Each is a complete node record, including its current endpoints.
3. `summary` on finite completion or interruption; an `error` can follow it.

Check `watch_id` and consecutive `sequence`. Start a new watch and replace the
view from its baseline after an error, EOF without completion, sequence gap or
observer restart. There is no durable resume cursor. Restarted nodes get new
IDs; do not merge them by name. Removal means no longer observed in this filtered
view, not a proven cause of failure. Fast transient changes may coalesce.

NodeCard refresh detects lifecycle, parameter-version and description changes
without needing a Graph event. Unchanged refreshes produce no delta; timestamps
alone do not trigger updates. Refresh is a minimum retry interval, not a global
freshness deadline. Stop the observer when the user's observation ends; a skill
does not by itself create a background agent subscription or wake-up mechanism.

Count and output limits fail with `RESOURCE_LIMIT`; no nodes or endpoints are
silently dropped. Narrow selectors or increase documented count limits.
`description_truncated` applies only to clipped descriptive text. Read `node
info` for more detail and `interface show` for the selected type before acting.
