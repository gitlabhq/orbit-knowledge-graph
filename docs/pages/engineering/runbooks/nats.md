# NATS

Orbit uses NATS JetStream for indexing work, dead letters and indexing locks. NATS runs in
namespace `nats`. Use the `nats-box` deployment for all commands. Its default context has the
mTLS client certificates.

## Run commands

Open a shell. For staging, use context `gke_gl-orbit-stg_us-east1_orbit-stg`:

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec -it deploy/nats-box -- sh
```

Or run one command:

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats stream ls
```

## Find the current names

The stream and bucket names change with each release and schema version. `<release>` is the
image tag with dashes, for example `0-159-0` for `0.159.0`. `<schema>` is the schema version.

| Command | Shows |
| --- | --- |
| `nats stream ls` | `GKG_INDEXER_V<release>` and `GKG_DEAD_LETTERS_V<release>` |
| `nats kv ls` | `indexing_locks_v<schema>` |

Use the streams whose release is the running image tag. Old streams and buckets stay in
production and do not affect indexing: ignore them. The dispatcher removes the streams of an
old release about one hour after that release stops. Streams and buckets without a release
or schema suffix, and the `_V79`, `_V80`, `_v72` and `_v94` ones, never go away by
themselves.

## Check stream and consumer health

```shell
nats stream info GKG_INDEXER_V<release>
nats consumer ls GKG_INDEXER_V<release>
```

Each indexing subject has one consumer. For more detail, run
`nats consumer info GKG_INDEXER_V<release> <consumer>`. Healthy:

- `Unprocessed Messages` stays near 0. A value that grows means the indexers do not keep up.
- `Outstanding Acks` (`Ack Pending` in the list) goes up and down. A value that stays at the
  maximum means that handlers are stuck.
- `Redelivered Messages` stays low. A value that grows means handlers fail or time out.

## Dead letters

For dead-lettered code indexing messages, see [code indexing](code_indexing.md).

> [!warning]
> Do not delete or purge streams or KV buckets, except as a step of
> [rebuild and reindex](manual_rebuild.md). The Siphon streams are on the same NATS server.

## Dedicated

NATS runs in namespace `nats` of the tenant cluster, without TLS. The Dedicated EOC runs
the commands.
