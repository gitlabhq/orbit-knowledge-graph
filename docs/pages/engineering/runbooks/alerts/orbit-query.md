# Orbit query alerts

Covers `OrbitServiceOrbitQueryApdexSLOViolation`, `OrbitServiceOrbitQueryErrorSLOViolation`, `OrbitServiceOrbitQueryTrafficCessation`, `OrbitServiceOrbitQueryTrafficAbsent`, `OrbitQueryLatencyDegraded`.

Orbit queries fail or are slow, or the query metrics stopped. Users see errors or slow responses from the Orbit API.

## Check

On Dedicated, only the Dedicated EOC runs commands. Use `gkg-webserver` and no `--context`.

1. Run a real query. Healthy: `status` says `healthy` and the query returns one project in less than 5 seconds.

   ```shell
   GITLAB_HOST=gitlab.com glab orbit status
   GITLAB_HOST=gitlab.com glab orbit query 'MATCH (p:Project) WHERE p.id = 77960826 RETURN p.id, p.full_path LIMIT 1'
   ```

1. Check the pods. Healthy: all pods are `Running`, READY is full and RESTARTS does not increase.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods | grep orbit-webserver
   ```

1. Open the [webserver dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-webserver). Healthy: p95 is below 5 seconds and there are no server errors.
1. Look for a recent deploy. Healthy: the newest ReplicaSet is older than the alert.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | grep orbit-webserver | tail -1
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Started with a deploy | [Roll back](../maintenance.md#rollback). | The query in step 1 works. |
| `query` fails, `status` works, logs show `LazyLock instance has previously been poisoned` | [Roll back](../maintenance.md#rollback). | The query in step 1 works. |
| All query types slow, or errors with `TIMEOUT_EXCEEDED` | The graph ClickHouse is overloaded. High indexer writes: [stop one indexer pool](../README.md#emergency-actions). Else page Orbit tier-2. | p95 is below 5 seconds. |
| Errors with `status="timeout"` | The permission check in GitLab Rails is slow. Page the on-call of the GitLab API. | No `timeout` errors. |
| One query type slow | One caller sends heavy queries. Find it in the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter `json.mode: webserver`. Page Orbit tier-2. | p95 is below 5 seconds. |
| The first queries after a quiet time are slow, then fast | No action. The graph ClickHouse starts after idle time. | p95 is below 5 seconds. |
| Webserver pods show `OOMKilled` | Follow [OrbitContainerOOMKilled](OrbitContainerOOMKilled.md). | RESTARTS does not increase. |
| `TrafficAbsent`, but the query in step 1 works | Users are not affected. Post in `#f_orbit_dev`. | Orbit tier-2 has the issue. |

## Escalate

All queries fail: declare an incident ([first 5 minutes](../README.md#first-5-minutes)) and page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
