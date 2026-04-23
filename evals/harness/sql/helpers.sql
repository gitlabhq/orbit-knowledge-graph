-- Reusable macros for querying eval state.
-- Loaded after ddl.sql via ensure_schema().

-- Scalar: total cost for a run
CREATE OR REPLACE MACRO run_cost(rid) AS (
    SELECT coalesce(sum(cost), 0) FROM task_results WHERE run_id = rid
);

-- Scalar: success rate for a run+arm
CREATE OR REPLACE MACRO success_rate(rid, a) AS (
    SELECT count(*) FILTER (WHERE status = 'success')::DOUBLE
         / nullif(count(*), 0)
    FROM task_results
    WHERE run_id = rid AND arm = a
);

-- Table: per-arm summary for a run
CREATE OR REPLACE MACRO arm_summary(rid) AS TABLE (
    SELECT
        arm,
        count(*) AS total,
        count(*) FILTER (WHERE status = 'success') AS successes,
        count(*) FILTER (WHERE status = 'timeout') AS timeouts,
        count(*) FILTER (WHERE status = 'agent_error') AS agent_errors,
        count(*) FILTER (WHERE status = 'infra_error') AS infra_errors,
        round(sum(cost), 4) AS total_cost,
        round(avg(cost), 4) AS avg_cost,
        round(avg(duration_ms), 0)::INTEGER AS avg_duration_ms,
        round(avg(steps), 1) AS avg_steps,
        round(avg(tool_calls), 1) AS avg_tool_calls
    FROM task_results
    WHERE run_id = rid
    GROUP BY arm
    ORDER BY arm
);

-- Table: all runs with their config hash and summary stats
CREATE OR REPLACE MACRO run_overview() AS TABLE (
    SELECT
        r.run_id,
        r.started_at,
        r.completed_at,
        r.status,
        r.task_count,
        rc.config_name,
        rc.config_version,
        rc.config_hash,
        round(coalesce((SELECT sum(cost) FROM task_results t WHERE t.run_id = r.run_id), 0), 4) AS total_cost,
        (SELECT count(*) FILTER (WHERE status = 'success') FROM task_results t WHERE t.run_id = r.run_id) AS successes
    FROM runs r
    LEFT JOIN run_configs rc ON r.run_id = rc.run_id
    ORDER BY r.started_at DESC
);

-- Table: find all runs sharing a config hash
CREATE OR REPLACE MACRO runs_by_config(h) AS TABLE (
    SELECT
        rc.run_id,
        rc.config_name,
        rc.config_version,
        r.started_at,
        r.status,
        (SELECT count(*) FILTER (WHERE status = 'success') FROM task_results t WHERE t.run_id = rc.run_id) AS successes,
        (SELECT count(*) FROM task_results t WHERE t.run_id = rc.run_id) AS total,
        round(coalesce((SELECT sum(cost) FROM task_results t WHERE t.run_id = rc.run_id), 0), 4) AS total_cost
    FROM run_configs rc
    JOIN runs r ON r.run_id = rc.run_id
    WHERE rc.config_hash = h
    ORDER BY r.started_at DESC
);

-- Table: head-to-head task comparison between two arms in a run
CREATE OR REPLACE MACRO compare_arms(rid, arm_a, arm_b) AS TABLE (
    SELECT
        coalesce(a.task_id, b.task_id) AS task_id,
        a.status AS status_a,
        b.status AS status_b,
        a.cost AS cost_a,
        b.cost AS cost_b,
        a.duration_ms AS duration_a,
        b.duration_ms AS duration_b,
        a.steps AS steps_a,
        b.steps AS steps_b,
        a.tool_calls AS tools_a,
        b.tool_calls AS tools_b
    FROM task_results a
    FULL OUTER JOIN task_results b
        ON a.run_id = b.run_id AND a.task_id = b.task_id
    WHERE a.run_id = rid AND a.arm = arm_a
      AND b.run_id = rid AND b.arm = arm_b
    ORDER BY task_id
);

-- Table: live event stream for a run+arm+task (or all tasks)
CREATE OR REPLACE MACRO live(rid, a) AS TABLE (
    SELECT
        arm,
        task_id,
        seq,
        event_type,
        timestamp,
        json_extract_string(data, '$.type') AS detail_type,
        substr(data::VARCHAR, 1, 200) AS data_preview
    FROM live_events
    WHERE run_id = rid AND arm = a
    ORDER BY task_id, seq
);

-- Scalar: count live events for a run (quick progress check)
CREATE OR REPLACE MACRO event_count(rid) AS (
    SELECT count(*) FROM live_events WHERE run_id = rid
);

-- Table: task results for a run+arm (used by ResultStore.read_results)
CREATE OR REPLACE MACRO results_for_arm(rid, a) AS TABLE (
    SELECT
        task_id, arm, status, timestamp, structured_output,
        error, error_type, session_id, steps, tool_calls,
        tokens_input, tokens_output, tokens_cache_read, cost, duration_ms
    FROM task_results
    WHERE run_id = rid AND arm = a
    ORDER BY timestamp
);

-- Table: completed task IDs for a run+arm (used by ResultStore.completed_task_ids)
CREATE OR REPLACE MACRO completed_tasks(rid, a) AS TABLE (
    SELECT task_id
    FROM task_results
    WHERE run_id = rid AND arm = a
);

-- Table: all scores for a run (used by ResultStore.read_scores)
CREATE OR REPLACE MACRO scores_for_run(rid) AS TABLE (
    SELECT arm, task_id, evaluator, score
    FROM scores
    WHERE run_id = rid
    ORDER BY arm, task_id, evaluator
);

-- Table: snapshot data for a run+arm+task
CREATE OR REPLACE MACRO snapshot(rid, a, tid) AS TABLE (
    SELECT data
    FROM snapshots
    WHERE run_id = rid AND arm = a AND task_id = tid
);

-- Table: all run IDs with task results, most recent first
CREATE OR REPLACE MACRO all_run_ids() AS TABLE (
    SELECT DISTINCT run_id
    FROM task_results
    ORDER BY run_id DESC
);

-- Table: config snapshot for a run
CREATE OR REPLACE MACRO run_config(rid) AS TABLE (
    SELECT config_name, config_version, config_hash, config, files
    FROM run_configs
    WHERE run_id = rid
);

-- Table: run IDs sharing a config hash (lightweight, no join with runs)
CREATE OR REPLACE MACRO run_ids_by_config(h) AS TABLE (
    SELECT run_id
    FROM run_configs
    WHERE config_hash = h
    ORDER BY run_id DESC
);

-- Table: task results with their scores for a run+arm
CREATE OR REPLACE MACRO task_detail(rid, a) AS TABLE (
    SELECT
        tr.task_id,
        tr.status,
        tr.cost,
        tr.duration_ms,
        tr.steps,
        tr.tool_calls,
        tr.error_type,
        s.evaluator,
        s.score
    FROM task_results tr
    LEFT JOIN scores s
        ON tr.run_id = s.run_id AND tr.arm = s.arm AND tr.task_id = s.task_id
    WHERE tr.run_id = rid AND tr.arm = a
    ORDER BY tr.task_id, s.evaluator
);
