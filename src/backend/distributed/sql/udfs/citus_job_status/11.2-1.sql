CREATE OR REPLACE FUNCTION pg_catalog.citus_job_status (
    job_id bigint DEFAULT NULL
)
    RETURNS TABLE (
            job_id bigint,
            state pg_catalog.citus_job_status,
            job_type name,
            description text,
            started_at timestamptz,
            finished_at timestamptz,
            details jsonb
    )
    LANGUAGE SQL
    STRICT
    AS $fn$
    WITH join_all_rebalance_records AS (
        SELECT
            grp.sessionid AS grp_sessionid,
            grp.table_name AS grp_table_name,
            grp.shardid AS grp_shardid,
            grp.shard_size AS grp_shard_size,
            grp.sourcename AS grp_sourcename,
            grp.sourceport AS grp_sourceport,
            grp.targetname AS grp_targetname,
            grp.targetport AS grp_targetport,
            grp.progress AS grp_progress,
            grp.source_shard_size AS grp_source_shard_size,
            grp.target_shard_size AS grp_target_shard_size,
            grp.operation_type AS grp_operation_type,
            grp.source_lsn AS grp_source_lsn,
            grp.target_lsn AS grp_target_lsn,
            grp.status AS grp_status,
            t.job_id AS t_job_id,
            t.task_id AS t_task_id,
            t.owner AS t_owner,
            t.pid AS t_pid,
            t.status AS t_status,
            t.command AS t_command,
            t.retry_count AS t_retry_count,
            t.not_before AS t_not_before,
            t.message AS t_message,
            j.job_id AS j_job_id,
            j.state AS j_state,
            j.job_type AS j_job_type,
            j.description AS j_description,
            j.started_at AS j_started_at,
            j.finished_at AS j_finished_at
        FROM
            get_rebalance_progress () grp
            FULL OUTER JOIN pg_dist_background_task t ON grp.sessionid = t.pid
            JOIN pg_dist_background_job j ON t.job_id = j.job_id
        WHERE
            j.job_id = $1
        ORDER BY
            t.task_id ASC
    ), task_state_occurence_counts AS (
        SELECT jsonb_object_agg(t_status, count) AS counts
        FROM (
            SELECT t_status, count(*)
            FROM join_all_rebalance_records
            GROUP BY t_status
        ) subquery
    ), grp_state_occurence_counts AS (
        SELECT coalesce(jsonb_object_agg(grp_status, count), '["No ongoing rebalance in progress"]'::jsonb) AS counts
        FROM (
            SELECT grp_status, count(*)
            FROM join_all_rebalance_records
            WHERE grp_status IS NOT NULL
            GROUP BY grp_status
        ) subquery
    ), total_task_retry_count AS (
        SELECT coalesce(sum(t_retry_count),0)
        FROM join_all_rebalance_records
    )
    SELECT
        j_job_id AS job_id,
        j_state AS state,
        j_job_type AS job_type,
        j_description AS description,
        j_started_at AS started_at,
        j_finished_at AS finished_at,
        jsonb_build_object(
            'task_state_counts', (SELECT * FROM task_state_occurence_counts),
            'total_task_retry_count', (SELECT * FROM total_task_retry_count),
            'ongoing_rebalance_state_counts', (SELECT * FROM grp_state_occurence_counts)
        ) AS details
    FROM
        join_all_rebalance_records
$fn$;
