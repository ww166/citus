-- File to create functions and helpers needed for subsequent tests

-- create a helper function to create objects on each node
CREATE OR REPLACE FUNCTION run_command_on_master_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- Create a function to make sure that queries returning the same result
CREATE OR REPLACE FUNCTION raise_failed_execution(query text) RETURNS void AS $$
BEGIN
	EXECUTE query;
	EXCEPTION WHEN OTHERS THEN
	IF SQLERRM LIKE 'failed to execute task%' THEN
		RAISE 'Task failed to execute';
	END IF;
END;
$$LANGUAGE plpgsql;

-- Create a function to ignore worker plans in explain output
CREATE OR REPLACE FUNCTION coordinator_plan(explain_command text, out query_plan text)
RETURNS SETOF TEXT AS $$
BEGIN
  FOR query_plan IN execute explain_command LOOP
    RETURN next;
    IF query_plan LIKE '%Task Count:%'
    THEN
        RETURN;
    END IF;
  END LOOP;
  RETURN;
END; $$ language plpgsql;

-- Create a function to ignore worker plans in explain output
-- It also shows task count for plan and subplans
CREATE OR REPLACE FUNCTION coordinator_plan_with_subplans(explain_command text, out query_plan text)
RETURNS SETOF TEXT AS $$
DECLARE
    task_count_line_reached boolean := false;
BEGIN
  FOR query_plan IN execute explain_command LOOP
    IF NOT task_count_line_reached THEN
        RETURN next;
    END IF;
    IF query_plan LIKE '%Task Count:%' THEN
        IF NOT task_count_line_reached THEN
            SELECT true INTO task_count_line_reached;
        ELSE
            RETURN next;
        END IF;
    END IF;
  END LOOP;
  RETURN;
END; $$ language plpgsql;

-- Create a function to normalize Memory Usage, Buckets, Batches
CREATE OR REPLACE FUNCTION plan_normalize_memory(explain_command text, out query_plan text)
RETURNS SETOF TEXT AS $$
BEGIN
  FOR query_plan IN execute explain_command LOOP
    query_plan := regexp_replace(query_plan, '(Memory( Usage)?|Buckets|Batches): \S*',  '\1: xxx', 'g');
    RETURN NEXT;
  END LOOP;
END; $$ language plpgsql;

-- helper function that returns true if output of given explain has "is not null" (case in-sensitive)
CREATE OR REPLACE FUNCTION explain_has_is_not_null(explain_command text)
RETURNS BOOLEAN AS $$
DECLARE
  query_plan text;
BEGIN
  FOR query_plan IN EXECUTE explain_command LOOP
    IF query_plan ILIKE '%is not null%'
    THEN
        RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END; $$ language plpgsql;

-- helper function that returns true if output of given explain has "is not null" (case in-sensitive)
CREATE OR REPLACE FUNCTION explain_has_distributed_subplan(explain_command text)
RETURNS BOOLEAN AS $$
DECLARE
  query_plan text;
BEGIN
  FOR query_plan IN EXECUTE explain_command LOOP
    IF query_plan ILIKE '%Distributed Subplan %_%'
    THEN
        RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END; $$ language plpgsql;

--helper function to check there is a single task
CREATE OR REPLACE FUNCTION explain_has_single_task(explain_command text)
RETURNS BOOLEAN AS $$
DECLARE
  query_plan text;
BEGIN
  FOR query_plan IN EXECUTE explain_command LOOP
    IF query_plan ILIKE '%Task Count: 1%'
    THEN
        RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END; $$ language plpgsql;

-- helper function to quickly run SQL on the whole cluster
CREATE OR REPLACE FUNCTION run_command_on_coordinator_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- 1. Marks the given procedure as colocated with the given table.
-- 2. Marks the argument index with which we route the procedure.
CREATE OR REPLACE FUNCTION colocate_proc_with_table(procname text, tablerelid regclass, argument_index int)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    update pg_catalog.pg_dist_object
    set distribution_argument_index = argument_index, colocationid = pg_dist_partition.colocationid
    from pg_proc, pg_dist_partition
    where proname = procname and oid = objid and pg_dist_partition.logicalrelid = tablerelid;
END;$$;

-- helper function to verify the function of a coordinator is the same on all workers
CREATE OR REPLACE FUNCTION verify_function_is_same_on_workers(funcname text)
    RETURNS bool
    LANGUAGE plpgsql
AS $func$
DECLARE
    coordinatorSql text;
    workerSql text;
BEGIN
    SELECT pg_get_functiondef(funcname::regprocedure) INTO coordinatorSql;
    FOR workerSql IN SELECT result FROM run_command_on_workers('SELECT pg_get_functiondef(' || quote_literal(funcname) || '::regprocedure)') LOOP
            IF workerSql != coordinatorSql THEN
                RAISE INFO 'functions are different, coordinator:% worker:%', coordinatorSql, workerSql;
                RETURN false;
            END IF;
        END LOOP;

    RETURN true;
END;
$func$;

--
-- Procedure for creating shards for range partitioned distributed table.
--
CREATE OR REPLACE PROCEDURE create_range_partitioned_shards(rel regclass, minvalues text[], maxvalues text[])
AS $$
DECLARE
  new_shardid bigint;
  idx int;
BEGIN
  FOR idx IN SELECT * FROM generate_series(1, array_length(minvalues, 1))
  LOOP
    SELECT master_create_empty_shard(rel::text) INTO new_shardid;
    UPDATE pg_dist_shard SET shardminvalue=minvalues[idx], shardmaxvalue=maxvalues[idx] WHERE shardid=new_shardid;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Introduce a function that waits until all cleanup records are deleted, for testing purposes
CREATE OR REPLACE FUNCTION wait_for_resource_cleanup() RETURNS void
SET client_min_messages TO WARNING
AS $$
DECLARE
record_count integer;
BEGIN
    EXECUTE 'SELECT COUNT(*) FROM pg_catalog.pg_dist_cleanup' INTO record_count;
    WHILE  record_count != 0 LOOP
      CALL pg_catalog.citus_cleanup_orphaned_resources();
      EXECUTE 'SELECT COUNT(*) FROM pg_catalog.pg_dist_cleanup' INTO record_count;
    END LOOP;
END$$ LANGUAGE plpgsql;
