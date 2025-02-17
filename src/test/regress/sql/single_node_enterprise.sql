-- we already have lots of tests targeting
-- single node citus clusters in sql/single_node.sql
-- in this file, we are testing enterprise features
CREATE SCHEMA single_node_ent;
SET search_path TO single_node_ent;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 90730500;

-- idempotently add node to allow this test to run without add_coordinator
SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);
RESET client_min_messages;

SELECT 1 FROM master_set_node_property('localhost', :master_port, 'shouldhaveshards', true);

CREATE USER full_access_single_node;
CREATE USER read_access_single_node;
CREATE USER no_access_single_node;

CREATE TYPE new_type AS (n int, m text);
CREATE TABLE test(x int, y int, z new_type);
SELECT create_distributed_table('test','x');

CREATE TABLE ref(a int, b int);
SELECT create_reference_table('ref');

-- we want to test replicate_table_shards()
-- which requiest statement based
CREATE TABLE statement_replicated(a int PRIMARY KEY);
SELECT create_distributed_table('statement_replicated','a', colocate_with:='none');
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid='statement_replicated'::regclass;

-- We create this function to make sure
-- GRANT ALL ON ALL FUNCTIONS IN SCHEMA  doesn't get stuck.
CREATE FUNCTION notice(text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '%', $1;
END;
$$;
SELECT create_distributed_function('notice(text)');

-- allow access to various users
GRANT ALL ON TABLE test,ref TO full_access_single_node;
GRANT USAGE ON SCHEMA single_node_ent TO full_access_single_node;
ALTER ROLE full_access_single_node WITH LOGIN;
GRANT ALL PRIVILEGES ON DATABASE postgres TO no_access_single_node;
REVOKE ALL PRIVILEGES ON DATABASE postgres FROM no_access_single_node;
REVOKE USAGE, CREATE ON SCHEMA single_node_ent FROM no_access_single_node;
GRANT SELECT ON ref,test TO no_access_single_node;
REVOKE SELECT ON ref,test FROM no_access_single_node;

-- we have to use local execution, otherwise we hit to a known issue
-- (see https://github.com/citusdata/citus-enterprise/ issues/474)
-- to force local execution, use transaction block
BEGIN;
GRANT USAGE ON SCHEMA single_node_ent TO read_access_single_node;
GRANT SELECT ON ALL TABLES IN SCHEMA single_node_ent TO read_access_single_node;
COMMIT;

-- revoke SELECT access for the next 3-4 tests
REVOKE SELECT ON test FROM read_access_single_node;

-- Make sure the access is revoked
SET ROLE read_access_single_node;
SELECT COUNT(*) FROM test;

SET ROLE postgres;

BEGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA single_node_ent TO read_access_single_node;
-- Make sure we can now read as read_access_single_node role
SET ROLE read_access_single_node;
SELECT COUNT(*) FROM test;
SET ROLE postgres;

-- Make sure REVOKE .. IN SCHEMA also works
REVOKE SELECT ON ALL TABLES IN SCHEMA single_node_ent FROM read_access_single_node;
SET ROLE read_access_single_node;
SELECT COUNT(*) FROM test;
ROLLBACK;

GRANT ALL ON ALL FUNCTIONS IN SCHEMA single_node_ent to full_access_single_node;
GRANT SELECT ON ALL TABLES IN SCHEMA single_node_ent TO read_access_single_node;
GRANT SELECT ON test, ref TO read_access_single_node;
SET ROLE read_access_single_node;
-- Make sure we can now read as read_access_single_node role
SELECT COUNT(*) FROM test;

SET ROLE full_access_single_node;

INSERT INTO test VALUES (1, 1, (95, 'citus9.5')::new_type);

-- should fail as only read access is allowed
SET ROLE read_access_single_node;
INSERT INTO test VALUES (1, 1, (95, 'citus9.5')::new_type);

SELECT nodeid AS coordinator_node_id FROM pg_dist_node WHERE nodename = 'localhost' AND nodeport = :master_port
\gset


-- pg_dist_poolinfo should work fine for coordinator
-- put outright bad values
SET ROLE postgres;
INSERT INTO pg_dist_poolinfo VALUES (:coordinator_node_id, 'host=failhost');
\c
SET search_path TO single_node_ent;

\set VERBOSITY terse

-- supress OS specific error message
DO $$
BEGIN
        BEGIN
            -- we want to force remote execution
        	SET LOCAL citus.enable_local_execution TO false;
			SET LOCAL client_min_messages TO ERROR;
            SELECT COUNT(*) FROM test;
        EXCEPTION WHEN OTHERS THEN
                IF SQLERRM LIKE 'connection to the remote node%%' THEN
                       RAISE 'failed to execute select';
                END IF;
        END;
END;
$$;

TRUNCATE pg_dist_poolinfo;

-- using 127.0.0.1 should work fine
INSERT INTO pg_dist_poolinfo VALUES (:coordinator_node_id,  'host=127.0.0.1 port=' || :master_port);
\c
SET search_path TO single_node_ent;
SET citus.log_remote_commands TO ON;
SET client_min_messages TO DEBUG1;
-- force multi-shard query to be able to
-- have remote connections
SELECT COUNT(*) FROM test WHERE x = 1 OR x = 2;
RESET citus.log_remote_commands;
RESET client_min_messages;
TRUNCATE pg_dist_poolinfo;

-- reconnect
\c
SET search_path TO single_node_ent;

-- now, create a colocated table
-- add a new node, and move the
-- shards to the new node
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 90731500;

CREATE TABLE colocated_table (x int PRIMARY KEY,  y int);
SELECT create_distributed_table('colocated_table','x', colocate_with:='single_node_ent.test');

-- create some foreign keys
TRUNCATE test, ref;

ALTER TABLE test ADD CONSTRAINT p_key PRIMARY KEY(x);
ALTER TABLE ref ADD CONSTRAINT p_key_2 PRIMARY KEY(a);

ALTER TABLE colocated_table ADD CONSTRAINT fkey  FOREIGN KEY (x) REFERENCES test(x);
ALTER TABLE test ADD CONSTRAINT fkey FOREIGN KEY (x) REFERENCES ref(a);

-- load some data
INSERT INTO ref SELECT i, i*2 FROM generate_series(0,50)i;
INSERT INTO test SELECT i, i*2, (i, 'citus' || i)::new_type FROM generate_series(0,50)i;
INSERT INTO colocated_table SELECT i, i*2 FROM generate_series(0,50)i;

-- run a very basic query
SELECT count(*) FROM (test JOIN colocated_table USING (x)) as foo LEFT JOIN ref ON(foo.x = a);

CREATE VIEW view_created_before_shard_moves AS
	SELECT count(*) FROM (test JOIN colocated_table USING (x)) as foo LEFT JOIN ref ON(foo.x = a);

SELECT * FROM view_created_before_shard_moves;

-- show that tenant isolation works fine
SELECT isolate_tenant_to_new_shard('test', 5, 'CASCADE', shard_transfer_mode => 'block_writes');

-- in the first iteration, have an
-- hybrid cluster meaning that
-- the shards exists on both the coordinator
-- and on the workers
SELECT 1 FROM master_add_node('localhost', :worker_1_port);

-- make sure that we can replicate tables as well
select replicate_table_shards('statement_replicated', shard_replication_factor:=2, shard_transfer_mode:='block_writes');
-- we don't need the table anymore, it complicates the output of rebalances
DROP TABLE statement_replicated;

-- move 1 set of colocated shards in non-blocking mode
-- and the other in block_writes
SELECT rebalance_table_shards(max_shard_moves:=1);
SELECT rebalance_table_shards(shard_transfer_mode:='block_writes');

-- should fail as only read access is allowed
SET ROLE read_access_single_node;
INSERT INTO test VALUES (1, 1, (95, 'citus9.5')::new_type);

SET ROLE postgres;
\c
SET search_path TO single_node_ent;

-- the same query should work
SELECT count(*) FROM (test JOIN colocated_table USING (x)) as foo LEFT JOIN ref ON(foo.x = a);

-- make sure that composite type is created
-- on the worker
SELECT * FROM test ORDER BY 1 DESC, 2, 3 LIMIT 1;

-- make sure that we can execute with intermediate
-- results that are needed on all shards on the
-- final step
WITH cte_1 AS (SELECT * FROM test ORDER BY 1 DESC, 2, 3 LIMIT 5)
SELECT count(*) FROM colocated_table JOIN cte_1 USING (x);

-- make sure that we can still query the view
SELECT * FROM view_created_before_shard_moves;

-- we should be able to switch the cluster to CitusMX
SELECT start_metadata_sync_to_node('localhost', :master_port);
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

-- sanity-check: the same queries should work
SELECT count(*) FROM (test JOIN colocated_table USING (x)) as foo LEFT JOIN ref ON(foo.x = a);
SELECT * FROM test ORDER BY 1 DESC,2,3 LIMIT 1;
WITH cte_1 AS (SELECT * FROM test ORDER BY 1 DESC,2,3 LIMIT 5)
SELECT count(*) FROM colocated_table JOIN cte_1 USING (x);

-- all DDLs should work
ALTER TABLE colocated_table ADD COLUMN z single_node_ent.new_type;
UPDATE colocated_table SET z = (x, y::text)::new_type;
SELECT * FROM colocated_table ORDER BY 1 DESC,2,3 LIMIT 1;
CREATE INDEX i_colocated_table ON colocated_table(y);
BEGIN;
	CREATE INDEX i_colocated_table_2 ON colocated_table(x,y);
ROLLBACK;

-- sanity check: transaction blocks spanning both nodes should work fine
BEGIN;
	UPDATE colocated_table SET y = y + 1;
	UPDATE test SET y = y -1;
	SELECT max(y) FROM colocated_table;
	SELECT max(y) FROM test;
ROLLBACK;

-- generate data so that we can enforce fkeys
INSERT INTO ref SELECT i, i*2 FROM generate_series(100,150)i;

-- the first insert goes to a shard on the worker
-- the second insert goes to a shard on the coordinator
BEGIN;
	SET LOCAL citus.log_remote_commands TO ON;
	INSERT INTO test(x,y) VALUES (101,100);
	INSERT INTO test(x,y) VALUES (102,100);

	-- followed by a multi-shard command
	SELECT count(*) FROM test;
ROLLBACK;

-- the first insert goes to a shard on the coordinator
-- the second insert goes to a shard on the worker
BEGIN;
	SET LOCAL citus.log_remote_commands TO ON;
	INSERT INTO test(x,y) VALUES (102,100);
	INSERT INTO test(x,y) VALUES (101,100);

	-- followed by a multi-shard command
	SELECT count(*) FROM test;
ROLLBACK;

-- now, lets move all the shards of distributed tables out of the coordinator
-- block writes is much faster for the sake of the test timings we prefer it
SELECT master_drain_node('localhost', :master_port, shard_transfer_mode:='block_writes');

-- should return false as master_drain_node had just set it to false for coordinator
SELECT shouldhaveshards FROM pg_dist_node WHERE nodeport = :master_port;

-- sanity-check: the same queries should work
SELECT count(*) FROM (test JOIN colocated_table USING (x)) as foo LEFT JOIN ref ON(foo.x = a);
SELECT * FROM test ORDER BY 1 DESC,2 ,3 LIMIT 1;
WITH cte_1 AS (SELECT * FROM test ORDER BY 1 DESC,2 , 3 LIMIT 5)
SELECT count(*) FROM colocated_table JOIN cte_1 USING (x);

-- make sure that we can still query the view
SELECT * FROM view_created_before_shard_moves;

-- and make sure that all the shards are remote
BEGIN;
	SET LOCAL citus.log_remote_commands TO ON;
	INSERT INTO test(x,y) VALUES (101,100);
	INSERT INTO test(x,y) VALUES (102,100);

	-- followed by a multi-shard command
	SELECT count(*) FROM test;
ROLLBACK;

-- should fail as only read access is allowed
SET ROLE read_access_single_node;
INSERT INTO test VALUES (1, 1, (95, 'citus9.5')::new_type);

SET ROLE postgres;
\c

SET search_path TO single_node_ent;

-- Cleanup
RESET citus.log_remote_commands;
SET client_min_messages TO WARNING;
DROP SCHEMA single_node_ent CASCADE;

DROP OWNED BY full_access_single_node;
DROP OWNED BY read_access_single_node;
DROP ROLE full_access_single_node;
DROP ROLE read_access_single_node;

-- remove the nodes for next tests
SELECT 1 FROM master_remove_node('localhost', :master_port);
SELECT 1 FROM master_remove_node('localhost', :worker_1_port);

-- restart nodeid sequence so that multi_cluster_management still has the same
-- nodeids
ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 1;
ALTER SEQUENCE pg_dist_groupid_seq RESTART 1;
