CREATE OR REPLACE FUNCTION pg_catalog.worker_copy_table_to_node(
    source_table regclass,
    target_node_name text,
	target_node_port integer,
	is_local bool default false)
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_copy_table_to_node$$;
COMMENT ON FUNCTION pg_catalog.worker_copy_table_to_node(regclass, text, integer, bool)
    IS 'Perform copy of a shard';
