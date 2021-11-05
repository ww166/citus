CREATE SCHEMA "prepared statements";
SET search_path TO "prepared statements";
SELECT grant_schema_to_regularuser('"prepared statements"');

CREATE TABLE repartition_prepared_test (a int, b int);
SELECT create_distributed_table('repartition_prepared_test', 'a');
SELECT grant_table_to_regularuser('"prepared statements"."repartition_prepared_test"');
INSERT INTO repartition_prepared_test SELECT i%2, i%3 FROM generate_series(0,24)i;

-- create a custom type which also exists on worker nodes
CREATE TYPE test_composite_type AS (
    i integer,
    i2 integer
);

CREATE TABLE router_executor_table (
    id bigint NOT NULL,
    comment varchar(20),
    stats test_composite_type
);
SELECT create_distributed_table('router_executor_table', 'id');
SELECT grant_table_to_regularuser('"prepared statements"."router_executor_table"');



-- test router executor with prepare statements
CREATE TABLE prepare_table (
	key int,
	value int
);
SELECT create_distributed_table('prepare_table','key');
SELECT grant_table_to_regularuser('"prepared statements"."prepare_table"');


-- Testing parameters + function evaluation
CREATE TABLE prepare_func_table (
    key text,
    value1 int,
    value2 text,
    value3 timestamptz DEFAULT now()
);
SELECT create_distributed_table('prepare_func_table', 'key');
SELECT grant_table_to_regularuser('"prepared statements"."prepare_func_table"');

-- test function evaluation with parameters in an expression
PREPARE prepared_function_evaluation_insert(int) AS
	INSERT INTO prepare_func_table (key, value1) VALUES ($1+1, 0*random());


-- Text columns can give issues when there is an implicit cast from varchar
CREATE TABLE text_partition_column_table (
    key text NOT NULL,
    value int
);
SELECT create_distributed_table('text_partition_column_table', 'key');
SELECT grant_table_to_regularuser('"prepared statements"."text_partition_column_table"');


-- Domain type columns can give issues
-- and we use offset to prevent output diverging

CREATE DOMAIN test_key AS text CHECK(VALUE ~ '^test-\d$');
SELECT run_command_on_workers($$
  CREATE DOMAIN "prepared statements".test_key AS text CHECK(VALUE ~ '^test-\d$')
$$) OFFSET 10000;

CREATE TABLE domain_partition_column_table (
    key test_key NOT NULL,
    value int
);
SELECT create_distributed_table('domain_partition_column_table', 'key');
SELECT grant_table_to_regularuser('"prepared statements"."domain_partition_column_table"');


-- verify we re-evaluate volatile functions every time
CREATE TABLE http_request (
  site_id INT,
  ingest_time TIMESTAMPTZ DEFAULT now(),
  url TEXT,
  request_country TEXT,
  ip_address TEXT,
  status_code INT,
  response_time_msec INT
);

SELECT create_distributed_table('http_request', 'site_id');
SELECT grant_table_to_regularuser('"prepared statements"."http_request"');
