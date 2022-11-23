-- citus--11.1-1--11.2-1

DROP FUNCTION pg_catalog.worker_append_table_to_shard(text, text, text, integer);

#include "udfs/get_rebalance_progress/11.2-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/11.2-1.sql"
#include "datatypes/citus_cluster_clock/11.2-1.sql"
#include "udfs/citus_get_node_clock/11.2-1.sql"
#include "udfs/citus_get_transaction_clock/11.2-1.sql"
#include "udfs/citus_is_clock_after/11.2-1.sql"
#include "udfs/citus_internal_adjust_local_clock_to_remote/11.2-1.sql"
#include "udfs/worker_split_shard_replication_setup/11.2-1.sql"
#include "udfs/citus_task_wait/11.2-1.sql"

CREATE TABLE citus.pg_dist_shardgroup (
    shardgroupid bigint PRIMARY KEY,
    colocationid integer NOT NULL,
    shardminvalue text,
    shardmaxvalue text
);
ALTER TABLE citus.pg_dist_shardgroup SET SCHEMA pg_catalog;

INSERT INTO pg_catalog.pg_dist_shardgroup
     SELECT min(shardid) as shardgroupid,
            colocationid,
            shardminvalue,
            shardmaxvalue
       FROM pg_dist_shard
       JOIN pg_dist_partition USING (logicalrelid)
   GROUP BY colocationid, shardminvalue, shardmaxvalue;

ALTER TABLE pg_catalog.pg_dist_shard ADD COLUMN shardgroupid bigint;

-- backfill shardgroupid field by finding the generated shardgroup above by joining the colocationid, shardminvalue and
-- shardmaxvalue (for the shardvalues we want to treat NULL values as equal, hence the complex conditions for those).
-- After this operation _all_ shards should have a shardgroupid associated which satisfies the colocation invariant of
-- the shards in the same colocationid.
UPDATE pg_catalog.pg_dist_shard AS shard
   SET shardgroupid = shardgroup.shardgroupid
  FROM (
      SELECT shardgroupid,
             colocationid,
             shardminvalue,
             shardmaxvalue,
             logicalrelid
        FROM pg_catalog.pg_dist_shardgroup
        JOIN pg_dist_partition USING (colocationid)
  ) AS shardgroup
WHERE shard.logicalrelid = shardgroup.logicalrelid
  AND (
         shard.shardminvalue = shardgroup.shardminvalue
      OR (
              shard.shardminvalue      IS NULL
          AND shardgroup.shardminvalue IS NULL
      )
  )
  AND (
          shard.shardmaxvalue = shardgroup.shardmaxvalue
      OR (    shard.shardmaxvalue      IS NULL
          AND shardgroup.shardmaxvalue IS NULL
      )
  );

-- risky, but we want to fail quickly here. If there are cases where a shard is not associated correctly with a
-- shardgroup we would not want the setup to use the new Citus version as it hard relies on the shardgroups being
-- correctly associated.
ALTER TABLE pg_catalog.pg_dist_shard ALTER COLUMN shardgroupid SET NOT NULL;

#include "udfs/citus_internal_add_shard_metadata/11.2-1.sql"
DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(
    relation_id regclass, shard_id bigint,
    storage_type "char", shard_min_value text,
    shard_max_value text
);
