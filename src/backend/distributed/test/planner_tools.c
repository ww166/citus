#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

#include "distributed/citus_ruleutils.h"
#include "test/planner_tools.h"


char *
DeparseQuery(Query *q)
{
	StringInfo queryString = makeStringInfo();
	pg_get_query_def(q, queryString);
	return queryString->data;
}


char *
RelidsAsString(Relids relids)
{
	StringInfo r = makeStringInfo();

	int relid = -1;
	while ((relid = bms_next_member(relids, relid)) >= 0)
	{
		appendStringInfo(r, "%d, ", relid);
	}

	return r->data;
}
