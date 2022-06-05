
#include "postgres.h"

#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_class.h"
#include "catalog/storage.h"
#include "storage/relfilenode.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/relcache.h"

#include "distributed/listutils.h"


PG_FUNCTION_INFO_V1(move_tuples);


static void
CopyAttributes(Relation sourceRelation, Relation targetRelation)
{
    /*
     * 1- Modify non-dropped attr's attrnums to match the source rel.
     */
    int targetNewDroppedAttrArrayLen = 0;
    Form_pg_attribute *targetNewDroppedAttrArray =
        palloc0(sizeof(Form_pg_attribute) *
                RelationGetNumberOfAttributes(sourceRelation));

    List *targetNewNonDroppedAttrTupleList = NIL;

    for (int i = 0; i < RelationGetNumberOfAttributes(sourceRelation); i++)
    {
        Form_pg_attribute sourceAttr =
            TupleDescAttr(RelationGetDescr(sourceRelation), i);

        if (sourceAttr->attisdropped)
        {
            Form_pg_attribute targetNewDroppedAttr = palloc0(sizeof(FormData_pg_attribute));
            *targetNewDroppedAttr = *((FormData_pg_attribute *) sourceAttr);

            targetNewDroppedAttr->attrelid = RelationGetRelid(targetRelation);
            targetNewDroppedAttrArray[targetNewDroppedAttrArrayLen++] = targetNewDroppedAttr;
        }
        else
        {
            HeapTuple targetAttrTuple = SearchSysCache2(
                ATTNAME,
                ObjectIdGetDatum(RelationGetRelid(targetRelation)),
                PointerGetDatum(sourceAttr->attname.data));

            if (!HeapTupleIsValid(targetAttrTuple))
            {
                ereport(ERROR, (errmsg("no matching column")));
            }

            Form_pg_attribute targetAttr= (Form_pg_attribute) GETSTRUCT(targetAttrTuple);
            targetAttr->attnum = sourceAttr->attnum;

            targetNewNonDroppedAttrTupleList = lappend(targetNewNonDroppedAttrTupleList,
                                                       heap_copytuple(targetAttrTuple));

            ReleaseSysCache(targetAttrTuple);
        }
    }

    CommandCounterIncrement();

    Relation pgAttr = table_open(AttributeRelationId, RowExclusiveLock);

    HeapTuple updateTuple = NULL;
    foreach_ptr(updateTuple, targetNewNonDroppedAttrTupleList)
    {
        CatalogTupleUpdate(pgAttr, &updateTuple->t_self, updateTuple);
    }

    table_close(pgAttr, RowExclusiveLock);

    /*
     * 2- Update natts of target rel.
     */
    Relation pgClass = table_open(RelationRelationId, RowExclusiveLock);

	HeapTuple targetRelTup = SearchSysCache1(
        RELOID,
        ObjectIdGetDatum(RelationGetRelid(targetRelation)));
	if (!HeapTupleIsValid(targetRelTup))
	{
        ereport(ERROR,
                (errmsg("cache lookup failed for relation with OID %u",
                        RelationGetRelid(targetRelation))));
	}

	Form_pg_class classForm = (Form_pg_class) GETSTRUCT(targetRelTup);
    classForm->relnatts = RelationGetNumberOfAttributes(sourceRelation);

    CatalogTupleUpdate(pgClass, &targetRelTup->t_self, targetRelTup);

    ReleaseSysCache(targetRelTup);

    table_close(pgClass, RowExclusiveLock);

    /*
     * 3- Drop target rel's dropped attrs.
     */
    pgAttr = table_open(AttributeRelationId, RowExclusiveLock);

    for (int i = 0; i < RelationGetNumberOfAttributes(targetRelation); i++)
    {
        Form_pg_attribute targetAttr =
            TupleDescAttr(RelationGetDescr(targetRelation), i);

        if (!targetAttr->attisdropped)
        {
            continue;
        }

        Form_pg_attribute targetDroppedAttr = palloc0(sizeof(FormData_pg_attribute));
        *targetDroppedAttr = *((FormData_pg_attribute *) targetAttr);

        HeapTuple targetDroppedAttrTuple = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(RelationGetRelid(targetRelation)),
            PointerGetDatum(targetDroppedAttr->attnum));

        if (!HeapTupleIsValid(targetDroppedAttrTuple))
        {
            ereport(ERROR, (errmsg("cache lookup failed for attnum %d of "
                                   "relation with OID %u",
                                   targetDroppedAttr->attnum,
                                   RelationGetRelid(targetRelation))));
        }

        CatalogTupleDelete(pgAttr, &targetDroppedAttrTuple->t_self);

        ReleaseSysCache(targetDroppedAttrTuple);
    }

    table_close(pgAttr, RowExclusiveLock);

    /*
     * 4- Transfer dropped attrs.
     */
    pgAttr = table_open(AttributeRelationId, RowExclusiveLock);

    CatalogIndexState pgAttrIndexState = CatalogOpenIndexes(pgAttr);

    InsertPgAttributeTuples(
        pgAttr,
        CreateTupleDesc(targetNewDroppedAttrArrayLen, targetNewDroppedAttrArray),
        RelationGetRelid(targetRelation),
        NULL,
        pgAttrIndexState);

    CatalogCloseIndexes(pgAttrIndexState);

    table_close(pgAttr, RowExclusiveLock);
}


static void
RelationSetRelNode(Oid relationId, Oid relfileNodeSpcNode, Oid relfileNodeRelNode)
{
	Relation pgClass = table_open(RelationRelationId, RowExclusiveLock);
	HeapTuple pgClassTuple = SearchSysCacheCopy1(RELOID,
                                                ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(pgClassTuple))
    {
		ereport(ERROR, (errmsg("cache lookup failed for relation %u",
                               relationId)));
    }

	Form_pg_class pgClassForm = (Form_pg_class) GETSTRUCT(pgClassTuple);

    pgClassForm->reltablespace = relfileNodeSpcNode;
    pgClassForm->relfilenode = relfileNodeRelNode;

    CatalogTupleUpdate(pgClass, &pgClassTuple->t_self, pgClassTuple);

	heap_freetuple(pgClassTuple);
	table_close(pgClass, RowExclusiveLock);
}


/*
 * Move tuples of one rel to another.
 *
 * TODO:
 * - How to WAL-log this operation ?
 * - Doesn't handle indexes, maybe simply execute reindex for both rels ?
 * - Need to check all non-dropped attributes are same before CopyAttribute().
 * - Constraints / indexes etc would still refer to old attrNumbers, need to handle this.
 * - Need to handle partitioned tables maybe ?
 *
CREATE FUNCTION move_tuples(source_table regclass,
							target_table regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus', $$move_tuples$$;
*/
Datum
move_tuples(PG_FUNCTION_ARGS)
{
    /*
     * 1- Lock rels and verify they exist.
     */
    Oid sourceRelationId = PG_GETARG_OID(0);
	Relation sourceRelation = table_open(sourceRelationId, AccessExclusiveLock);

    Oid targetRelationId = PG_GETARG_OID(1);
	Relation targetRelation = table_open(targetRelationId, AccessExclusiveLock);

    /*
     * 2- Verify that they're suitable for the "move" operation.
     */
    if (sourceRelation->rd_rel->relkind != RELKIND_RELATION ||
        targetRelation->rd_rel->relkind != RELKIND_RELATION)
    {
        ereport(ERROR, (errmsg("both must be relation")));
    }

    if (sourceRelation->rd_rel->relpersistence !=
        targetRelation->rd_rel->relpersistence)
    {
        ereport(ERROR, (errmsg("relations must have same persistency")));
    }

    if (sourceRelation->rd_rel->relam != targetRelation->rd_rel->relam)
    {
        ereport(ERROR, (errmsg("relations must be of same tableAM")));
    }

    /*
     * TODO: Reconsider if we need to compare some other fields of two rels'
     *       Relation objects.
     */

    /*
     * 3- Move the ownership of relfilenode of source rel to target and
     *    drop the old storage of target rel.
     */
	RelationDropStorage(targetRelation);

    RelFileNode sourceOldRelfilenode = sourceRelation->rd_node;
    RelationSetRelNode(targetRelationId, sourceOldRelfilenode.spcNode,
                       sourceOldRelfilenode.relNode);

	CommandCounterIncrement();
	RelationAssumeNewRelfilenode(targetRelation);

    CopyAttributes(sourceRelation, targetRelation);

    /*
     * 4- Create a new relfilenode and a new storage for the source rel.
     */
    RelFileNode sourceNewRelfilenode = sourceOldRelfilenode;
	sourceNewRelfilenode.relNode = GetNewRelFileNode(
        sourceRelation->rd_rel->reltablespace, NULL,
        sourceRelation->rd_rel->relpersistence);

    TransactionId dummyXactId = InvalidTransactionId;
    MultiXactId dummyMXactId = InvalidMultiXactId;
    table_relation_set_new_filenode(sourceRelation, &sourceNewRelfilenode,
                                    sourceRelation->rd_rel->relpersistence,
                                    &dummyXactId, &dummyMXactId);

    RelationSetRelNode(sourceRelationId, sourceOldRelfilenode.spcNode,
                       sourceNewRelfilenode.relNode);

    CommandCounterIncrement();
	RelationAssumeNewRelfilenode(sourceRelation);

    /*
     * 5- Unlock the rels, we're done.
     */
    table_close(targetRelation, AccessExclusiveLock);
    table_close(sourceRelation, AccessExclusiveLock);

    PG_RETURN_VOID();
}
