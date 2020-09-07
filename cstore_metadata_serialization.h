/*-------------------------------------------------------------------------
 *
 * cstore_metadata_serialization.h
 *
 * Type and function declarations to serialize/deserialize cstore metadata.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CSTORE_SERIALIZATION_H
#define CSTORE_SERIALIZATION_H

/* Function declarations for metadata serialization */
extern StringInfo SerializePostScript(uint64 tableFooterLength);
extern StringInfo SerializeTableFooter(TableFooter *tableFooter);
extern StringInfo SerializeColumnSkipList(ColumnBlockSkipNode *blockSkipNodeArray,
										  uint32 blockCount, bool typeByValue,
										  int typeLength);

/* Function declarations for metadata deserialization */
extern void DeserializePostScript(StringInfo buffer, uint64 *tableFooterLength);
extern TableFooter * DeserializeTableFooter(StringInfo buffer);
extern uint32 DeserializeBlockCount(StringInfo buffer);
extern uint32 DeserializeRowCount(StringInfo buffer);
extern ColumnBlockSkipNode * DeserializeColumnSkipList(StringInfo buffer,
													   bool typeByValue, int typeLength,
													   uint32 blockCount);


#endif   /* CSTORE_SERIALIZATION_H */
