/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/log_utils.h"


/*
 * HashLogMessage is deprecated and doesn't do anything anymore. Its indirect
 * usage will be removed later.
 */
char *
HashLogMessage(const char *logText)
{
	return (char *) logText;
}
