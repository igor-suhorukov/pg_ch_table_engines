/*-------------------------------------------------------------------------
 *
 * types.c
 *	  A PostgreSQL column's ClickHouse type (ch_table_engines.md §2.5): one
 *	  whose values are exactly the column's, so a round trip is the identity.
 *
 *	  Every column is Nullable(T), NOT NULL or not: SET and DROP NOT NULL then
 *	  change nothing in the parts, and a null map of zeros costs next to
 *	  nothing compressed.  A column is named c<attnum> in ClickHouse, so a
 *	  rename changes nothing and no PostgreSQL name meets ClickHouse's
 *	  quoting or its virtual columns' names.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/lsyscache.h"

#include "chte.h"

/* The widest Decimal kept as a scaled integer: Decimal128 */
#define CH_DECIMAL_MAX_PRECISION 38

void
ch_column_for_attribute(Form_pg_attribute att, ChColumn *col)
{
	memset(col, 0, sizeof(*col));
	if (att->attisdropped)
	{
		col->kind = CH_COL_DROPPED;
		return;
	}
	col->typid = att->atttypid;
	col->typmod = att->atttypmod;
	col->typlen = att->attlen;
	col->typbyval = att->attbyval;

	switch (att->atttypid)
	{
		case BOOLOID:
			col->kind = CH_COL_FIXED;
			col->width = 1;
			strcpy(col->chtype, "Bool");
			return;
		case INT2OID:
			col->kind = CH_COL_FIXED;
			col->width = 2;
			strcpy(col->chtype, "Int16");
			return;
		case INT4OID:
		case DATEOID:			/* days from 2000-01-01, PostgreSQL's epoch */
			col->kind = CH_COL_FIXED;
			col->width = 4;
			strcpy(col->chtype, "Int32");
			return;
		case INT8OID:
		case TIMEOID:			/* microseconds */
		case TIMESTAMPOID:		/* microseconds from 2000-01-01 */
		case TIMESTAMPTZOID:
			col->kind = CH_COL_FIXED;
			col->width = 8;
			strcpy(col->chtype, "Int64");
			return;
		case OIDOID:
			col->kind = CH_COL_FIXED;
			col->width = 4;
			strcpy(col->chtype, "UInt32");
			return;
		case FLOAT4OID:
			col->kind = CH_COL_FIXED;
			col->width = 4;
			strcpy(col->chtype, "Float32");
			return;
		case FLOAT8OID:
			col->kind = CH_COL_FIXED;
			col->width = 8;
			strcpy(col->chtype, "Float64");
			return;
		case NUMERICOID:
			if (att->atttypmod >= (int32) VARHDRSZ)
			{
				int			precision = ((att->atttypmod - VARHDRSZ) >> 16) & 0xffff;
				int			scale = (((att->atttypmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;

				if (precision >= 1 && precision <= CH_DECIMAL_MAX_PRECISION && scale >= 0 && scale <= precision)
				{
					col->kind = CH_COL_NUMERIC;
					col->precision = precision;
					col->scale = scale;
					col->width = precision <= 9 ? 4 : precision <= 18 ? 8 : 16;
					snprintf(col->chtype, sizeof(col->chtype), "Decimal(%d, %d)", precision, scale);
					return;
				}
			}
			/* NaN, infinities, a wider or a negative scale: PostgreSQL's bytes */
			break;
		default:
			break;
	}

	if (att->attbyval && (att->attlen == 1 || att->attlen == 2 || att->attlen == 4 || att->attlen == 8))
	{
		col->kind = CH_COL_FIXED;
		col->width = att->attlen;
		snprintf(col->chtype, sizeof(col->chtype), "Int%d", att->attlen * 8);
	}
	else if (att->attlen > 0)
	{
		/* fixed length by reference: uuid, name, interval, macaddr, ... */
		col->kind = CH_COL_FIXED;
		col->width = att->attlen;
		snprintf(col->chtype, sizeof(col->chtype), "FixedString(%d)", att->attlen);
	}
	else if (att->attlen == -2)
	{
		col->kind = CH_COL_CSTRING;
		strcpy(col->chtype, "String");
	}
	else
	{
		col->kind = CH_COL_VARLENA;
		strcpy(col->chtype, "String");
	}
}
