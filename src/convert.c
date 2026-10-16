/*-------------------------------------------------------------------------
 *
 * convert.c
 *	  PostgreSQL's datums to the engine library's column vectors and back
 *	  (ch_table_engines.md §2.5, include/chte_engine.h's layouts).
 *
 *	  numeric(p, s) with p <= 38 is a Decimal: a scaled integer of 4, 8 or 16
 *	  bytes.  numeric(p, s) admits NaN; it is kept as the integer type's
 *	  greatest value, which no value of precision p reaches and which sorts
 *	  above every number, as PostgreSQL sorts NaN.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/tupmacs.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

#include "chte.h"

/* A growable byte array */
typedef struct Bytes
{
	char	   *data;
	Size		len;
	Size		cap;
} Bytes;

typedef struct BuilderColumn
{
	ChColumn   *col;			/* NULL for the row id */
	Bytes		values;
	Bytes		offsets;		/* uint64, strings only */
	Bytes		nulls;
	bool		has_nulls;
} BuilderColumn;

struct ChVectorBuilder
{
	ChTable    *table;
	MemoryContext cxt;
	int			ncols;			/* the table's columns, then the row id */
	BuilderColumn *cols;
	int		   *attnums;		/* per builder column: the attribute index, -1 the row id */
	uint32		nrows;
	chte_vector *vectors;
};

static void
bytes_reserve(MemoryContext cxt, Bytes *b, Size more)
{
	if (b->len + more <= b->cap)
		return;
	{
		Size		cap = Max(b->cap * 2, Max(b->len + more, (Size) 1024));

		if (b->data == NULL)
			b->data = MemoryContextAllocHuge(cxt, cap);
		else
			b->data = repalloc_huge(b->data, cap);
		b->cap = cap;
	}
}

static inline void
bytes_append(MemoryContext cxt, Bytes *b, const void *data, Size len)
{
	bytes_reserve(cxt, b, len);
	if (len > 0)
		memcpy(b->data + b->len, data, len);
	b->len += len;
}

static inline void
bytes_zero(MemoryContext cxt, Bytes *b, Size len)
{
	bytes_reserve(cxt, b, len);
	memset(b->data + b->len, 0, len);
	b->len += len;
}

ChVectorBuilder *
ch_builder_create(ChTable *table, MemoryContext cxt)
{
	ChVectorBuilder *b = MemoryContextAllocZero(cxt, sizeof(ChVectorBuilder));
	int			n = 0;

	b->table = table;
	b->cxt = cxt;
	for (int i = 0; i < table->natts; i++)
		if (table->columns[i].kind != CH_COL_DROPPED)
			n++;
	b->ncols = n + 1;
	b->cols = MemoryContextAllocZero(cxt, sizeof(BuilderColumn) * b->ncols);
	b->attnums = MemoryContextAllocZero(cxt, sizeof(int) * b->ncols);
	b->vectors = MemoryContextAllocZero(cxt, sizeof(chte_vector) * b->ncols);
	n = 0;
	for (int i = 0; i < table->natts; i++)
	{
		if (table->columns[i].kind == CH_COL_DROPPED)
			continue;
		b->cols[n].col = &table->columns[i];
		b->attnums[n] = i;
		n++;
	}
	b->attnums[n] = -1;
	ch_builder_reset(b);
	return b;
}

void
ch_builder_reset(ChVectorBuilder *b)
{
	for (int c = 0; c < b->ncols; c++)
	{
		BuilderColumn *bc = &b->cols[c];

		bc->values.len = 0;
		bc->nulls.len = 0;
		bc->offsets.len = 0;
		bc->has_nulls = false;
		if (bc->col && (bc->col->kind == CH_COL_VARLENA || bc->col->kind == CH_COL_CSTRING))
		{
			uint64		zero = 0;

			bytes_append(b->cxt, &bc->offsets, &zero, sizeof(zero));
		}
	}
	b->nrows = 0;
}

/* numeric to the scaled integer of a Decimal(p, s) */
static void
numeric_to_scaled(const ChColumn *col, Datum value, void *out)
{
	Numeric		num = DatumGetNumeric(value);
	int128		result = 0;
	bool		negative = false;
	char	   *text;
	char	   *p;
	int			frac = 0;
	bool		in_frac = false;

	if (numeric_is_nan(num))
	{
		if (col->width == 4)
			*(int32 *) out = PG_INT32_MAX;
		else if (col->width == 8)
			*(int64 *) out = PG_INT64_MAX;
		else
			*(int128 *) out = ((int128) PG_INT64_MAX << 64) | (uint64) PG_UINT64_MAX;
		return;
	}
	if (numeric_is_inf(num))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("infinity cannot be stored in a numeric(%d, %d) column", col->precision, col->scale)));

	/* the column's typmod has rounded the value to the scale already */
	text = DatumGetCString(DirectFunctionCall1(numeric_out, value));
	for (p = text; *p; p++)
	{
		if (*p == '-')
			negative = true;
		else if (*p == '.')
			in_frac = true;
		else if (*p >= '0' && *p <= '9')
		{
			if (in_frac)
			{
				if (frac >= col->scale)
					break;
				frac++;
			}
			result = result * 10 + (*p - '0');
		}
	}
	for (; frac < col->scale; frac++)
		result *= 10;
	if (negative)
		result = -result;
	pfree(text);

	if (col->width == 4)
		*(int32 *) out = (int32) result;
	else if (col->width == 8)
		*(int64 *) out = (int64) result;
	else
		memcpy(out, &result, sizeof(int128));
}

/* A Decimal's scaled integer as numeric */
static Datum
scaled_to_numeric(const ChColumn *col, const void *in)
{
	int128		v;
	bool		negative;
	char		digits[64];
	char		text[72];
	int			n = 0;
	int			pos = 0;

	if (col->width == 4)
	{
		v = *(const int32 *) in;
		if (v == PG_INT32_MAX)
			return DirectFunctionCall3(numeric_in, CStringGetDatum("NaN"), ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(col->typmod));
	}
	else if (col->width == 8)
	{
		v = *(const int64 *) in;
		if (v == PG_INT64_MAX)
			return DirectFunctionCall3(numeric_in, CStringGetDatum("NaN"), ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(col->typmod));
	}
	else
	{
		memcpy(&v, in, sizeof(int128));
		if (v == (((int128) PG_INT64_MAX << 64) | (uint64) PG_UINT64_MAX))
			return DirectFunctionCall3(numeric_in, CStringGetDatum("NaN"), ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(col->typmod));
	}
	negative = v < 0;
	if (negative)
		v = -v;
	do
	{
		digits[n++] = '0' + (int) (v % 10);
		v /= 10;
	} while (v > 0 || n <= col->scale);
	if (negative)
		text[pos++] = '-';
	for (int i = n - 1; i >= 0; i--)
	{
		text[pos++] = digits[i];
		if (i == col->scale && col->scale > 0)
			text[pos++] = '.';
	}
	text[pos] = '\0';
	return DirectFunctionCall3(numeric_in, CStringGetDatum(text), ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(col->typmod));
}

static void
append_value(ChVectorBuilder *b, BuilderColumn *bc, Datum value, bool isnull)
{
	const ChColumn *col = bc->col;
	uint8		null = isnull ? 1 : 0;

	bytes_append(b->cxt, &bc->nulls, &null, 1);
	if (isnull)
		bc->has_nulls = true;

	switch (col->kind)
	{
		case CH_COL_FIXED:
			if (isnull)
				bytes_zero(b->cxt, &bc->values, col->width);
			else if (col->typbyval)
			{
				char		buf[8];

				switch (col->width)
				{
					case 1:
						buf[0] = DatumGetChar(value);
						break;
					case 2:
						*(int16 *) buf = DatumGetInt16(value);
						break;
					case 4:
						*(int32 *) buf = DatumGetInt32(value);
						break;
					default:
						*(int64 *) buf = DatumGetInt64(value);
						break;
				}
				if (col->typid == BOOLOID)
					buf[0] = DatumGetBool(value) ? 1 : 0;
				bytes_append(b->cxt, &bc->values, buf, col->width);
			}
			else
				bytes_append(b->cxt, &bc->values, DatumGetPointer(value), col->width);
			break;
		case CH_COL_NUMERIC:
			if (isnull)
				bytes_zero(b->cxt, &bc->values, col->width);
			else
			{
				char		buf[16];

				numeric_to_scaled(col, value, buf);
				bytes_append(b->cxt, &bc->values, buf, col->width);
			}
			break;
		case CH_COL_SIGN:
			{
				int64		v = 0;
				int8		b8;

				if (!isnull)
					v = col->typid == INT2OID ? DatumGetInt16(value) :
						col->typid == INT4OID ? DatumGetInt32(value) : DatumGetInt64(value);
				if (v < PG_INT8_MIN || v > PG_INT8_MAX)
					ereport(ERROR,
							(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							 errmsg("sign value " INT64_FORMAT " is out of range", v),
							 errhint("A collapsing engine's sign is 1 or -1.")));
				b8 = (int8) v;
				bytes_append(b->cxt, &bc->values, &b8, 1);
			}
			break;
		case CH_COL_VARLENA:
		case CH_COL_CSTRING:
			{
				uint64		end;

				if (!isnull)
				{
					if (col->kind == CH_COL_VARLENA)
					{
						struct varlena *v = (struct varlena *) DatumGetPointer(value);
						struct varlena *d = pg_detoast_datum_packed(v);

						bytes_append(b->cxt, &bc->values, VARDATA_ANY(d), VARSIZE_ANY_EXHDR(d));
						if (d != v)
							pfree(d);
					}
					else
					{
						const char *s = DatumGetCString(value);

						bytes_append(b->cxt, &bc->values, s, strlen(s));
					}
				}
				end = bc->values.len;
				bytes_append(b->cxt, &bc->offsets, &end, sizeof(end));
			}
			break;
		case CH_COL_DROPPED:
			break;
	}
}

void
ch_builder_add(ChVectorBuilder *b, TupleTableSlot *slot, uint64 rowid)
{
	slot_getallattrs(slot);
	for (int c = 0; c < b->ncols; c++)
	{
		BuilderColumn *bc = &b->cols[c];
		int			att = b->attnums[c];

		if (att < 0)
			bytes_append(b->cxt, &bc->values, &rowid, sizeof(rowid));
		else
			append_value(b, bc, slot->tts_values[att], slot->tts_isnull[att]);
	}
	b->nrows++;
}

uint32
ch_builder_rows(ChVectorBuilder *b)
{
	return b->nrows;
}

Size
ch_builder_bytes(ChVectorBuilder *b)
{
	Size		total = 0;

	for (int c = 0; c < b->ncols; c++)
		total += b->cols[c].values.len + b->cols[c].nulls.len + b->cols[c].offsets.len;
	return total;
}

chte_vector *
ch_builder_vectors(ChVectorBuilder *b)
{
	for (int c = 0; c < b->ncols; c++)
	{
		BuilderColumn *bc = &b->cols[c];
		chte_vector *v = &b->vectors[c];

		v->values = bc->values.data ? bc->values.data : "";
		v->offsets = bc->col && (bc->col->kind == CH_COL_VARLENA || bc->col->kind == CH_COL_CSTRING) ?
			(const uint64_t *) bc->offsets.data : NULL;
		v->nulls = bc->has_nulls ? (const uint8_t *) bc->nulls.data : NULL;
	}
	return b->vectors;
}

uint64
ch_builder_rowid(ChVectorBuilder *b, uint32 row)
{
	return ((const uint64 *) b->cols[b->ncols - 1].values.data)[row];
}

/*
 * A buffered row back into a slot, for a fetch by TID before the row is in a
 * part (§2.7 rule 3).  The values are copied into the slot's own memory.
 */
bool
ch_builder_fetch(ChVectorBuilder *b, uint32 row, TupleTableSlot *slot)
{
	chte_vector *vectors = ch_builder_vectors(b);
	MemoryContext old;

	ExecClearTuple(slot);
	old = MemoryContextSwitchTo(slot->tts_mcxt);
	for (int c = 0; c < b->ncols - 1; c++)
	{
		int			att = b->attnums[c];

		ch_vector_to_datum(b->cols[c].col, &vectors[c], row, &slot->tts_values[att],
						   &slot->tts_isnull[att], NULL);
	}
	for (int i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		if (b->table->columns[i].kind == CH_COL_DROPPED)
		{
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	MemoryContextSwitchTo(old);
	ExecStoreVirtualTuple(slot);
	ExecMaterializeSlot(slot);
	return true;
}

/*
 * One value of a column the engine library read, as a datum.  By-reference
 * values are built in the current memory context.
 */
void
ch_vector_to_datum(const ChColumn *col, const chte_vector *v, uint32 row, Datum *value, bool *isnull, char **arena)
{
	if (v->nulls && v->nulls[row])
	{
		*isnull = true;
		*value = (Datum) 0;
		return;
	}
	*isnull = false;
	switch (col->kind)
	{
		case CH_COL_FIXED:
			{
				const char *p = (const char *) v->values + (Size) row * col->width;

				if (col->typbyval)
				{
					if (col->typid == BOOLOID)
						*value = BoolGetDatum(*p != 0);
					else
						*value = fetch_att(p, true, col->width);
				}
				else
				{
					char	   *copy = palloc(col->width);

					memcpy(copy, p, col->width);
					*value = PointerGetDatum(copy);
				}
			}
			break;
		case CH_COL_NUMERIC:
			*value = scaled_to_numeric(col, (const char *) v->values + (Size) row * col->width);
			break;
		case CH_COL_SIGN:
			{
				int8		b8 = ((const int8 *) v->values)[row];

				*value = col->typid == INT2OID ? Int16GetDatum(b8) :
					col->typid == INT4OID ? Int32GetDatum(b8) : Int64GetDatum(b8);
			}
			break;
		case CH_COL_VARLENA:
			{
				uint64		start = v->offsets[row];
				uint64		len = v->offsets[row + 1] - start;
				struct varlena *d = palloc(VARHDRSZ + len);

				SET_VARSIZE(d, VARHDRSZ + len);
				memcpy(VARDATA(d), (const char *) v->values + start, len);
				*value = PointerGetDatum(d);
			}
			break;
		case CH_COL_CSTRING:
			{
				uint64		start = v->offsets[row];
				uint64		len = v->offsets[row + 1] - start;
				char	   *s = palloc(len + 1);

				memcpy(s, (const char *) v->values + start, len);
				s[len] = '\0';
				*value = CStringGetDatum(s);
			}
			break;
		case CH_COL_DROPPED:
			*isnull = true;
			*value = (Datum) 0;
			break;
	}
}
