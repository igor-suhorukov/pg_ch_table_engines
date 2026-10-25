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
#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

#include "chte.h"
#include "vexec/vexec_sink.h"

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
	bool		text;			/* text, varchar or bpchar: its values checked as a part is written (F5) */
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
		if (table->columns[i].kind == CH_COL_VARLENA)
		{
			Oid			base = getBaseType(table->columns[i].typid);

			b->cols[n].text = base == TEXTOID || base == VARCHAROID || base == BPCHAROID;
		}
		b->attnums[n] = i;
		n++;
	}
	b->attnums[n] = -1;
	ch_builder_reset(b);
	return b;
}

/*
 * F5 (pg_accel's pg_vector_executor.md §3.28): whether len bytes of UTF8
 * values are the encoding's whole, checked at once by PostgreSQL's
 * verifier -- no NUL, no truncated or invalid character -- a gigabyte at a
 * time, each piece ending where a character begins.
 */
static bool
utf8_bytes_whole(const char *p, Size len)
{
	while (len > 0)
	{
		Size		n = Min(len, (Size) 1 << 30);

		while (n < len && n > 0 && ((unsigned char) p[n] & 0xC0) == 0x80)
			n--;
		if (n == 0 || !pg_verify_mbstr(PG_UTF8, p, (int) n, true))
			return false;
		p += n;
		len -= n;
	}
	return true;
}

/*
 * F5: whether the builder's text is each value the database encoding's
 * whole, so that its part's text is vouched for to vexec (ChPart's
 * text_checked) and no scan of it checks them again.  In UTF8 a column's
 * bytes at once, as vexec's text kernels check a batch's
 * (vexec_text_batch_whole()), and no value beginning at a continuation
 * byte, inside a character; in another encoding a value at a time.
 */
bool
ch_builder_text_whole(ChVectorBuilder *b)
{
	int			enc = GetDatabaseEncoding();

	for (int c = 0; c < b->ncols; c++)
	{
		BuilderColumn *bc = &b->cols[c];
		const uint64 *ends = (const uint64 *) bc->offsets.data;
		const char *data = bc->values.data;
		uint64		prev = 0;

		if (!bc->text || b->nrows == 0)
			continue;
		if (enc == PG_UTF8)
		{
			uint64		last = ends[b->nrows - 1];

			if (!utf8_bytes_whole(data, last))
				return false;
			for (uint32 i = 0; i + 1 < b->nrows; i++)
				if (ends[i] < last && ((unsigned char) data[ends[i]] & 0xC0) == 0x80)
					return false;
			continue;
		}
		for (uint32 i = 0; i < b->nrows; prev = ends[i++])
			if (!pg_verify_mbstr(enc, data + prev, (int) (ends[i] - prev), true))
				return false;
	}
	return true;
}

/*
 * F5: the same of a sink's batch of nrows rows, its text in the layouts the
 * sink took (sink_supports(), vexec.c): offsets, a column's bytes at once
 * in UTF8; Datums, a value at a time.  A NULL row's bytes among the offsets
 * are checked too.
 */
bool
ch_builder_batch_text_whole(ChVectorBuilder *b, const VexecColumn *columns, uint32 nrows)
{
	int			enc = GetDatabaseEncoding();

	for (int c = 0; c < b->ncols; c++)
	{
		const VexecColumn *vc;

		if (!b->cols[c].text || nrows == 0)
			continue;
		vc = &columns[b->attnums[c]];
		if (vc->encoding != VEXEC_FLAT)
			return false;
		if (vc->layout == VEXEC_OFFSETS)
		{
			const int32 *off = vc->values;
			const char *data = vc->buffers[0];

			if (enc == PG_UTF8)
			{
				if (!utf8_bytes_whole(data + off[0], (Size) (off[nrows] - off[0])))
					return false;
				for (uint32 i = 1; i < nrows; i++)
					if (off[i] < off[nrows] && ((unsigned char) data[off[i]] & 0xC0) == 0x80)
						return false;
				continue;
			}
			for (uint32 i = 0; i < nrows; i++)
				if (!pg_verify_mbstr(enc, data + off[i], off[i + 1] - off[i], true))
					return false;
		}
		else if (vc->layout == VEXEC_DATUM)
		{
			const Datum *datums = vc->values;

			for (uint32 i = 0; i < nrows; i++)
			{
				struct varlena *v;

				if (vc->validity != NULL && !(vc->validity[i / 64] & (UINT64CONST(1) << (i % 64))))
					continue;
				v = (struct varlena *) DatumGetPointer(datums[i]);
				if (!pg_verify_mbstr(enc, VARDATA_ANY(v), (int) VARSIZE_ANY_EXHDR(v), true))
					return false;
			}
		}
		else
			return false;
	}
	return true;
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

/* The builder's columns, the row id's among them: a writer's, in this order */
int
ch_builder_ncolumns(ChVectorBuilder *b)
{
	return b->ncols;
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
	return ch_builder_fetch_vectors(b, ch_builder_vectors(b), row, slot);
}

/*
 * A row of vectors in the builder's column order -- its own, or a writer's
 * (VC18) -- into a slot, as ch_builder_fetch() makes it
 */
bool
ch_builder_fetch_vectors(ChVectorBuilder *b, const chte_vector *vectors, uint32 row, TupleTableSlot *slot)
{
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

/* A LowCardinality column's index at a row of its batch (chte_engine.h) */
uint64
ch_dictionary_index(const chte_dictionary *d, uint32 row)
{
	switch (d->index_width)
	{
		case 1:
			return ((const uint8 *) d->indexes)[row];
		case 2:
			return ((const uint16 *) d->indexes)[row];
		case 4:
			return ((const uint32 *) d->indexes)[row];
		default:
			return ((const uint64 *) d->indexes)[row];
	}
}

/* One value of a LowCardinality column, as ch_vector_to_datum() gives it */
void
ch_dictionary_to_datum(const ChColumn *col, const chte_dictionary *d, uint32 row, Datum *value, bool *isnull)
{
	uint64		index = ch_dictionary_index(d, row);

	if (index >= d->nvalues)
		elog(ERROR, "a LowCardinality column's index " UINT64_FORMAT " is past its dictionary's " UINT64_FORMAT " values",
			 index, (uint64) d->nvalues);
	if (d->has_null && index == d->null_index)
	{
		*isnull = true;
		*value = (Datum) 0;
		return;
	}
	ch_vector_to_datum(col, &d->values, (uint32) index, value, isnull, NULL);
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

/* ---- a batch by column, from vexec's sink (vexec.c) ---- */

static inline bool
column_valid(const VexecColumn *vc, uint32 i)
{
	return vc->validity == NULL || (vc->validity[i / 64] >> (i % 64)) & 1;
}

/* The batch's NULLs as the builder keeps them, a byte a row; whether there was one */
static bool
append_nulls(ChVectorBuilder *b, BuilderColumn *bc, const VexecColumn *vc, uint32 nrows)
{
	uint8	   *nulls;
	bool		any = false;

	bytes_reserve(b->cxt, &bc->nulls, nrows);
	nulls = (uint8 *) bc->nulls.data + bc->nulls.len;
	for (uint32 i = 0; i < nrows; i++)
	{
		nulls[i] = column_valid(vc, i) ? 0 : 1;
		any |= nulls[i];
	}
	bc->nulls.len += nrows;
	bc->has_nulls |= any;
	return any;
}

/* Fixed-width values, at their stride, of width bytes each; NULL rows zero */
static void
append_fixed(ChVectorBuilder *b, BuilderColumn *bc, const VexecColumn *vc, uint32 nrows, int width,
			 bool nulls)
{
	char	   *dst;
	const char *src = vc->values;

	bytes_reserve(b->cxt, &bc->values, (Size) nrows * width);
	dst = bc->values.data + bc->values.len;
	if (vc->stride == width)
		memcpy(dst, src, (Size) nrows * width);
	else
		for (uint32 i = 0; i < nrows; i++)
			memcpy(dst + (Size) i * width, src + (Size) i * vc->stride, width);
	if (nulls)
		for (uint32 i = 0; i < nrows; i++)
			if (!column_valid(vc, i))
				memset(dst + (Size) i * width, 0, width);
	bc->values.len += (Size) nrows * width;
}

/*
 * VC18: a sink's batch -- nrows rows of the relation's attributes, a column
 * each in the layout the sink took it in (vexec.c) -- and their row ids, as
 * the engine library takes them into ClickHouse's columns
 * (chte_write_columns()), in the builder's column order: the values, and
 * text's offsets, where the batch holds them, read by the library alone; a
 * collapsing sign narrowed to Int8 and a varlena that is not text made
 * bytes and offsets in cxt, as ch_builder_add_columns() makes them.
 * Returns the bytes they hold, as the builder counts its own.
 */
Size
ch_builder_columns_in(ChVectorBuilder *b, const VexecColumn *columns, uint32 nrows, const uint64 *rowids,
					  MemoryContext cxt, chte_column_in *out)
{
	MemoryContext old = MemoryContextSwitchTo(cxt);
	Size		bytes = 0;

	for (int c = 0; c < b->ncols; c++)
	{
		const ChColumn *col = b->cols[c].col;
		chte_column_in *in = &out[c];
		const VexecColumn *vc;

		memset(in, 0, sizeof(chte_column_in));
		if (b->attnums[c] < 0)
		{
			in->values = rowids;
			in->width = in->stride = sizeof(uint64);
			bytes += sizeof(uint64) * nrows;
			continue;
		}
		vc = &columns[b->attnums[c]];
		in->validity = vc->validity;
		bytes += nrows;			/* a null map's byte a row */
		switch (col->kind)
		{
			case CH_COL_FIXED:
			case CH_COL_NUMERIC:
				in->values = vc->values;
				in->width = (uint32) vc->width;
				in->stride = (uint32) vc->stride;
				bytes += (Size) col->width * nrows;
				break;
			case CH_COL_SIGN:
				{
					int8	   *signs = palloc(Max(nrows, 1));

					for (uint32 i = 0; i < nrows; i++)
					{
						const char *p = (const char *) vc->values + (Size) i * vc->stride;
						int64		v = 0;

						if (column_valid(vc, i))
							v = vc->width == 2 ? *(const int16 *) p : vc->width == 4 ? *(const int32 *) p :
								*(const int64 *) p;
						if (v < PG_INT8_MIN || v > PG_INT8_MAX)
							ereport(ERROR,
									(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
									 errmsg("sign value " INT64_FORMAT " is out of range", v),
									 errhint("A collapsing engine's sign is 1 or -1.")));
						signs[i] = (int8) v;
					}
					in->values = signs;
					in->width = in->stride = 1;
					bytes += nrows;
				}
				break;
			case CH_COL_VARLENA:
				if (vc->layout == VEXEC_OFFSETS)
				{
					const int32 *off = vc->values;

					in->values = vc->buffers[0];
					in->offsets32 = off;
					bytes += (Size) (off[nrows] - off[0]) + sizeof(uint64) * nrows;
				}
				else
				{
					/* DATUM: headered values, never external or compressed (vexec_sink.h) */
					const Datum *datums = vc->values;
					uint64	   *offsets = palloc(sizeof(uint64) * (nrows + 1));
					StringInfoData data;

					initStringInfo(&data);
					offsets[0] = 0;
					for (uint32 i = 0; i < nrows; i++)
					{
						if (column_valid(vc, i))
						{
							struct varlena *v = (struct varlena *) DatumGetPointer(datums[i]);

							appendBinaryStringInfo(&data, VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
						}
						offsets[i + 1] = data.len;
					}
					in->values = data.data;
					in->offsets64 = offsets;
					bytes += data.len + sizeof(uint64) * nrows;
				}
				break;
			case CH_COL_CSTRING:
			case CH_COL_DROPPED:
				elog(ERROR, "a column of kind %d cannot be taken from a batch", (int) col->kind);
				break;
		}
	}
	MemoryContextSwitchTo(old);
	return bytes;
}

/*
 * nrows rows of the relation's attributes, a column each in the layout the
 * sink took it in (vexec.c), and their row ids: the batch appended column by
 * column, as ch_builder_add() appends a row.
 */
void
ch_builder_add_columns(ChVectorBuilder *b, const VexecColumn *columns, uint32 nrows, const uint64 *rowids)
{
	for (int c = 0; c < b->ncols; c++)
	{
		BuilderColumn *bc = &b->cols[c];
		const ChColumn *col = bc->col;
		const VexecColumn *vc;
		bool		nulls;

		if (b->attnums[c] < 0)
		{
			bytes_append(b->cxt, &bc->values, rowids, sizeof(uint64) * nrows);
			continue;
		}
		vc = &columns[b->attnums[c]];
		nulls = append_nulls(b, bc, vc, nrows);
		switch (col->kind)
		{
			case CH_COL_FIXED:
				append_fixed(b, bc, vc, nrows, col->width, nulls);
				break;
			case CH_COL_NUMERIC:
				if (vc->width == col->width)
					append_fixed(b, bc, vc, nrows, col->width, nulls);
				else
				{
					/* a numeric(p, s) with p <= 9 is a Decimal32: int64 scaled values narrowed */
					const int64 *src = vc->values;
					int32	   *dst;

					Assert(vc->width == 8 && col->width == 4);
					bytes_reserve(b->cxt, &bc->values, sizeof(int32) * nrows);
					dst = (int32 *) (bc->values.data + bc->values.len);
					for (uint32 i = 0; i < nrows; i++)
						dst[i] = column_valid(vc, i) ? (int32) src[i] : 0;
					bc->values.len += sizeof(int32) * nrows;
				}
				break;
			case CH_COL_SIGN:
				for (uint32 i = 0; i < nrows; i++)
				{
					const char *p = (const char *) vc->values + (Size) i * vc->stride;
					int64		v = 0;
					int8		b8;

					if (column_valid(vc, i))
						v = vc->width == 2 ? *(const int16 *) p : vc->width == 4 ? *(const int32 *) p :
							*(const int64 *) p;
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
				if (vc->layout == VEXEC_OFFSETS)
				{
					const int32 *off = vc->values;
					const char *bytes = vc->buffers[0];
					uint64		base = bc->values.len;
					uint64	   *dst;

					bytes_append(b->cxt, &bc->values, bytes + off[0], off[nrows] - off[0]);
					bytes_reserve(b->cxt, &bc->offsets, sizeof(uint64) * nrows);
					dst = (uint64 *) (bc->offsets.data + bc->offsets.len);
					for (uint32 i = 0; i < nrows; i++)
						dst[i] = base + (uint64) (off[i + 1] - off[0]);
					bc->offsets.len += sizeof(uint64) * nrows;
				}
				else
				{
					/* DATUM: headered values, never external or compressed (vexec_sink.h) */
					const Datum *datums = vc->values;

					for (uint32 i = 0; i < nrows; i++)
					{
						uint64		end;

						if (column_valid(vc, i))
						{
							struct varlena *v = (struct varlena *) DatumGetPointer(datums[i]);

							bytes_append(b->cxt, &bc->values, VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
						}
						end = bc->values.len;
						bytes_append(b->cxt, &bc->offsets, &end, sizeof(end));
					}
				}
				break;
			case CH_COL_CSTRING:
			case CH_COL_DROPPED:
				elog(ERROR, "a column of kind %d cannot be taken from a batch", (int) col->kind);
				break;
		}
	}
	b->nrows += nrows;
}
