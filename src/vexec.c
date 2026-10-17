/*-------------------------------------------------------------------------
 *
 * vexec.c
 *	  vexec's batch source and batch sink for the tables of the engines
 *	  (ch_table_engines.md §3, §2.16; the contracts are pg_accel's
 *	  vexec_source.h and vexec_sink.h, copied into include/vexec/).
 *
 *	  The source.  vexec begins the scan itself, through table_beginscan(),
 *	  and hands it to begin(): the batches come from the scan of scan.c, its
 *	  parts, units and parallel share, the columns vexec needs set on it and
 *	  its pruning quals as a filter, so that ClickHouse reads only those
 *	  columns' granules that the key and the parts' statistics keep.  An
 *	  engine library batch -- a granule -- is cut into batches of max_rows
 *	  rows, each pointing into the library's columns where vexec's layout is
 *	  ClickHouse's:
 *
 *		a ClickHouse column		vexec's layout				copies
 *		integers, floats,		FIXED, the type's width		none
 *		 dates, timestamps
 *		Bool					BYTE_BOOL					none
 *		FixedString (uuid, ...)	FIXED, its width			none
 *		Decimal32				SCALED, widened to 8 bytes	the values
 *		Decimal64, Decimal128	SCALED						none
 *		 -- with a NaN			DATUM						every value
 *		String					VIEW where asked, else		the views, or
 *								OFFSETS						int32 offsets
 *		a collapsing sign		FIXED, widened				the values
 *		a column a part lacks	CONST, its missing value	one value
 *
 *	  a Nullable's null map as validity bits, and the row ids as TIDs.  A
 *	  batch's arrays are written over by the next batch; one vexec retains
 *	  keeps them and the library's columns it points into until released,
 *	  retained before the next call of next(), as vexec's contract has it.
 *
 *	  aggregate() answers a unit from its part's statistics -- rows from the
 *	  catalog, NULLs, least and greatest values from ClickHouse's "basic"
 *	  statistics, exact (filter.c) -- where the unit is the whole part, and
 *	  count(*) of any unit, from its granules' rows; sums are left to next().
 *	  set_keys() and the spec's keys pass over a unit whose part's
 *	  statistics show no row within them.  estimate() is the needed columns'
 *	  compressed bytes in the granules the quals keep.
 *
 *	  The sink.  VecInsert's batches go into the write buffer (buffer.c) by
 *	  column, in the layouts ClickHouse's columns have, as rows of the same
 *	  command id: FIXED for fixed-width types, BYTE_BOOL for bool, SCALED
 *	  for numeric(p, s), OFFSETS for text, varchar, bpchar and bytea, DATUM
 *	  for any other varlena; a cstring column takes no batch.  Each row gets
 *	  its row id, its TID, as a row through tuple_insert gets it, and the
 *	  statement's end makes the part.
 *
 *	  Both are registered while the postmaster preloads the library; without
 *	  vexec they are unused entries of its registries.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "varatt.h"

#include "vexec/vexec_sink.h"
#include "vexec/vexec_source.h"

#include "chte.h"
#include "scan.h"

/* A view of Arrow's binary view layout, 16 bytes */
typedef union ChView
{
	struct
	{
		int32		size;
		char		data[12];
	}			inlined;
	struct
	{
		int32		size;
		char		prefix[4];
		int32		buffer_index;
		int32		offset;
	}			ref;
	int64		align;
} ChView;

#define CH_VIEW_INLINE 12

/* A column the source hands */
typedef struct SrcColumn
{
	AttrNumber	attnum;
	const ChColumn *col;
	Oid			basetype;
	uint8		want;			/* the layout the format asks for */
	VexecColumn missing;		/* the column's missing value, as a constant */
} SrcColumn;

/* One batch's arrays; a retained one keeps the library's columns too */
typedef struct SrcOut
{
	MemoryContext cxt;
	VexecColumn *columns;
	ItemPointerData *tids;
	bool		retained;
	chte_batch *keep;
	ResourceOwner keep_owner;
} SrcOut;

typedef struct SrcState
{
	TableScanDesc scan;
	Relation	rel;
	MemoryContext cxt;
	int			ncols;
	SrcColumn  *cols;
	bool		tids;
	int			rowid_col;		/* the row id's index among the scan's columns, or -1 */
	uint32		max_rows;
	ChScanBatch batch;
	bool		have_batch;
	uint32		pos;			/* the batch's next row */
	SrcOut	   *out;			/* the next batch's arrays */
	/* the spec's keys and set_keys()'s, ANDed: units passed over by them */
	int			nspec_keys;
	ScanKeyData *spec_keys;
	int			nbound_keys;
	ScanKeyData *bound_keys;
	/* aggregate()'s by-reference answers, until the next call */
	MemoryContext answer_cxt;
} SrcState;

static uint64 null_row = 0;		/* a constant's validity: its one value NULL */

static bool unit_within_keys(void *arg, const ChScanUnit *unit);

/* The base type a column is handed as, and whether the source can hand it at all */
static bool
src_supports(Relation rel, AttrNumber attnum)
{
	ChTable    *t;

	if (attnum == SelfItemPointerAttributeNumber)
		return true;
	if (attnum < 1 || attnum > RelationGetNumberOfAttributes(rel) ||
		TupleDescAttr(RelationGetDescr(rel), attnum - 1)->attisdropped)
		return false;
	t = ch_table(rel);
	return t->columns[attnum - 1].kind != CH_COL_CSTRING &&
		t->columns[attnum - 1].kind != CH_COL_DROPPED;
}

/* The shape a column of the source's has, by kind */
static void
set_shape(VexecColumn *vc, uint8 layout, int width, int stride, int scale)
{
	vc->layout = layout;
	vc->encoding = VEXEC_FLAT;
	vc->arrow_values = false;
	vc->width = width;
	vc->stride = stride;
	vc->scale = (int16) scale;
}

static int
scaled_width(const ChColumn *col)
{
	return col->precision <= 18 ? 8 : 16;
}

/* A column's missing value as a constant of the layout the column is handed in */
static void
make_missing(SrcState *st, SrcColumn *c, TupleDesc desc)
{
	bool		isnull;
	Datum		value = getmissingattr(desc, c->attnum, &isnull);
	VexecColumn *vc = &c->missing;
	const ChColumn *col = c->col;

	memset(vc, 0, sizeof(VexecColumn));
	if (col->kind == CH_COL_FIXED && col->typbyval)
	{
		char	   *v = palloc0(Max(col->typlen, (int) sizeof(Datum)));

		if (!isnull)
			store_att_byval(v, value, col->typlen);
		set_shape(vc, c->basetype == BOOLOID ? VEXEC_BYTE_BOOL : VEXEC_FIXED, col->typlen, col->typlen, 0);
		vc->values = v;
	}
	else if (col->kind == CH_COL_FIXED || col->kind == CH_COL_SIGN)
	{
		char	   *v = palloc0(Max(col->typlen, (int) sizeof(Datum)));

		if (!isnull && col->typbyval)
			store_att_byval(v, value, col->typlen);
		else if (!isnull)
			memcpy(v, DatumGetPointer(value), col->typlen);
		set_shape(vc, VEXEC_FIXED, col->typlen, col->typlen, 0);
		vc->values = v;
	}
	else
	{
		/* numeric and varlena: the Datum itself */
		Datum	   *d = palloc(sizeof(Datum));

		*d = isnull ? (Datum) 0 : datumCopy(value, false, -1);
		set_shape(vc, VEXEC_DATUM, 0, 0, 0);
		vc->values = d;
	}
	vc->encoding = VEXEC_CONST;
	vc->validity = isnull ? &null_row : NULL;
}

static void *
src_begin(TableScanDesc scan, const VexecSourceSpec *spec)
{
	Relation	rel = scan->rs_rd;
	TupleDesc	desc = RelationGetDescr(rel);
	MemoryContext cxt;
	MemoryContext old;
	SrcState   *st;
	ChTable    *t;
	AttrNumber *attnums;
	int			n = 0;
	uint32		flags = spec->size >= offsetof(VexecSourceSpec, flags) + sizeof(spec->flags) ? spec->flags : 0;

	if (!ch_is_engine_rel(rel, NULL))
		elog(ERROR, "the batch source of the ClickHouse engines was given a scan of \"%s\"",
			 RelationGetRelationName(rel));
	cxt = AllocSetContextCreate(CurrentMemoryContext, "ch batch source", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(cxt);
	st = palloc0(sizeof(SrcState));
	st->scan = scan;
	st->rel = rel;
	st->cxt = cxt;
	st->answer_cxt = AllocSetContextCreate(cxt, "ch source answers", ALLOCSET_SMALL_SIZES);
	st->tids = (flags & VEXEC_SRC_TIDS) != 0;
	st->max_rows = spec->max_rows > 0 ? (uint32) spec->max_rows : 8192;
	t = ch_scan_table(scan);

	st->cols = palloc0(sizeof(SrcColumn) * Max(spec->ncolumns, 1));
	attnums = palloc(sizeof(AttrNumber) * Max(spec->ncolumns, 1));
	for (int i = 0; i < spec->ncolumns; i++)
	{
		SrcColumn  *c = &st->cols[n];
		AttrNumber	attnum = spec->attnums[i];

		if (attnum == SelfItemPointerAttributeNumber)
		{
			st->tids = true;
			continue;
		}
		if (!src_supports(rel, attnum))
			elog(ERROR, "the batch source of the ClickHouse engines cannot read attribute %d of \"%s\"",
				 attnum, RelationGetRelationName(rel));
		c->attnum = attnum;
		c->col = &t->columns[attnum - 1];
		c->basetype = getBaseType(c->col->typid);
		c->want = spec->layouts ? spec->layouts[i] : VEXEC_DATUM;
		make_missing(st, c, desc);
		attnums[n++] = attnum;
	}
	st->ncols = n;
	st->rowid_col = st->tids ? n : -1;
	ch_scan_set_columns(scan, n, attnums, st->tids);
	if (spec->quals != NIL)
		(void) ch_scan_set_filter(scan, spec->quals);
	if (spec->nkeys > 0)
	{
		st->nspec_keys = spec->nkeys;
		st->spec_keys = palloc(sizeof(ScanKeyData) * spec->nkeys);
		memcpy(st->spec_keys, spec->keys, sizeof(ScanKeyData) * spec->nkeys);
		ch_scan_set_unit_check(scan, unit_within_keys, st);
	}
	MemoryContextSwitchTo(old);
	return st;
}

static SrcOut *
new_out(SrcState *st)
{
	SrcOut	   *out = MemoryContextAllocZero(st->cxt, sizeof(SrcOut));

	out->cxt = AllocSetContextCreate(st->cxt, "ch source batch", ALLOCSET_DEFAULT_SIZES);
	return out;
}

/* The arrays of the next batch: the last batch's, unless vexec retained it */
static SrcOut *
next_out(SrcState *st)
{
	if (st->out == NULL || st->out->retained)
		st->out = new_out(st);
	else
		MemoryContextReset(st->out->cxt);
	st->out->columns = MemoryContextAllocZero(st->out->cxt, sizeof(VexecColumn) * Max(st->ncols, 1));
	return st->out;
}

/* A null map's rows [pos, pos + n) as validity bits, or NULL where none is NULL */
static const uint64 *
validity_of(SrcOut *out, const chte_vector *v, uint32 pos, uint32 n)
{
	uint64	   *words;
	bool		any = false;

	if (v->nulls == NULL)
		return NULL;
	words = MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((n + 63) / 64));
	for (uint32 i = 0; i < n; i++)
	{
		if (v->nulls[pos + i])
			any = true;
		else
			words[i / 64] |= UINT64CONST(1) << (i % 64);
	}
	if (!any)
	{
		pfree(words);
		return NULL;
	}
	return words;
}

/* A String column's rows as views over the library's bytes */
static void
fill_views(SrcOut *out, VexecColumn *vc, const chte_vector *v, uint32 pos, uint32 n)
{
	ChView	   *views = MemoryContextAllocZero(out->cxt, sizeof(ChView) * n);
	const char *bytes = v->values;
	const void **buffers = MemoryContextAlloc(out->cxt, sizeof(void *));
	int64	   *sizes = MemoryContextAlloc(out->cxt, sizeof(int64));
	uint64		first = v->offsets[pos];
	uint64		last = v->offsets[pos + n];

	for (uint32 i = 0; i < n; i++)
	{
		uint64		start = v->offsets[pos + i];
		uint64		len = v->offsets[pos + i + 1] - start;

		views[i].inlined.size = (int32) len;
		if (len <= CH_VIEW_INLINE)
			memcpy(views[i].inlined.data, bytes + start, len);
		else
		{
			memcpy(views[i].ref.prefix, bytes + start, 4);
			views[i].ref.buffer_index = 0;
			views[i].ref.offset = (int32) (start - first);
		}
	}
	buffers[0] = bytes + first;
	sizes[0] = (int64) (last - first);
	set_shape(vc, VEXEC_VIEW, 0, 0, 0);
	vc->values = views;
	vc->buffers = buffers;
	vc->buffer_sizes = sizes;
	vc->nbuffers = 1;
}

/* A String column's rows as Arrow's binary: int32 offsets into the library's bytes */
static bool
fill_offsets(SrcOut *out, VexecColumn *vc, const chte_vector *v, uint32 pos, uint32 n)
{
	uint64		first = v->offsets[pos];
	int32	   *offsets;
	const void **buffers;
	int64	   *sizes;

	if (v->offsets[pos + n] - first > (uint64) PG_INT32_MAX)
		return false;
	offsets = MemoryContextAlloc(out->cxt, sizeof(int32) * (n + 1));
	for (uint32 i = 0; i <= n; i++)
		offsets[i] = (int32) (v->offsets[pos + i] - first);
	buffers = MemoryContextAlloc(out->cxt, sizeof(void *));
	sizes = MemoryContextAlloc(out->cxt, sizeof(int64));
	buffers[0] = (const char *) v->values + first;
	sizes[0] = offsets[n];
	set_shape(vc, VEXEC_OFFSETS, 0, 0, 0);
	vc->values = offsets;
	vc->buffers = buffers;
	vc->buffer_sizes = sizes;
	vc->nbuffers = 1;
	return true;
}

/* Every row as a Datum, as getnextslot makes it */
static void
fill_datums(SrcOut *out, VexecColumn *vc, const ChColumn *col, const chte_vector *v, uint32 pos, uint32 n)
{
	Datum	   *datums = MemoryContextAllocZero(out->cxt, sizeof(Datum) * n);
	MemoryContext old = MemoryContextSwitchTo(out->cxt);

	for (uint32 i = 0; i < n; i++)
	{
		bool		isnull;

		ch_vector_to_datum(col, v, pos + i, &datums[i], &isnull, NULL);
	}
	MemoryContextSwitchTo(old);
	set_shape(vc, VEXEC_DATUM, 0, 0, 0);
	vc->values = datums;
}

/* Whether a Decimal column's rows [pos, pos + n) hold a NaN: the integer's greatest */
static bool
has_nan(const ChColumn *col, const chte_vector *v, uint32 pos, uint32 n)
{
	for (uint32 i = pos; i < pos + n; i++)
	{
		bool		nan;

		if (v->nulls && v->nulls[i])
			continue;
		if (col->width == 4)
			nan = ((const int32 *) v->values)[i] == PG_INT32_MAX;
		else if (col->width == 8)
			nan = ((const int64 *) v->values)[i] == PG_INT64_MAX;
		else
		{
			int128		x;

			memcpy(&x, (const char *) v->values + (Size) i * 16, 16);
			nan = x == (((int128) PG_INT64_MAX << 64) | (uint64) PG_UINT64_MAX);
		}
		if (nan)
			return true;
	}
	return false;
}

static void
fill_column(SrcState *st, SrcOut *out, int i, uint32 pos, uint32 n)
{
	SrcColumn  *c = &st->cols[i];
	const ChColumn *col = c->col;
	VexecColumn *vc = &out->columns[i];
	const chte_vector *v = &st->batch.vectors[i];

	if (st->batch.missing[i])
	{
		*vc = c->missing;
		return;
	}
	switch (col->kind)
	{
		case CH_COL_FIXED:
			set_shape(vc, c->basetype == BOOLOID ? VEXEC_BYTE_BOOL : VEXEC_FIXED, col->width, col->width, 0);
			vc->values = (const char *) v->values + (Size) pos * col->width;
			break;
		case CH_COL_SIGN:
			{
				/* Int8 widened to the column's integer */
				char	   *values = MemoryContextAlloc(out->cxt, (Size) n * col->typlen);
				const int8 *src = (const int8 *) v->values + pos;

				for (uint32 k = 0; k < n; k++)
				{
					if (col->typlen == 2)
						((int16 *) values)[k] = src[k];
					else if (col->typlen == 4)
						((int32 *) values)[k] = src[k];
					else
						((int64 *) values)[k] = src[k];
				}
				set_shape(vc, VEXEC_FIXED, col->typlen, col->typlen, 0);
				vc->values = values;
			}
			break;
		case CH_COL_NUMERIC:
			if (has_nan(col, v, pos, n))
				fill_datums(out, vc, col, v, pos, n);
			else if (col->width == 4)
			{
				int64	   *values = MemoryContextAlloc(out->cxt, sizeof(int64) * n);
				const int32 *src = (const int32 *) v->values + pos;

				for (uint32 k = 0; k < n; k++)
					values[k] = src[k];
				set_shape(vc, VEXEC_SCALED, 8, 8, col->scale);
				vc->values = values;
			}
			else
			{
				set_shape(vc, VEXEC_SCALED, col->width, col->width, col->scale);
				vc->values = (const char *) v->values + (Size) pos * col->width;
			}
			break;
		case CH_COL_VARLENA:
			if (c->want == VEXEC_VIEW)
				fill_views(out, vc, v, pos, n);
			else if (!fill_offsets(out, vc, v, pos, n))
				fill_datums(out, vc, col, v, pos, n);
			break;
		default:
			elog(ERROR, "a column of kind %d cannot be handed as a batch", (int) col->kind);
	}
	vc->validity = validity_of(out, v, pos, n);
}

/* Whether a part's statistics leave a row within the keys: false passes its units over */
static bool
keys_may_match(SrcState *st, const ChPart *part, int nkeys, const ScanKeyData *keys)
{
	ChTable    *t = ch_scan_table(st->scan);

	for (int k = 0; k < nkeys; k++)
	{
		const ScanKeyData *key = &keys[k];
		bool		has_values;
		Datum		min;
		Datum		max;
		int64		nulls;
		Datum		bound;
		bool		ok;

		if (key->sk_flags & (SK_ISNULL | SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL | SK_ROW_HEADER))
			continue;
		if (!ch_part_minmax(t, part, key->sk_attno, &has_values, &min, &max, &nulls))
			continue;
		if (!has_values)
			return false;		/* every value NULL, which no key's comparison keeps */
		switch (key->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				bound = min;
				break;
			case BTGreaterStrategyNumber:
			case BTGreaterEqualStrategyNumber:
				bound = max;
				break;
			default:
				continue;
		}
		ok = DatumGetBool(FunctionCall2Coll((FmgrInfo *) &key->sk_func, key->sk_collation, bound,
											key->sk_argument));
		if (!ok)
			return false;
	}
	return true;
}

/* The scan's check of each unit it claims: false where no row of it is within the keys */
static bool
unit_within_keys(void *arg, const ChScanUnit *unit)
{
	SrcState   *st = arg;

	return keys_may_match(st, unit->part, st->nspec_keys, st->spec_keys) &&
		keys_may_match(st, unit->part, st->nbound_keys, st->bound_keys);
}

static bool
src_next(void *arg, VexecSourceBatch *out_batch)
{
	SrcState   *st = arg;
	SrcOut	   *out;
	uint32		n;
	uint32		pos;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		if (st->have_batch && st->pos < st->batch.nrows)
			break;
		st->have_batch = false;
		if (!ch_scan_next_batch(st->scan, &st->batch))
			return false;
		st->have_batch = true;
		st->pos = 0;
	}

	pos = st->pos;
	n = Min(st->max_rows, st->batch.nrows - pos);
	st->pos += n;
	out = next_out(st);
	for (int i = 0; i < st->ncols; i++)
		fill_column(st, out, i, pos, n);
	if (st->tids)
	{
		const uint64 *ids = (const uint64 *) st->batch.vectors[st->rowid_col].values + pos;

		out->tids = MemoryContextAlloc(out->cxt, sizeof(ItemPointerData) * n);
		for (uint32 k = 0; k < n; k++)
			ItemPointerSet(&out->tids[k], CH_ROWID_TO_BLOCK(ids[k]), CH_ROWID_TO_OFFSET(ids[k]));
	}
	out_batch->nrows = (int) n;
	out_batch->visible = NULL;
	out_batch->tids = st->tids ? out->tids : NULL;
	out_batch->columns = out->columns;
	out_batch->owner = out;

	/* as getnextslot counts each row it returns */
	if (pgstat_should_count_relation(st->rel))
		st->rel->pgstat_info->counts.tuples_returned += n;
	return true;
}

static void
src_retain(void *arg, void *owner)
{
	SrcState   *st = arg;
	SrcOut	   *out = owner;

	if (out->retained)
		return;
	out->retained = true;
	if (st->have_batch && st->batch.reader)
	{
		out->keep_owner = ch_handle_reserve();
		out->keep = ch_engine()->read_retain(st->batch.reader);
		if (out->keep == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory keeping a batch of the engine library")));
		ch_handle_opened(out->keep_owner, CH_HANDLE_BATCH, out->keep);
	}
}

static void
src_release(void *arg, void *owner)
{
	SrcState   *st = arg;
	SrcOut	   *out = owner;

	if (!out->retained)
		return;
	if (out->keep)
	{
		ch_handle_closing(out->keep_owner, CH_HANDLE_BATCH, out->keep);
		ch_engine()->batch_release(out->keep);
		out->keep = NULL;
	}
	out->retained = false;
	if (out != st->out)
	{
		MemoryContextDelete(out->cxt);
		pfree(out);
	}
}

/* After table_rescan(), which began the scan again (scan.c) */
static void
src_rescan(void *arg)
{
	SrcState   *st = arg;

	st->have_batch = false;
	st->pos = 0;
	st->nbound_keys = 0;
	ch_scan_set_unit_check(st->scan, st->nspec_keys > 0 ? unit_within_keys : NULL, st);
}

/* Before table_endscan() */
static void
src_end(void *arg)
{
	SrcState   *st = arg;

	MemoryContextDelete(st->cxt);
}

/*
 * The needed columns' compressed bytes in the granules the quals keep, and
 * their rows, over the parts the snapshot sees.
 */
static void
src_estimate(Relation rel, Snapshot snapshot, const VexecSourceSpec *spec, double *rows, double *bytes)
{
	ch_estimate(rel, snapshot, spec->ncolumns, spec->attnums,
				spec->size >= offsetof(VexecSourceSpec, flags) + sizeof(spec->flags) &&
				(spec->flags & VEXEC_SRC_TIDS) != 0,
				spec->quals, rows, bytes, NULL);
}

/* An aggregate's answer from a whole part's statistics; false where there is none */
static bool
answer_from_stats(SrcState *st, const ChScanUnit *unit, const VexecSourceAgg *req, VexecSourceAggAnswer *answer)
{
	ChTable    *t = ch_scan_table(st->scan);
	Form_pg_attribute att;
	const ChColumn *col;
	bool		has_values;
	Datum		min;
	Datum		max;
	int64		nulls;
	Oid			basetype;

	if (req->kind == VEXEC_SRC_AGG_ROWS)
	{
		answer->value = Int64GetDatum((int64) unit->rows);
		answer->isnull = false;
		return true;
	}
	if (req->attnum < 1 || req->attnum > RelationGetNumberOfAttributes(st->rel))
		return false;
	att = TupleDescAttr(RelationGetDescr(st->rel), req->attnum - 1);
	col = &t->columns[req->attnum - 1];
	if (req->kind == VEXEC_SRC_AGG_COUNT && att->attnotnull)
	{
		answer->value = Int64GetDatum((int64) unit->rows);
		answer->isnull = false;
		return true;
	}
	if (!unit->whole || (req->kind != VEXEC_SRC_AGG_COUNT && req->kind != VEXEC_SRC_AGG_MIN &&
						 req->kind != VEXEC_SRC_AGG_MAX))
		return false;
	basetype = getBaseType(col->typid);
	if (req->kind != VEXEC_SRC_AGG_COUNT && req->type != att->atttypid)
		return false;
	if (!ch_part_minmax(t, unit->part, req->attnum, &has_values, &min, &max, &nulls))
	{
		/* no least and greatest kept for the column, which a count does not need */
		if (req->kind != VEXEC_SRC_AGG_COUNT)
			return false;
		if (!ch_part_nulls(t, unit->part, req->attnum, &nulls))
			return false;
		has_values = nulls < unit->rows;
	}
	(void) basetype;
	if (req->kind == VEXEC_SRC_AGG_COUNT)
	{
		answer->value = Int64GetDatum((int64) unit->rows - nulls);
		answer->isnull = false;
		return true;
	}
	if (!has_values)
	{
		answer->value = (Datum) 0;
		answer->isnull = true;
		return true;
	}
	answer->value = datumCopy(req->kind == VEXEC_SRC_AGG_MIN ? min : max, att->attbyval, att->attlen);
	answer->isnull = false;
	return true;
}

static bool
src_aggregate(void *arg, int nreqs, const VexecSourceAgg *reqs, VexecSourceAggAnswer *answers, int64 *nrows)
{
	SrcState   *st = arg;
	ChScanUnit	unit;
	MemoryContext old;
	bool		ok = true;

	if (st->have_batch && st->pos < st->batch.nrows)
		return false;			/* inside a batch: its rows come first */
	st->have_batch = false;
	if (!ch_scan_claim_unit(st->scan, &unit))
		return false;
	MemoryContextReset(st->answer_cxt);
	old = MemoryContextSwitchTo(st->answer_cxt);
	for (int i = 0; i < nreqs && ok; i++)
		ok = answer_from_stats(st, &unit, &reqs[i], &answers[i]);
	MemoryContextSwitchTo(old);
	if (!ok)
		return false;			/* the claimed unit is read by next() */
	ch_scan_pass_unit(st->scan);
	*nrows = (int64) unit.rows;
	if (pgstat_should_count_relation(st->rel))
		st->rel->pgstat_info->counts.tuples_returned += unit.rows;
	return true;
}

static void
src_set_keys(void *arg, int nkeys, const ScanKeyData *keys)
{
	SrcState   *st = arg;
	MemoryContext old = MemoryContextSwitchTo(st->cxt);

	if (st->bound_keys)
		pfree(st->bound_keys);
	st->bound_keys = nkeys > 0 ? palloc(sizeof(ScanKeyData) * nkeys) : NULL;
	if (nkeys > 0)
		memcpy(st->bound_keys, keys, sizeof(ScanKeyData) * nkeys);
	st->nbound_keys = nkeys;
	ch_scan_set_unit_check(st->scan, st->nspec_keys > 0 || nkeys > 0 ? unit_within_keys : NULL, st);
	MemoryContextSwitchTo(old);
}

/* ---- the sink ---- */

typedef struct SinkState
{
	Relation	rel;
	CommandId	cid;
	bool		tids;
	uint64	   *rowids;
	uint32		cap;
} SinkState;

static bool
sink_supports(Relation rel, AttrNumber attnum, VexecSinkLayout *layout)
{
	ChTable    *t = ch_table(rel);
	const ChColumn *col = &t->columns[attnum - 1];
	Oid			basetype = getBaseType(col->typid);

	memset(layout, 0, sizeof(VexecSinkLayout));
	switch (col->kind)
	{
		case CH_COL_FIXED:
			layout->layout = basetype == BOOLOID ? VEXEC_BYTE_BOOL : VEXEC_FIXED;
			layout->width = col->width;
			layout->stride = col->width;
			return true;
		case CH_COL_SIGN:
			layout->layout = VEXEC_FIXED;
			layout->width = col->typlen;
			layout->stride = col->typlen;
			return true;
		case CH_COL_NUMERIC:
			layout->layout = VEXEC_SCALED;
			layout->width = scaled_width(col);
			layout->stride = layout->width;
			layout->scale = (int16) col->scale;
			return true;
		case CH_COL_VARLENA:
			layout->layout = basetype == TEXTOID || basetype == VARCHAROID || basetype == BPCHAROID ||
				basetype == BYTEAOID ? VEXEC_OFFSETS : VEXEC_DATUM;
			return true;
		default:
			return false;
	}
}

static void *
sink_begin(Relation rel, const VexecSinkSpec *spec)
{
	SinkState  *st = palloc0(sizeof(SinkState));

	st->rel = rel;
	st->cid = spec->cid;
	st->tids = (spec->flags & VEXEC_SINK_TIDS) != 0;
	return st;
}

static void
sink_put(void *state, VexecSinkBatch *batch)
{
	SinkState  *st = state;
	ChVectorBuilder *builder;

	CHECK_FOR_INTERRUPTS();
	if (batch->nrows <= 0)
		return;
	if (st->cap < (uint32) batch->nrows)
	{
		st->rowids = st->rowids ? repalloc(st->rowids, sizeof(uint64) * batch->nrows) :
			palloc(sizeof(uint64) * batch->nrows);
		st->cap = batch->nrows;
	}
	builder = ch_buffer_batch_begin(st->rel, st->cid, batch->nrows, st->rowids);
	ch_builder_add_columns(builder, batch->columns, batch->nrows, st->rowids);
	if (st->tids && batch->tids)
		for (int i = 0; i < batch->nrows; i++)
			ItemPointerSet(&batch->tids[i], CH_ROWID_TO_BLOCK(st->rowids[i]), CH_ROWID_TO_OFFSET(st->rowids[i]));
	pgstat_count_heap_insert(st->rel, batch->nrows);
	ch_buffer_batch_done(st->rel);
}

/* The statement's end makes the part, as for its rows through tuple_insert (module.c) */
static void
sink_end(void *state)
{
	SinkState  *st = state;

	if (st->rowids)
		pfree(st->rowids);
	pfree(st);
}

/* ---- registration ---- */

static VexecSourceRoutine sources[CH_ENGINE_COUNT];
static VexecSinkRoutine sinks[CH_ENGINE_COUNT];

void
ch_vexec_register(void)
{
	for (int k = 0; k < CH_ENGINE_COUNT; k++)
	{
		VexecSourceRoutine *r = &sources[k];
		VexecSinkRoutine *s = &sinks[k];

		memset(r, 0, sizeof(VexecSourceRoutine));
		r->size = sizeof(VexecSourceRoutine);
		r->minor = VEXEC_SOURCE_MINOR;
		r->am = ch_am_routine((ChEngineKind) k);
		r->name = ch_engine_name((ChEngineKind) k);
		r->supports = src_supports;
		r->begin = src_begin;
		r->next = src_next;
		r->retain = src_retain;
		r->release = src_release;
		r->rescan = src_rescan;
		r->end = src_end;
		r->estimate = src_estimate;
		r->aggregate = src_aggregate;
		r->set_keys = src_set_keys;
		vexec_register_source(r);

		memset(s, 0, sizeof(VexecSinkRoutine));
		s->size = sizeof(VexecSinkRoutine);
		s->minor = VEXEC_SINK_MINOR;
		s->am = r->am;
		s->name = r->name;
		s->supports = sink_supports;
		s->begin = sink_begin;
		s->put = sink_put;
		s->end = sink_end;
		vexec_register_sink(s);
	}
}
