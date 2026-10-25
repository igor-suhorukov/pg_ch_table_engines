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
 *		LowCardinality(String)	DICT where asked (VC5): its	the codes, and
 *								dictionary as String's,		the dictionary's
 *								codes widened to int32		offsets or views
 *														once per dictionary
 *		a collapsing sign		FIXED, widened				the values
 *		a column a part lacks	CONST, its missing value	one value
 *
 *	  a Nullable's null map as validity bits, none where a batch holds no
 *	  NULL (VC11), and the row ids as TIDs.  A
 *	  batch's arrays are written over by the next batch; one vexec retains
 *	  keeps them and the library's columns it points into until released,
 *	  retained before the next call of next(), as vexec's contract has it.
 *	  A dictionary is the library's for a part's granules -- one a reader
 *	  shares between them, until it reads another -- and is made vexec's
 *	  once for all of them, named to vexec by the library's id
 *	  (dictionary_id(), the contract's minor 3).
 *
 *	  aggregate() answers a unit from its part's statistics -- rows from the
 *	  catalog, NULLs, least and greatest values from ClickHouse's "basic"
 *	  statistics, exact (filter.c) -- where the unit is the whole part, and
 *	  count(*) of any unit, from its granules' rows; sums are left to next().
 *	  set_keys() and the spec's keys pass over a unit whose part's
 *	  statistics show no row within them.  estimate() is the needed columns'
 *	  compressed bytes in the granules the quals keep, and their rows;
 *	  estimate_ex() (the contract's minor 3, VC13) adds the parts a serial
 *	  scan reads, and whether aggregate() answers every one of them, and
 *	  from minor 4 the price of a fetch by TID of the spec's columns (VC16,
 *	  fetch.c), which fetch_columns() makes a fetch into a slot read alone.
 *	  ordering() (minor 4, VC4) is the table's key, as far as its parts,
 *	  read one after another in the key's order, keep it -- one part, or
 *	  parts whose ranges of its first column are apart.  A scan begun with
 *	  VEXEC_SRC_STEPS (minor 5, VC2) reads its first step's columns with
 *	  each granule and a later step's only where next_rest() asks for it,
 *	  the engine library leaving the granules no batch asked it for
 *	  undecoded (scan.c).  set_join_keys() (minor 6, VC3) takes a hash
 *	  join's inner keys -- their range, and their values where they are
 *	  few -- into the scan's filter, ANDed with its quals, for the units it
 *	  claims from then on (filter.c), so that the parts' summaries and
 *	  statistics and the primary key pass over the parts and granules no
 *	  inner row meets.
 *
 *	  The sink.  VecInsert's batches go into the write buffer (buffer.c) by
 *	  column -- straight into the engine library's writer where it takes
 *	  them so (VC18) -- in the layouts ClickHouse's columns have, as rows of
 *	  the same command id: FIXED for fixed-width types, BYTE_BOOL for bool, SCALED
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
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "varatt.h"

#include "vexec/vexec_sink.h"
#include "vexec/vexec_source.h"

#include "chte.h"
#include "scan.h"
#include "tombstones.h"

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

/*
 * A LowCardinality column's dictionary in vexec's layout (VC5): made once for
 * the batches whose library dictionary has its id, and kept while the
 * column's batches have it and a retained batch does
 */
typedef struct SrcDict
{
	uint64		id;				/* the library's */
	int			refs;			/* the column's own, and each retained batch's */
	MemoryContext cxt;			/* the offsets or views */
	VexecColumn values;
} SrcDict;

/* A column the source hands */
typedef struct SrcColumn
{
	AttrNumber	attnum;
	const ChColumn *col;
	Oid			basetype;
	uint8		want;			/* the layout the format asks for */
	bool		dict;			/* handed as its dictionary where the library has one */
	SrcDict    *cur_dict;		/* the last batch's dictionary, or NULL */
	VexecColumn missing;		/* the column's missing value, as a constant */
} SrcColumn;

/* One batch's arrays; a retained one keeps the library's columns too */
typedef struct SrcOut
{
	MemoryContext cxt;
	VexecColumn *columns;
	SrcDict   **dicts;			/* per column: its dictionary, or NULL */
	ItemPointerData *tids;
	bool		retained;
	chte_batch *keep;
	ResourceOwner keep_owner;
	uint32		pos;			/* its rows of the library's batch */
	uint32		n;
	uint32		steps_handed;	/* VC2: a bit a step handed */
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
	/* VEXEC_SRC_ORDERED: the key's attributes this scan hands its rows in the order of (VC4) */
	int			norder;
	AttrNumber *order;
	/* VEXEC_SRC_STEPS (VC2): the steps, and each column's; 0, every column with next() */
	int			nsteps;
	int		   *col_step;
	/* W6: a claimed MergeTree unit handed in runs of granules, the next run's first */
	bool		mt_open;
	ChScanUnit	mt_unit;
	uint32		mt_next;
	uint32		mt_end;
	chte_page_file *mt_files;	/* the pages store's: the claimed part's manifest */
	size_t		mt_nfiles;
	/* F7 (minor 10, VC30): vexec's condition cache, which names the granules passed over; NULL none */
	const uint64 *(*granules_passed) (void *arg, const uint64 *unit_id, uint32 ngranules, uint32 begin,
									  uint32 end);
	void	   *granules_arg;
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

/* F7: the part's granules vexec's condition cache passes over (scan.c's skip_granules()) */
static const uint64 *
granules_skip(void *arg, const uint64 *unit_id, uint32 ngranules, uint32 begin, uint32 end)
{
	SrcState   *st = arg;

	return st->granules_passed(st->granules_arg, unit_id, ngranules, begin, end);
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

	if (!ch_is_scan_rel(rel))
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

	/*
	 * VC5: a LowCardinality column as its dictionary, where vexec welcomes
	 * dictionaries and asks for a layout of the bytes -- text's, in either
	 * format -- and the library can
	 */
	if (flags & VEXEC_SRC_DICT)
	{
		bool	   *keep = palloc0(sizeof(bool) * Max(n, 1));
		bool		any = false;

		for (int i = 0; i < n; i++)
		{
			SrcColumn  *c = &st->cols[i];

			keep[i] = c->col->low_cardinality && c->col->kind == CH_COL_VARLENA &&
				(c->want == VEXEC_OFFSETS || c->want == VEXEC_VIEW || c->want == VEXEC_DATUM);
			any |= keep[i];
		}
		if (any && ch_scan_set_dictionaries(scan, keep))
			for (int i = 0; i < n; i++)
				st->cols[i].dict = keep[i];
		pfree(keep);
	}
	/*
	 * VC4: a serial scan's parts read in the order of the table's key, where
	 * their ranges of its first column are apart -- its rows then come in
	 * that order -- or meet, the first column's alone
	 */
	if (flags & VEXEC_SRC_ORDERED)
	{
		int			k;

		st->order = palloc(sizeof(AttrNumber) * Max(t->natts, 1));
		k = ch_table_ordering(rel, t->natts, st->order);
		if (k > 0)
		{
			int			level = ch_scan_order_parts(scan, st->order[0]);

			st->norder = level == 2 ? k : level == 1 ? 1 : 0;
		}
	}
	/*
	 * VC2: the columns in the spec's steps, where the library reads steps --
	 * else every one with next(), next_rest() handing nothing more
	 */
	if ((flags & VEXEC_SRC_STEPS) &&
		spec->size >= offsetof(VexecSourceSpec, step_ends) + sizeof(spec->step_ends) &&
		spec->nsteps >= 2 && spec->nsteps <= VEXEC_SRC_MAX_STEPS && n == spec->ncolumns)
	{
		int			step = 0;

		st->col_step = palloc0(sizeof(int) * Max(n, 1));
		for (int i = 0; i < n; i++)
		{
			while (step < spec->nsteps - 1 && i >= spec->step_ends[step])
				step++;
			st->col_step[i] = step;
		}
		if (ch_scan_set_steps(scan, st->col_step, spec->nsteps))
			st->nsteps = spec->nsteps;
	}
	if (spec->quals != NIL)
		(void) ch_scan_set_filter(scan, spec->quals);
	/*
	 * F7 (minor 10, VC30): vexec's condition cache names a part's granules
	 * to pass over as the scan selects them, and learns each batch's
	 * granule, where the spec is of that size and has it
	 */
	if (spec->size >= offsetof(VexecSourceSpec, granules_arg) + sizeof(spec->granules_arg) &&
		spec->granules_passed != NULL)
	{
		st->granules_passed = spec->granules_passed;
		st->granules_arg = spec->granules_arg;
		ch_scan_set_granule_skip(scan, granules_skip, st);
	}
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
	st->out->dicts = MemoryContextAllocZero(st->out->cxt, sizeof(SrcDict *) * Max(st->ncols, 1));
	return st->out;
}

/*
 * A null map's rows [pos, pos + n) as validity bits, or NULL where none is
 * NULL (VC11).  ClickHouse's null map is a byte a row, 1 for a NULL and 0
 * for a value (ColumnNullable), so a memchr() for a 1 finds whether the
 * batch holds a NULL at all: most do not, and they need no bits.  Where one
 * does, eight bytes become a byte of bits at a time: each byte's low bit
 * inverted, and the eight gathered into the top byte of a product -- the
 * multiplier puts byte k's bit at bit 56 + k, and no two partial products
 * meet there.  A bit a row took 31% of a scan's cycles (§3, "Transfer and
 * statistics, measured 2026-10-17").
 */
static const uint64 *
validity_of(SrcOut *out, const chte_vector *v, uint32 pos, uint32 n)
{
	const uint8 *nulls;
	uint64	   *words;
	uint32		i = 0;

	if (v->nulls == NULL)
		return NULL;
	nulls = v->nulls + pos;
	if (memchr(nulls, 1, n) == NULL)
		return NULL;
	words = MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((n + 63) / 64));
#ifndef WORDS_BIGENDIAN
	/* a little-endian word's byte k holds its bits 8k to 8k + 7 */
	for (; i + 8 <= n; i += 8)
	{
		uint64		w;

		memcpy(&w, nulls + i, sizeof(w));
		w = ~w & UINT64CONST(0x0101010101010101);
		((uint8 *) words)[i / 8] = (uint8) ((w * UINT64CONST(0x0102040810204080)) >> 56);
	}
#endif
	for (; i < n; i++)
		if (!nulls[i])
			words[i / 64] |= UINT64CONST(1) << (i % 64);
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
dict_unref(SrcDict *d)
{
	if (--d->refs == 0)
	{
		MemoryContextDelete(d->cxt);
		pfree(d);
	}
}

/*
 * A library dictionary's values in vexec's layout: views where the format
 * asks for them, else int32 offsets into the library's bytes.  NULL where
 * its bytes are past int32's offsets.
 */
static SrcDict *
make_dict(SrcState *st, const SrcColumn *c, const chte_dictionary *d)
{
	SrcDict    *sd;
	SrcOut		tmp;
	bool		ok PG_USED_FOR_ASSERTS_ONLY = true;

	if (d->nvalues > (uint64) PG_INT32_MAX ||
		(c->want != VEXEC_VIEW && d->values.offsets[d->nvalues] - d->values.offsets[0] > (uint64) PG_INT32_MAX))
		return NULL;
	sd = MemoryContextAllocZero(st->cxt, sizeof(SrcDict));
	sd->id = d->id;
	sd->refs = 1;
	sd->cxt = AllocSetContextCreate(st->cxt, "ch source dictionary", ALLOCSET_DEFAULT_SIZES);
	memset(&tmp, 0, sizeof(tmp));
	tmp.cxt = sd->cxt;
	if (c->want == VEXEC_VIEW)
		fill_views(&tmp, &sd->values, &d->values, 0, (uint32) d->nvalues);
	else
		ok = fill_offsets(&tmp, &sd->values, &d->values, 0, (uint32) d->nvalues);
	Assert(ok);
	sd->values.validity = NULL;
	return sd;
}

/*
 * A LowCardinality column's rows [pos, pos + n) as DICT: their indexes
 * widened to int32 codes, a NULL row's validity bit clear and its code the
 * dictionary's NULL entry, the type's default; the dictionary made vexec's
 * once for its id.  False where it cannot be: the rows are then handed as
 * Datums.
 */
static bool
fill_dict(SrcState *st, SrcOut *out, int i, uint32 pos, uint32 n)
{
	SrcColumn  *c = &st->cols[i];
	const chte_dictionary *d = &st->batch.dicts[i];
	VexecColumn *vc = &out->columns[i];
	int32	   *codes;
	uint64	   *words;
	bool		any_null = false;

	if (c->cur_dict == NULL || c->cur_dict->id != d->id)
	{
		SrcDict    *nd = make_dict(st, c, d);

		if (nd == NULL)
			return false;
		if (c->cur_dict)
			dict_unref(c->cur_dict);
		c->cur_dict = nd;
	}
	codes = MemoryContextAlloc(out->cxt, sizeof(int32) * Max(n, 1));
	words = d->has_null ? MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((n + 63) / 64)) : NULL;
	/* the indexes widened, a loop for each of their widths */
#define WIDEN(T) \
	do { \
		const T    *ix = (const T *) d->indexes + pos; \
		uint64		most = 0; \
		for (uint32 k = 0; k < n; k++) \
		{ \
			codes[k] = (int32) ix[k]; \
			most = Max(most, (uint64) ix[k]); \
		} \
		if (most >= d->nvalues) \
			elog(ERROR, "a LowCardinality column's index " UINT64_FORMAT " is past its dictionary's " UINT64_FORMAT " values", \
				 most, (uint64) d->nvalues); \
	} while (0)
	switch (d->index_width)
	{
		case 1:
			WIDEN(uint8);
			break;
		case 2:
			WIDEN(uint16);
			break;
		case 4:
			WIDEN(uint32);
			break;
		default:
			WIDEN(uint64);
			break;
	}
#undef WIDEN
	/* a NULL row's bit cleared, a word of rows made at a time */
	if (words != NULL)
	{
		int32		null_code = (int32) d->null_index;

		for (uint32 w = 0; w < (n + 63) / 64; w++)
		{
			uint32		base = w * 64;
			uint32		end = Min(n - base, 64);
			uint64		all = end == 64 ? ~UINT64CONST(0) : (UINT64CONST(1) << end) - 1;
			uint64		word = 0;

			for (uint32 b = 0; b < end; b++)
				word |= (uint64) (codes[base + b] != null_code) << b;
			words[w] = word;
			if (word != all)
				any_null = true;
		}
	}
	if (words && !any_null)
	{
		pfree(words);
		words = NULL;
	}
	*vc = c->cur_dict->values;
	vc->encoding = VEXEC_DICT;
	vc->validity = words;
	vc->values = NULL;
	vc->buffers = NULL;
	vc->buffer_sizes = NULL;
	vc->nbuffers = 0;
	vc->datums = NULL;
	vc->codes = codes;
	vc->dictionary = &c->cur_dict->values;
	vc->ndictionary = (int) d->nvalues;
	out->dicts[i] = c->cur_dict;
	return true;
}

/* A LowCardinality column's rows [pos, pos + n) as Datums */
static void
fill_dict_datums(SrcState *st, SrcOut *out, int i, uint32 pos, uint32 n)
{
	const chte_dictionary *d = &st->batch.dicts[i];
	VexecColumn *vc = &out->columns[i];
	Datum	   *datums = MemoryContextAllocZero(out->cxt, sizeof(Datum) * Max(n, 1));
	uint64	   *words = MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((n + 63) / 64));
	bool		any_null = false;
	MemoryContext old = MemoryContextSwitchTo(out->cxt);

	for (uint32 k = 0; k < n; k++)
	{
		bool		isnull;

		ch_dictionary_to_datum(st->cols[i].col, d, pos + k, &datums[k], &isnull);
		if (isnull)
			any_null = true;
		else
			words[k / 64] |= UINT64CONST(1) << (k % 64);
	}
	MemoryContextSwitchTo(old);
	set_shape(vc, VEXEC_DATUM, 0, 0, 0);
	vc->values = datums;
	vc->validity = any_null ? words : NULL;
}

/*
 * F5 (the source contract's minor version 10): a text column's flags -- its
 * values each the encoding's whole, VEXEC_COL_TEXT_WHOLE, where the batch's
 * part's text was checked as it was written (ChPart.text_checked): rows of
 * PostgreSQL's, or a merge or fold of such parts alone.  vexec then checks
 * none of them again, as PostgreSQL checks no stored value.
 */
static uint8
text_flags(const SrcState *st, const SrcColumn *c)
{
	if (st->batch.part == NULL || !st->batch.part->text_checked ||
		(c->basetype != TEXTOID && c->basetype != VARCHAROID && c->basetype != BPCHAROID))
		return 0;
	return VEXEC_COL_TEXT_WHOLE;
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
	if (c->dict && st->batch.dicts && st->batch.dicts[i].encoded)
	{
		if (!fill_dict(st, out, i, pos, n))
			fill_dict_datums(st, out, i, pos, n);
		vc->flags = text_flags(st, c);
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
	vc->flags = text_flags(st, c);
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
	uint32		returned;

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
	returned = n;
	st->pos += n;
	out = next_out(st);
	out->pos = pos;
	out->n = n;
	out->steps_handed = 1;
	/* VC2: the first step's columns; a later step's come from next_rest() */
	for (int i = 0; i < st->ncols; i++)
		if (st->nsteps == 0 || st->col_step[i] == 0)
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
	/* deleted rows (§2.10): the batch's slice of the scan's visible bits */
	if (st->batch.visible)
	{
		uint64	   *bits = MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((n + 63) / 64));
		uint32		live = 0;

		for (uint32 k = 0; k < n; k++)
			if (st->batch.visible[(pos + k) / 64] & (UINT64CONST(1) << ((pos + k) % 64)))
			{
				bits[k / 64] |= UINT64CONST(1) << (k % 64);
				live++;
			}
		out_batch->visible = bits;
		returned = live;
	}
	out_batch->tids = st->tids ? out->tids : NULL;
	out_batch->columns = out->columns;
	out_batch->owner = out;
	/*
	 * F7 (minor 10): the part's granule the rows are of, the library's batch
	 * a granule's rows or some of them, the last slice of it the granule's
	 * last where the library's batch was
	 */
	if (st->granules_passed != NULL)
	{
		if (st->batch.granule >= 0 && st->batch.part != NULL)
			ch_part_unit_id(ch_scan_table(st->scan), st->batch.part, out_batch->unit_id);
		out_batch->granule = st->batch.granule;
		out_batch->granule_last = st->batch.granule_last && pos + n == st->batch.nrows;
	}

	/* as getnextslot counts each row it returns */
	if (pgstat_should_count_relation(st->rel))
		st->rel->pgstat_info->counts.tuples_returned += returned;
	return true;
}

/*
 * VC2: step `step`'s columns of a batch, read from the library's batch -- a
 * granule -- where no batch of it asked for them before
 */
static void
hand_step(SrcState *st, SrcOut *out, int step)
{
	if (out->steps_handed & (1U << step))
		return;
	ch_scan_read_step(st->scan, step, &st->batch);
	for (int i = 0; i < st->ncols; i++)
		if (st->col_step[i] == step)
			fill_column(st, out, i, out->pos, out->n);
	out->steps_handed |= 1U << step;
}

/*
 * next_rest() (minor 5, VC2): a later step's columns of the batch next()
 * handed last.  The library decodes a granule's columns of a step where a
 * batch of it first asks for them, for every row of it; the rows vexec
 * selects need not be looked at.
 */
static void
src_next_rest(void *arg, VexecSourceBatch *batch, int step, const uint64 *selected)
{
	SrcState   *st = arg;
	SrcOut	   *out = batch->owner;

	if (st->nsteps == 0)
		return;					/* every column came with next() */
	if (out != st->out || !st->have_batch || step < 1 || step >= st->nsteps)
		elog(ERROR, "the batch source of the ClickHouse engines was asked for step %d of a batch it no longer holds",
			 step);
	hand_step(st, out, step);
}

static void
src_retain(void *arg, void *owner)
{
	SrcState   *st = arg;
	SrcOut	   *out = owner;

	if (out->retained)
		return;
	/* VC2: the library's batch kept with every step, so that no later step is read into another */
	if (st->nsteps > 0 && out == st->out && st->have_batch)
		for (int step = 1; step < st->nsteps; step++)
			hand_step(st, out, step);
	out->retained = true;
	for (int i = 0; i < st->ncols; i++)
		if (out->dicts[i])
			out->dicts[i]->refs++;
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
	for (int i = 0; i < st->ncols; i++)
		if (out->dicts[i])
			dict_unref(out->dicts[i]);
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
	st->mt_open = false;		/* W6: no claimed unit's runs left */
	ch_scan_set_unit_check(st->scan, st->nspec_keys > 0 ? unit_within_keys : NULL, st);
	/* and the join's keys (VC3) */
	ch_scan_set_join_keys(st->scan, 0, NULL);
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

/*
 * estimate_ex() (VC13): estimate()'s rows and bytes; the parts a serial scan
 * reads, its units; and VEXEC_EST_AGG_EXACT where aggregate() answers each
 * of them from its statistics -- a scan with no qual and no TIDs, no row of
 * it deleted, and every column one whose least and greatest values a part's
 * statistics keep (answer_from_stats()).
 */
static void
src_estimate_ex(Relation rel, Snapshot snapshot, const VexecSourceSpec *spec, VexecSourceEstimate *est)
{
	bool		tids = spec->size >= offsetof(VexecSourceSpec, flags) + sizeof(spec->flags) &&
		(spec->flags & VEXEC_SRC_TIDS) != 0;
	double		rows;
	double		bytes;
	double		units;
	bool		deleted;
	uint32		flags = 0;

	ch_estimate(rel, snapshot, spec->ncolumns, spec->attnums, tids, spec->quals, &rows, &bytes, NULL);
	ch_estimate_units(rel, snapshot, &units, &deleted);
	if (spec->quals == NIL && !tids && !deleted)
	{
		ChTable    *t = ch_table(rel);
		bool		kept = true;

		for (int i = 0; i < spec->ncolumns && kept; i++)
			kept = spec->attnums[i] >= 1 && spec->attnums[i] <= t->natts &&
				ch_column_minmax_kept(&t->columns[spec->attnums[i] - 1]);
		if (kept)
			flags |= VEXEC_EST_AGG_EXACT;
	}
	est->rows = rows;
	est->bytes = bytes;
	if (est->size >= offsetof(VexecSourceEstimate, units) + sizeof(est->units))
		est->units = units;
	if (est->size >= offsetof(VexecSourceEstimate, flags) + sizeof(est->flags))
		est->flags = flags;
	/* minor 4 (VC16): a row fetched by its TID, the spec's columns decoded */
	if (est->size >= offsetof(VexecSourceEstimate, fetch) + sizeof(est->fetch) &&
		(ch_fetch_cost > 0 || ch_fetch_value_cost > 0))
		est->fetch = ch_fetch_price(rel, spec->ncolumns);
}

/*
 * fetch_columns() (minor 4, VC16): the columns a fetch by TID into the slot
 * decodes (fetch.c), the slot's others left NULL
 */
static void
src_fetch_columns(Relation rel, TupleTableSlot *slot, int n, const AttrNumber *attnums)
{
	if (ch_enable_fetch_columns)
		ch_fetch_set_columns(rel, slot, n, attnums);
}

/*
 * ordering() (minor 4, VC4): the table's key, as far as its parts, read one
 * after another, keep its order -- those the snapshot sees now, for the
 * planner; a scan's, as it began.
 */
static int
src_ordering(Relation rel, void *arg, int max, AttrNumber *attnums)
{
	SrcState   *st = arg;
	ChTable    *t;
	List	   *parts;
	List	   *ordered;
	int			k;
	int			level;

	if (st != NULL)
	{
		k = Min(st->norder, max);
		for (int i = 0; i < k; i++)
			attnums[i] = st->order[i];
		return k;
	}
	k = ch_table_ordering(rel, max, attnums);
	if (k == 0)
		return 0;
	t = ch_table(rel);
	ch_table_pin(t);
	PG_TRY();
	{
		parts = ch_parts_to_scan(&t->storage, ActiveSnapshotSet() ? GetActiveSnapshot() : NULL);
		level = ch_parts_order(rel, t, parts, attnums[0], &ordered);
	}
	PG_FINALLY();
	{
		ch_table_unpin(t);
	}
	PG_END_TRY();
	return level == 2 ? k : level == 1 ? 1 : 0;
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
	/*
	 * A unit some of whose rows may be deleted: neither its rows, which every
	 * answer reports -- count(*) asks for nothing else -- nor its statistics
	 * are its rows'.  next() reads it.
	 */
	if (unit.deletions)
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

/*
 * V8e (pg_vector_executor.md §3.25, N2): the scan's next unit for the GPU's
 * executor to read itself, between batches (minor 7).  A unit of an
 * external Parquet or Arrow table's file, of no deletion, whose columns
 * the executor finds by their names there -- a column's file name is its
 * attribute's, matched exactly by libcudf, and a name of a nested column's
 * form, with a dot, is not -- is handed so, and passed over; any other is
 * the CPU's, read by the next batches alone.  A part of a table of its own
 * files (§2.18, CH8) is handed so too, its columns c<attnum> there, its
 * rows the snapshot sees deleted as the unit's deleted bits, from its
 * rows' ids in the file.  The token keeps what read_unit() reads again:
 * the file's part and the granules' ranges.
 */
typedef struct SrcUnit
{
	const ChPart *part;
	chte_mark_range *ranges;
	int			nranges;
	uint64		rows;
} SrcUnit;

static bool
unit_for_device(SrcState *st, const ChScanUnit *unit, VexecSourceUnit *u)
{
	ChExternalKind kind;
	chte_external_file info;
	SrcUnit    *keep;
	int32	   *parts;
	int			nparts;
	const char **names;
	uint8	   *flags;
	MemoryContext old;
	ChTable    *t = ch_scan_table(st->scan);

	/* W13 (minor 9): a scan of TIDs, a table of its own files', whose rows' ids are their files' */
	if (unit->part->listing == NULL || (unit->deletions && !t->own) || unit->part->deletes ||
		!ch_is_external_rel(st->rel, &kind) || (kind != CH_EXTERNAL_PARQUET && kind != CH_EXTERNAL_ARROW) ||
		(st->tids && (!t->own || ch_engine()->external_read_rows == NULL)) || st->ncols == 0)
		return false;
	old = MemoryContextSwitchTo(st->cxt);
	names = palloc(sizeof(char *) * st->ncols);
	flags = palloc0(sizeof(uint8) * st->ncols);
	for (int i = 0; i < st->ncols; i++)
	{
		Form_pg_attribute att = TupleDescAttr(RelationGetDescr(st->rel), st->cols[i].attnum - 1);
		const char *name = NameStr(att->attname);

		/* a table of its own files' column is c<attnum> in its files (§2.18) */
		if (t->own)
			name = psprintf("c%d", att->attnum);
		else if (strchr(name, '.') != NULL)
		{
			MemoryContextSwitchTo(old);
			return false;
		}
		names[i] = name;
		if (att->attnotnull)
			flags[i] |= VEXEC_UNIT_NOT_NULL;
		if ((att->atttypid == TEXTOID || att->atttypid == VARCHAROID || att->atttypid == JSONOID) &&
			GetDatabaseEncoding() == PG_UTF8)
			flags[i] |= VEXEC_UNIT_CHECK_UTF8;
	}
	CH_CALL("listing a table's files",
			ch_engine()->external_file_info(unit->part->listing->handle, (size_t) unit->part->file, &info, CH_ERR));
	keep = palloc0(sizeof(SrcUnit));
	nparts = ch_scan_unit_parts(st->scan, &parts, &keep->ranges, &keep->nranges);
	keep->part = unit->part;
	keep->rows = unit->rows;
	MemoryContextSwitchTo(old);

	u->format = kind == CH_EXTERNAL_PARQUET ? VEXEC_UNIT_PARQUET : VEXEC_UNIT_ARROW;
	u->path = MemoryContextStrdup(st->cxt, info.path);
	u->file_size = info.size;
	u->file_mtime = info.mtime;
	u->nparts = nparts;
	u->parts = parts;
	u->rows = unit->rows;
	u->names = names;
	u->column_flags = flags;
	u->deleted = NULL;
	u->token = keep;
	/* W13 (minor 9): a scan of TIDs: its rows' ids, its file's own column, read by the device */
	if (st->tids && u->size >= offsetof(VexecSourceUnit, rowid_name) + sizeof(u->rowid_name))
		u->rowid_name = CH_ROWID_COLUMN;
	if (unit->deletions)
	{
		/* CH8: the unit's rows the snapshot sees deleted, by their ids in the part's file */
		const uint64 *dead;
		int			ndead = ch_scan_unit_dead(st->scan, &dead);
		const uint64 *ids;
		size_t		nids;
		uint64	   *bits;

		CH_CALL("reading a part's row ids",
				ch_engine()->own_unit_ids(t->handle, unit->part->listing->handle, (size_t) unit->part->file, parts,
										  (size_t) nparts, &ids, &nids, CH_ERR));
		if (nids != unit->rows)
			elog(ERROR, "a unit of %zu row ids where its part's units have " UINT64_FORMAT " rows", nids, unit->rows);
		bits = MemoryContextAllocZero(st->cxt, sizeof(uint64) * Max((nids + 63) / 64, 1));
		/* the unit's ids ascend as its file's do, the dead ones too: one walk over both */
		for (size_t r = 0, d = 0; r < nids && d < (size_t) ndead; r++)
		{
			while (d < (size_t) ndead && dead[d] < ids[r])
				d++;
			if (d < (size_t) ndead && dead[d] == ids[r])
				bits[r / 64] |= UINT64CONST(1) << (r % 64);
		}
		u->deleted = bits;
	}
	return true;
}

/*
 * W6 (pg_vector_executor.md §3.26; minor 8): granules of a MergeTree part
 * for the GPU's executor to decompress and decode itself -- a wide part's
 * columns, each stream's compressed blocks named by its file in the part,
 * whose bytes read_unit_bytes() copies, the pages store's through its
 * buffers.  A claimed unit goes in runs of at most CH_DEVICE_UNIT_MARKS
 * granules, from its ranges' first granule to their last: where its scan's
 * quals pruned some between, the device's filter of the scan's quals (W4)
 * reads them again and keeps what the quals keep.  The plain engine's
 * parts of no deletion alone; read_unit() reads a run again on the CPU.
 */
#define CH_DEVICE_UNIT_MARKS	256

typedef struct SrcMtUnit
{
	SrcUnit		unit;			/* read_unit()'s: the part and the run, first */
	chte_mark_range range;
	uint64		first_offset;	/* W13: the run's first row's offset in its part */
	int			nfiles;
	char	  (*files)[128];	/* per stream: its file, */
	uint64	   *from;			/* its first block's offset there, */
	chte_page_file *pages;		/* and, the pages store's, the file's pages; path NULL: the library's read */
} SrcMtUnit;

/*
 * The library's description of a part's columns over granules [begin, end)
 * for the device, kept for every process (streams.c, VC29): false where it
 * refuses them.  Asked of the library, the part attached first, where they
 * are not kept; valid until the next call.
 */
static bool
mergetree_streams(SrcState *st, uint32 begin, uint32 end, int ncols, const AttrNumber *attnums,
				  chte_part_streams *ps, chte_part_stream **kept)
{
	ChTable    *t = ch_scan_table(st->scan);
	const ChPart *part = st->mt_unit.part;
	size_t		nfound;
	uint64		rows;
	StringInfoData list;

	memset(ps, 0, sizeof(*ps));
	*kept = NULL;
	if (ch_streams_find(t, part, begin, end, ncols, attnums, &rows, kept, &nfound))
	{
		ps->rows = rows;
		ps->nstreams = nfound;
		ps->streams = *kept;
		return true;
	}
	initStringInfo(&list);
	for (int i = 0; i < ncols; i++)
		appendStringInfo(&list, "%sc%d", i > 0 ? ", " : "", attnums[i]);
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, part);
	CH_CALL("describing a part's columns for the device",
			ch_engine()->part_streams_of(t->handle, part->name, list.data, begin, end, ps, CH_ERR));
	pfree(list.data);
	ch_streams_described();
	if (ps->refusal != NULL)
		return false;
	ch_streams_keep(t, part, begin, end, ncols, attnums, ps);
	return true;
}

/* The claimed unit's next run of granules for the device; false, nothing set, where its part's columns are not read so. */
static bool
mergetree_unit(SrcState *st, VexecSourceUnit *u)
{
	uint32		begin = st->mt_next;
	uint32		end = Min(begin + CH_DEVICE_UNIT_MARKS, st->mt_end);
	chte_part_streams ps;
	MemoryContext old;
	SrcMtUnit  *keep;
	VexecSourceStream *streams;
	const char **names;
	uint8	   *flags;
	AttrNumber *attnums;
	chte_part_stream *kept;
	uint64		first_offset = 0;

	/*
	 * W13: a scan of TIDs: the run's first row's place in its part, the rows
	 * of the granules before it -- asked first: a call's streams are the
	 * library's until the next
	 */
	if (st->tids && begin > 0)
	{
		chte_part_streams before;

		if (!mergetree_streams(st, 0, begin, 1, &st->cols[0].attnum, &before, &kept))
			return false;
		first_offset = before.rows;
		if (kept)
			pfree(kept);
	}
	attnums = palloc(sizeof(AttrNumber) * st->ncols);
	for (int i = 0; i < st->ncols; i++)
		attnums[i] = st->cols[i].attnum;
	if (!mergetree_streams(st, begin, end, st->ncols, attnums, &ps, &kept))
	{
		pfree(attnums);
		return false;
	}
	pfree(attnums);
	old = MemoryContextSwitchTo(st->cxt);
	keep = palloc0(sizeof(SrcMtUnit));
	keep->unit.part = st->mt_unit.part;
	keep->range.begin = begin;
	keep->range.end = end;
	keep->unit.ranges = &keep->range;
	keep->unit.nranges = 1;
	keep->unit.rows = ps.rows;
	keep->first_offset = first_offset;
	keep->nfiles = (int) ps.nstreams;
	keep->files = palloc(sizeof(*keep->files) * Max(ps.nstreams, 1));
	keep->from = palloc(sizeof(uint64) * Max(ps.nstreams, 1));
	keep->pages = palloc0(sizeof(chte_page_file) * Max(ps.nstreams, 1));
	streams = palloc0(sizeof(VexecSourceStream) * Max(ps.nstreams, 1));
	for (size_t i = 0; i < ps.nstreams; i++)
	{
		const chte_part_stream *x = &ps.streams[i];

		memcpy(keep->files[i], x->file, sizeof(keep->files[i]));
		keep->from[i] = x->from;
		/* the file's pages, as the part's manifest has them, its name the stream's */
		for (size_t f = 0; f < st->mt_nfiles; f++)
			if (strcmp(st->mt_files[f].path, x->file) == 0)
			{
				keep->pages[i] = st->mt_files[f];
				keep->pages[i].path = keep->files[i];
				break;
			}
		streams[i].column = x->column;
		streams[i].kind = x->kind == CHTE_STREAM_SIZES ? VEXEC_STREAM_SIZES :
			x->kind == CHTE_STREAM_NULLS ? VEXEC_STREAM_NULLS :
			x->kind == CHTE_STREAM_SPARSE ? VEXEC_STREAM_SPARSE :
			x->kind == CHTE_STREAM_SPARSE_NULLS ? VEXEC_STREAM_SPARSE_NULLS : VEXEC_STREAM_VALUES;
		streams[i].width = x->width;
		streams[i].length = x->to - x->from;
		streams[i].skip = x->skip;
	}
	if (kept)
		pfree(kept);
	names = palloc(sizeof(char *) * st->ncols);
	flags = palloc0(sizeof(uint8) * st->ncols);
	for (int i = 0; i < st->ncols; i++)
	{
		Form_pg_attribute att = TupleDescAttr(RelationGetDescr(st->rel), st->cols[i].attnum - 1);

		names[i] = psprintf("c%d", st->cols[i].attnum);
		if (att->attnotnull)
			flags[i] |= VEXEC_UNIT_NOT_NULL;
		if ((att->atttypid == TEXTOID || att->atttypid == VARCHAROID || att->atttypid == JSONOID) &&
			GetDatabaseEncoding() == PG_UTF8)
			flags[i] |= VEXEC_UNIT_CHECK_UTF8;
	}
	u->format = VEXEC_UNIT_MERGETREE;
	/* what names the run as long as its part is: the database, the relation's storage, the part, the run */
	u->path = psprintf("mergetree/%u/%u/%s/%u-%u", MyDatabaseId, st->rel->rd_locator.relNumber,
					   st->mt_unit.part->name, begin, end);
	u->file_size = 0;
	u->file_mtime = 0;
	u->nparts = (int) (end - begin);
	{
		int32	   *parts = palloc(sizeof(int32) * (end - begin));

		for (uint32 m = begin; m < end; m++)
			parts[m - begin] = (int32) m;
		u->parts = parts;
	}
	u->rows = ps.rows;
	u->names = names;
	u->column_flags = flags;
	u->deleted = NULL;
	u->token = keep;
	u->nstreams = (int) ps.nstreams;
	u->streams = streams;
	MemoryContextSwitchTo(old);
	st->mt_next = end;
	return true;
}

/*
 * W6: whether the claimed unit goes to the device: a part of the plain
 * engine, of no deletion, read for no TID, its first run's columns ones the
 * device reads; its runs then from its ranges' first granule to their last.
 */
static bool
mergetree_begin(SrcState *st, const ChScanUnit *unit)
{
	ChTable    *t = ch_scan_table(st->scan);
	int32	   *parts;
	chte_mark_range *ranges;
	int			nranges;

	if (t->external || t->kind != CH_ENGINE_MERGETREE || ch_engine()->part_streams_of == NULL ||
		unit->deletions || st->ncols == 0)
		return false;
	for (int i = 0; i < st->ncols; i++)
		if (st->cols[i].col->kind == CH_COL_CSTRING || st->cols[i].col->low_cardinality)
			return false;
	(void) ch_scan_unit_parts(st->scan, &parts, &ranges, &nranges);
	if (nranges == 0)
		return false;
	/*
	 * The pages store's manifest of the part, its streams' pages, which
	 * read_unit_bytes() reads; the part attached to the library only where it
	 * is asked about the part (VC29)
	 */
	if (t->options.storage == CH_STORAGE_PAGES)
	{
		MemoryContext old = MemoryContextSwitchTo(st->cxt);

		if (st->mt_files != NULL)
		{
			for (size_t f = 0; f < st->mt_nfiles; f++)
				pfree(unconstify(char *, st->mt_files[f].path));
			pfree(st->mt_files);
		}
		st->mt_files = ch_pages_files(unit->part, &st->mt_nfiles);
		MemoryContextSwitchTo(old);
	}
	st->mt_unit = *unit;
	st->mt_next = ranges[0].begin;
	st->mt_end = ranges[nranges - 1].end;
	pfree(parts);
	pfree(ranges);
	return true;
}

static bool
src_next_unit(void *arg, VexecSourceUnit *u)
{
	SrcState   *st = arg;
	ChScanUnit	unit;

	if (st->have_batch && st->pos < st->batch.nrows)
		elog(ERROR, "a unit claimed inside a batch");
	st->have_batch = false;
	/* W6: the claimed MergeTree unit's next run, or, where it is refused, the rest on the CPU */
	if (st->mt_open)
	{
		if (st->mt_next < st->mt_end)
		{
			if (mergetree_unit(st, u))
				return true;
			ch_scan_read_unit(st->scan, st->mt_unit.part,
							  &(chte_mark_range) {st->mt_next, st->mt_end}, 1, 0);
			st->mt_open = false;
			u->format = VEXEC_UNIT_CPU;
			return true;
		}
		st->mt_open = false;
	}
	if (!ch_scan_claim_unit(st->scan, &unit))
		return false;
	if (unit.rows == 0)
	{
		/* none of its granules kept: read as nothing, here */
		ch_scan_one_unit(st->scan);
		u->format = VEXEC_UNIT_CPU;
		return true;
	}
	if (mergetree_begin(st, &unit))
	{
		if (mergetree_unit(st, u))
		{
			st->mt_open = true;
			ch_scan_pass_unit(st->scan);
			/* as getnextslot counts each row it returns */
			if (pgstat_should_count_relation(st->rel))
				st->rel->pgstat_info->counts.tuples_returned += unit.rows;
			return true;
		}
		ch_scan_one_unit(st->scan);
		u->format = VEXEC_UNIT_CPU;
		return true;
	}
	if (!unit_for_device(st, &unit, u))
	{
		ch_scan_one_unit(st->scan);
		u->format = VEXEC_UNIT_CPU;
		return true;
	}
	ch_scan_pass_unit(st->scan);
	/* as getnextslot counts each row it returns */
	if (pgstat_should_count_relation(st->rel))
		st->rel->pgstat_info->counts.tuples_returned += unit.rows;
	return true;
}

/* V8e: a unit next_unit() handed for the device, read by the next batches alone (minor 7) */
static void
src_read_unit(void *arg, const VexecSourceUnit *u)
{
	SrcState   *st = arg;
	SrcUnit    *keep = (SrcUnit *) u->token;

	if (st->have_batch && st->pos < st->batch.nrows)
		elog(ERROR, "a unit read again inside a batch");
	st->have_batch = false;
	ch_scan_read_unit(st->scan, keep->part, keep->ranges, keep->nranges, keep->rows);
}

/* W6: bytes of a stream of a MergeTree unit next_unit() handed, from its part's file (minor 8) */
static void
src_read_unit_bytes(void *arg, const VexecSourceUnit *u, int stream, uint64 offset, uint64 length, void *dest)
{
	SrcState   *st = arg;
	SrcMtUnit  *keep = (SrcMtUnit *) u->token;
	ChTable    *t = ch_scan_table(st->scan);

	if (u->format != VEXEC_UNIT_MERGETREE || stream < 0 || stream >= keep->nfiles ||
		offset + length > (uint64) u->streams[stream].length)
		elog(ERROR, "bytes asked of a unit's stream past it");
	if (keep->pages[stream].path != NULL)
	{
		ch_pages_read_file(t, &keep->pages[stream], keep->from[stream] + offset, length, dest);
		return;
	}
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, keep->unit.part);
	CH_CALL("reading a part's file for the device",
			ch_engine()->part_file_read(t->handle, keep->unit.part->name, keep->files[stream],
										keep->from[stream] + offset, length, dest, CH_ERR));
}

/*
 * W13 (pg_vector_executor.md §3.27; minor 9): the TIDs of rows of a unit
 * next_unit() handed a scan of TIDs, by their places among the unit's rows:
 * a MergeTree run's rows' ids read from the part's _pg_rowid at their
 * offsets in it, the granules holding them alone (chte_read_rows); a table
 * of its own files', its unit's ids, as its file holds them
 * (chte_own_unit_ids).  Each id's TID as next() makes it.
 */
static int
cmp_uint64(const void *a, const void *b)
{
	uint64		x = *(const uint64 *) a;
	uint64		y = *(const uint64 *) b;

	return x < y ? -1 : x > y ? 1 : 0;
}

/* W13 (minor 9): the TIDs of rows by their ids, as next() makes them */
static void
src_row_tids(Relation rel, int n, const uint64 *ids, ItemPointerData *tids)
{
	(void) rel;
	for (int i = 0; i < n; i++)
		ItemPointerSet(&tids[i], CH_ROWID_TO_BLOCK(ids[i]), CH_ROWID_TO_OFFSET(ids[i]));
}

static void
src_unit_tids(void *arg, const VexecSourceUnit *u, int n, const uint64 *rows, ItemPointerData *tids)
{
	SrcState   *st = arg;
	ChTable    *t = ch_scan_table(st->scan);
	const ChEngine *eng = ch_engine();

	if (n <= 0)
		return;
	for (int i = 0; i < n; i++)
		if (rows[i] >= u->rows)
			elog(ERROR, "a row's TID asked past its unit's rows");
	if (u->format == VEXEC_UNIT_MERGETREE)
	{
		SrcMtUnit  *keep = (SrcMtUnit *) u->token;
		uint64	   *offsets = palloc(sizeof(uint64) * n);
		uint64	   *ids = palloc(sizeof(uint64) * n);
		chte_reader *reader = NULL;
		ResourceOwner owner;
		int			m = 0;
		int			got = 0;

		/* the offsets ascending, each once */
		for (int i = 0; i < n; i++)
			offsets[i] = keep->first_offset + rows[i];
		qsort(offsets, n, sizeof(uint64), cmp_uint64);
		for (int i = 0; i < n; i++)
			if (m == 0 || offsets[m - 1] != offsets[i])
				offsets[m++] = offsets[i];
		if (t->options.storage == CH_STORAGE_PAGES)
			ch_pages_attach(t, keep->unit.part);
		owner = ch_handle_reserve();
		CH_CALL("reading rows' ids for their TIDs",
				eng->read_rows(t->handle, keep->unit.part->name, CH_ROWID_COLUMN, offsets, (size_t) m, &reader,
							   CH_ERR));
		ch_handle_opened(owner, CH_HANDLE_READER, reader);
		for (;;)
		{
			uint32		nrows;
			chte_vector v;

			CH_CALL("reading rows' ids for their TIDs", eng->read_next(reader, &nrows, CH_ERR));
			if (nrows == 0)
				break;
			CH_CALL("reading rows' ids for their TIDs", eng->read_vector(reader, 0, &v, CH_ERR));
			for (uint32 r = 0; r < nrows && got < m; r++)
				ids[got++] = ((const uint64 *) v.values)[r];
		}
		ch_handle_closing(owner, CH_HANDLE_READER, reader);
		eng->read_end(reader);
		if (got != m)
			elog(ERROR, "%d rows' ids read of a part where %d were asked for", got, m);
		for (int i = 0; i < n; i++)
		{
			uint64		off = keep->first_offset + rows[i];
			uint64	   *at = bsearch(&off, offsets, m, sizeof(uint64), cmp_uint64);

			ItemPointerSet(&tids[i], CH_ROWID_TO_BLOCK(ids[at - offsets]), CH_ROWID_TO_OFFSET(ids[at - offsets]));
		}
		pfree(offsets);
		pfree(ids);
		return;
	}
	if (u->format == VEXEC_UNIT_PARQUET || u->format == VEXEC_UNIT_ARROW)
	{
		/*
		 * a table of its own files': each row's place in its file, from its
		 * unit's row groups' rows in the file's metadata, and the ids at those
		 * places read from the row groups holding them alone
		 * (chte_external_read_rows) -- the file's every id, read once a
		 * backend (chte_own_unit_ids), took 50 ms of ClickBench's Q24 in a
		 * fresh one, its 5 million of them
		 */
		SrcUnit    *keep = (SrcUnit *) u->token;
		uint64	   *starts = palloc(sizeof(uint64) * (u->nparts + 1));
		uint64	   *at = palloc(sizeof(uint64) * u->nparts);
		uint64	   *offsets = palloc(sizeof(uint64) * n);
		uint64	   *places = palloc(sizeof(uint64) * n);
		uint64	   *ids = palloc(sizeof(uint64) * n);
		chte_reader *reader = NULL;
		ResourceOwner owner;
		int			m = 0;
		int			got = 0;

		if (!t->own || keep->part->listing == NULL || eng->external_read_rows == NULL || eng->external_select == NULL)
			elog(ERROR, "a unit's rows' TIDs asked of a table not of its own files");
		starts[0] = 0;
		for (int k = 0; k < u->nparts; k++)
		{
			chte_mark_range range;
			size_t		nranges = 0;
			uint64		grows = 0;

			CH_CALL("listing a file's row groups",
					eng->external_select(t->handle, keep->part->listing->handle, (size_t) keep->part->file, NULL,
										 (uint32) u->parts[k], (uint32) u->parts[k] + 1, &range, 1, &nranges,
										 &grows, CH_ERR));
			starts[k + 1] = starts[k] + grows;
			at[k] = UINT64_MAX;
		}
		if (starts[u->nparts] != u->rows)
			elog(ERROR, "a unit of " UINT64_FORMAT " rows in its row groups where it has " UINT64_FORMAT,
				 starts[u->nparts], u->rows);
		for (int i = 0; i < n; i++)
		{
			int			k = 0;

			while (k + 1 < u->nparts && rows[i] >= starts[k + 1])
				k++;
			/* the row group's first row's place in the file: the rows of those before it */
			if (at[k] == UINT64_MAX)
			{
				chte_mark_range range;
				size_t		nranges = 0;
				uint64		before = 0;

				if (u->parts[k] > 0)
					CH_CALL("listing a file's row groups",
							eng->external_select(t->handle, keep->part->listing->handle, (size_t) keep->part->file,
												 NULL, 0, (uint32) u->parts[k], &range, 1, &nranges, &before,
												 CH_ERR));
				at[k] = before;
			}
			places[i] = at[k] + rows[i] - starts[k];
			offsets[i] = places[i];
		}
		qsort(offsets, n, sizeof(uint64), cmp_uint64);
		for (int i = 0; i < n; i++)
			if (m == 0 || offsets[m - 1] != offsets[i])
				offsets[m++] = offsets[i];
		owner = ch_handle_reserve();
		CH_CALL("reading rows' ids for their TIDs",
				eng->external_read_rows(t->handle, keep->part->listing->handle, (size_t) keep->part->file,
										CH_ROWID_COLUMN, offsets, (size_t) m, &reader, CH_ERR));
		ch_handle_opened(owner, CH_HANDLE_READER, reader);
		for (;;)
		{
			uint32		nrows;
			chte_vector v;

			CH_CALL("reading rows' ids for their TIDs", eng->read_next(reader, &nrows, CH_ERR));
			if (nrows == 0)
				break;
			CH_CALL("reading rows' ids for their TIDs", eng->read_vector(reader, 0, &v, CH_ERR));
			for (uint32 r = 0; r < nrows && got < m; r++)
				ids[got++] = ((const uint64 *) v.values)[r];
		}
		ch_handle_closing(owner, CH_HANDLE_READER, reader);
		eng->read_end(reader);
		if (got != m)
			elog(ERROR, "%d rows' ids read of a file where %d were asked for", got, m);
		for (int i = 0; i < n; i++)
		{
			uint64	   *p = bsearch(&places[i], offsets, m, sizeof(uint64), cmp_uint64);

			ItemPointerSet(&tids[i], CH_ROWID_TO_BLOCK(ids[p - offsets]), CH_ROWID_TO_OFFSET(ids[p - offsets]));
		}
		pfree(starts);
		pfree(at);
		pfree(offsets);
		pfree(places);
		pfree(ids);
		return;
	}
	elog(ERROR, "a unit's rows' TIDs asked of a unit of no device's format");
}

/* The library's id of the dictionary of a column of the last batch (minor 3) */
static uint64
src_dictionary_id(void *arg, int column)
{
	SrcState   *st = arg;

	if (st->out == NULL || column < 0 || column >= st->ncols || st->out->dicts[column] == NULL)
		return 0;
	return st->out->dicts[column]->id;
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

/* A hash join's keys, lent to the scan on its outer side (minor 6, VC3) */
static void
src_set_join_keys(void *arg, int nkeys, const ScanKeyData *keys)
{
	SrcState   *st = arg;

	ch_scan_set_join_keys(st->scan, nkeys, keys);
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
	/* a copy: the syscache's lookup may accept an invalidation, at which the cached table goes */
	ChColumn	column = ch_table(rel)->columns[attnum - 1];
	const ChColumn *col = &column;
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

	CHECK_FOR_INTERRUPTS();
	if (batch->nrows <= 0)
		return;
	if (st->cap < (uint32) batch->nrows)
	{
		st->rowids = st->rowids ? repalloc(st->rowids, sizeof(uint64) * batch->nrows) :
			palloc(sizeof(uint64) * batch->nrows);
		st->cap = batch->nrows;
	}
	/* VC18: into the library's writer where it takes the batch's columns so */
	ch_buffer_put_columns(st->rel, st->cid, batch->columns, batch->nrows, st->rowids);
	if (st->tids && batch->tids)
		for (int i = 0; i < batch->nrows; i++)
			ItemPointerSet(&batch->tids[i], CH_ROWID_TO_BLOCK(st->rowids[i]), CH_ROWID_TO_OFFSET(st->rowids[i]));
	pgstat_count_heap_insert(st->rel, batch->nrows);
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

static VexecSourceRoutine sources[CH_ENGINE_COUNT + CH_EXTERNAL_COUNT];
static VexecSinkRoutine sinks[CH_ENGINE_COUNT];

/*
 * The engines' sources and sinks, and the external access methods' sources
 * (§2.17), whose tables' scans are scan.c's too; nothing writes those
 */
void
ch_vexec_register(void)
{
	for (int k = 0; k < CH_ENGINE_COUNT + CH_EXTERNAL_COUNT; k++)
	{
		VexecSourceRoutine *r = &sources[k];
		VexecSinkRoutine *s;
		bool		external = k >= CH_ENGINE_COUNT;

		memset(r, 0, sizeof(VexecSourceRoutine));
		r->size = sizeof(VexecSourceRoutine);
		r->minor = VEXEC_SOURCE_MINOR;
		r->am = external ? ch_external_routine((ChExternalKind) (k - CH_ENGINE_COUNT)) :
			ch_am_routine((ChEngineKind) k);
		r->name = external ? ch_external_name((ChExternalKind) (k - CH_ENGINE_COUNT)) :
			ch_engine_name((ChEngineKind) k);
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
		r->dictionary_id = src_dictionary_id;
		r->estimate_ex = src_estimate_ex;
		r->fetch_columns = src_fetch_columns;
		r->ordering = src_ordering;
		r->next_rest = src_next_rest;
		r->set_join_keys = src_set_join_keys;
		/*
		 * V8e (minor 7, N2): units the GPU's executor reads itself, a Parquet
		 * file's row groups and an Arrow file's record batches -- the parquet
		 * and arrow access methods' tables, external and of their own files.
		 * The engines' parts and Iceberg's data files are always the CPU's:
		 * their sources offer no next_unit(), so that vexec prices no device
		 * read of them (pg_accel's plan/agg.c, units_input())
		 */
		if (external && (k - CH_ENGINE_COUNT == CH_EXTERNAL_PARQUET || k - CH_ENGINE_COUNT == CH_EXTERNAL_ARROW))
		{
			r->next_unit = src_next_unit;
			r->read_unit = src_read_unit;
			/*
			 * W13 (minor 9): a TID scan's units -- its own files' alone --
			 * their rows' ids their files' column, or their TIDs by their places
			 */
			r->unit_tids = src_unit_tids;
			r->row_tids = src_row_tids;
		}
		/*
		 * W6 (minor 8): the plain engine's parts, their columns' compressed
		 * streams, which the device decompresses and decodes; a part the
		 * library cannot describe so, the CPU's (mergetree_begin())
		 */
		if (!external && k == CH_ENGINE_MERGETREE)
		{
			r->next_unit = src_next_unit;
			r->read_unit = src_read_unit;
			r->read_unit_bytes = src_read_unit_bytes;
			r->unit_tids = src_unit_tids;
			r->row_tids = src_row_tids;
		}
		vexec_register_source(r);
		if (external)
			continue;

		s = &sinks[k];
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
