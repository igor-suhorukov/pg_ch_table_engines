/*-------------------------------------------------------------------------
 *
 * filter.c
 *	  Quals as ClickHouse's filter for its key and statistics analysis
 *	  (ch_table_engines.md §2.8, "Key conditions"): `column op constant`
 *	  under a type whose order in ClickHouse is PostgreSQL's btree order
 *	  (§2.5), `= ANY (array)`, `IS [NOT] NULL`, a bool column, and AND and
 *	  OR of them; and ANDed with them, a hash join's keys vexec lends its
 *	  source (ch_table_engines.md §3, VC3; vexec_source.h, set_join_keys) --
 *	  an integer column's `>=` and `<=` the inner keys' least and greatest,
 *	  and `= ANY` their values -- as scan keys.  The values only where the
 *	  column is one of the primary key's, which passes granules over by
 *	  them, and as at most CH_JOIN_KEY_RANGES ranges, the smallest gaps
 *	  between them closed: ClickHouse's analysis of a filter checks each of
 *	  its comparisons for every range of granules it looks at, so that 900
 *	  values made it a third of a parallel scan's time where they pruned
 *	  nothing.  A column of no part of the key prunes parts by the least and
 *	  greatest alone.
 *
 *	  A filter only prunes: the executor evaluates every qual on what is
 *	  read.  So a qual is translated into a condition it implies -- itself,
 *	  where every part of it is translated; under AND, the parts that are;
 *	  under OR, only where every branch is -- and a qual that cannot be is
 *	  left out, pruning nothing.  The constants are converted into the
 *	  column's own ClickHouse type as a row's values are written
 *	  (convert.c), and one that has no value there exactly -- an integer
 *	  out of the column's range, a numeric of more scale or precision -- is
 *	  left out with its comparison, or out of an IN list, where no stored
 *	  value could equal it.
 *
 *	  The types:
 *		int2, int4, int8	any comparison among them, as integers
 *		oid, bool, date, time, timestamp, timestamptz, uuid, bytea
 *							their own type's comparisons: the same order as
 *							ClickHouse's integers and bytes
 *		numeric(p, s)		as its Decimal, NaN the greatest, as stored
 *		text, varchar		equality under a deterministic collation, every
 *							comparison under C's: ClickHouse orders strings
 *							by their bytes
 *	  Floats are left out: NaN is greater than every number in PostgreSQL
 *	  and compares false in ClickHouse.  bpchar is left out: its comparisons
 *	  ignore trailing blanks, which its stored bytes keep.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "utils/varlena.h"

#include "chte.h"
#include "scan.h"

#define CH_FILTER_MAX_VALUES 1024
#define CH_JOIN_KEY_RANGES 32

/* A filter's nodes, in prefix order, and the values they point at */
typedef struct Leaf
{
	chte_filter_node node;
	StringInfoData values;		/* the constants' bytes */
	uint64	   *offsets;		/* strings: nvalues + 1 */
} Leaf;

static bool
is_int_type(Oid t)
{
	return t == INT2OID || t == INT4OID || t == INT8OID;
}

/* The column a Var names, its type the base type, or NULL */
static const ChColumn *
var_column(ChTable *t, Node *node, AttrNumber *attnum)
{
	Var		   *var;

	while (node && IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;
	if (node == NULL || !IsA(node, Var))
		return NULL;
	var = (Var *) node;
	if (var->varattno < 1 || var->varattno > t->natts || var->varlevelsup != 0)
		return NULL;
	*attnum = var->varattno;
	return &t->columns[var->varattno - 1];
}

/* The column types whose order ClickHouse keeps (above) */
static bool
filterable_type(const ChColumn *col, Oid *basetype)
{
	Oid			typid = getBaseType(col->typid);

	*basetype = typid;
	switch (col->kind)
	{
		case CH_COL_FIXED:
			return typid == INT2OID || typid == INT4OID || typid == INT8OID || typid == OIDOID ||
				typid == BOOLOID || typid == DATEOID || typid == TIMEOID || typid == TIMESTAMPOID ||
				typid == TIMESTAMPTZOID || typid == UUIDOID;
		case CH_COL_NUMERIC:
			return typid == NUMERICOID;
		case CH_COL_VARLENA:
			return typid == TEXTOID || typid == VARCHAROID || typid == BYTEAOID;
		default:
			return false;
	}
}

/*
 * What an operator does to (column, constant) as a filter's op: one of the
 * column type's default btree family's comparisons, or <> as its equality's
 * negator; -1 for anything else.
 */
static int
classify_op(Oid opno, Oid basetype, Oid righttype, Oid collid)
{
	Oid			opclass;
	Oid			family;
	List	   *interps;
	ListCell   *lc;

	/* varchar compares as text, its comparisons text's */
	if (basetype == VARCHAROID)
		basetype = TEXTOID;
	if (righttype == VARCHAROID)
		righttype = TEXTOID;
	opclass = GetDefaultOpClass(basetype, BTREE_AM_OID);
	if (!OidIsValid(opclass))
		return -1;
	family = get_opclass_family(opclass);
	interps = get_op_index_interpretation(opno);
	foreach(lc, interps)
	{
		OpIndexInterpretation *oi = lfirst(lc);
		bool		equality = oi->cmptype == COMPARE_EQ || oi->cmptype == COMPARE_NE;

		if (oi->opfamily_id != family)
			continue;
		/* integers compare across their widths; every other type with itself */
		if (is_int_type(basetype) ? !is_int_type(oi->oprighttype) || !is_int_type(righttype) :
			oi->oprighttype != basetype || righttype != basetype)
			continue;
		if (basetype == TEXTOID)
		{
			if (!OidIsValid(collid))
				continue;
			if (equality ? !get_collation_isdeterministic(collid) :
				!pg_newlocale_from_collation(collid)->collate_is_c)
				continue;
		}
		switch (oi->cmptype)
		{
			case COMPARE_LT:
				return CHTE_FILTER_LT;
			case COMPARE_LE:
				return CHTE_FILTER_LE;
			case COMPARE_EQ:
				return CHTE_FILTER_EQ;
			case COMPARE_GE:
				return CHTE_FILTER_GE;
			case COMPARE_GT:
				return CHTE_FILTER_GT;
			case COMPARE_NE:
				return CHTE_FILTER_NE;
			default:
				break;
		}
	}
	return -1;
}

/*
 * A numeric as the scaled integer of the column's Decimal, when it is one
 * exactly: no more digits after the point than the scale but zeros, and no
 * more before it than the precision leaves.  NaN is the integer's greatest,
 * as stored (convert.c).
 */
static bool
numeric_exact(const ChColumn *col, Datum value, char *out)
{
	Numeric		num = DatumGetNumeric(value);
	char	   *text;
	char	   *p;
	int128		result = 0;
	int			before = 0;
	int			frac = 0;
	bool		in_frac = false;
	bool		negative = false;

	if (numeric_is_nan(num))
	{
		if (col->width == 4)
			*(int32 *) out = PG_INT32_MAX;
		else if (col->width == 8)
			*(int64 *) out = PG_INT64_MAX;
		else
		{
			int128		max = ((int128) PG_INT64_MAX << 64) | (uint64) PG_UINT64_MAX;

			memcpy(out, &max, sizeof(max));
		}
		return true;
	}
	if (numeric_is_inf(num))
		return false;
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
				{
					if (*p != '0')
						return false;
					continue;
				}
				frac++;
			}
			else if (result != 0 || *p != '0')
				before++;
			result = result * 10 + (*p - '0');
		}
	}
	if (before > col->precision - col->scale)
		return false;
	for (; frac < col->scale; frac++)
		result *= 10;
	if (negative)
		result = -result;
	if (col->width == 4)
		*(int32 *) out = (int32) result;
	else if (col->width == 8)
		*(int64 *) out = (int64) result;
	else
		memcpy(out, &result, sizeof(result));
	return true;
}

/* A constant into a leaf's values, in the column's own type; false if it has no value there */
static bool
add_value(Leaf *leaf, const ChColumn *col, Oid basetype, Datum value, Oid valtype)
{
	char		buf[16];

	switch (col->kind)
	{
		case CH_COL_FIXED:
			if (is_int_type(basetype))
			{
				int64		v = valtype == INT2OID ? DatumGetInt16(value) :
					valtype == INT4OID ? DatumGetInt32(value) : DatumGetInt64(value);

				if (col->width == 2)
				{
					if (v < PG_INT16_MIN || v > PG_INT16_MAX)
						return false;
					*(int16 *) buf = (int16) v;
				}
				else if (col->width == 4)
				{
					if (v < PG_INT32_MIN || v > PG_INT32_MAX)
						return false;
					*(int32 *) buf = (int32) v;
				}
				else
					*(int64 *) buf = v;
			}
			else if (basetype == BOOLOID)
				buf[0] = DatumGetBool(value) ? 1 : 0;
			else if (col->typbyval)
			{
				if (col->width == 4)
					*(int32 *) buf = DatumGetInt32(value);
				else if (col->width == 8)
					*(int64 *) buf = DatumGetInt64(value);
				else
					return false;
			}
			else
				memcpy(buf, DatumGetPointer(value), col->width);
			appendBinaryStringInfo(&leaf->values, buf, col->width);
			break;
		case CH_COL_NUMERIC:
			if (!numeric_exact(col, value, buf))
				return false;
			appendBinaryStringInfo(&leaf->values, buf, col->width);
			break;
		case CH_COL_VARLENA:
			{
				struct varlena *v = pg_detoast_datum_packed((struct varlena *) DatumGetPointer(value));
				int			n = leaf->node.nvalues;

				appendBinaryStringInfo(&leaf->values, VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
				leaf->offsets = leaf->offsets ? repalloc(leaf->offsets, sizeof(uint64) * (n + 2)) :
					palloc0(sizeof(uint64) * 2);
				leaf->offsets[n + 1] = leaf->values.len;
			}
			break;
		default:
			return false;
	}
	leaf->node.nvalues++;
	return true;
}

static Leaf *
make_leaf(int op, AttrNumber attnum)
{
	Leaf	   *leaf = palloc0(sizeof(Leaf));

	leaf->node.op = op;
	leaf->node.column = psprintf("c%d", attnum);
	initStringInfo(&leaf->values);
	return leaf;
}

static Leaf *
make_bool_node(int op, int nargs)
{
	Leaf	   *n = palloc0(sizeof(Leaf));

	n->node.op = op;
	n->node.nargs = nargs;
	return n;
}

/* column op constant, or constant op column */
static List *
translate_opexpr(ChTable *t, OpExpr *op)
{
	Node	   *left;
	Node	   *right;
	Oid			opno = op->opno;
	const ChColumn *col;
	AttrNumber	attnum;
	Const	   *c;
	Oid			basetype;
	int			fop;
	Leaf	   *leaf;

	if (list_length(op->args) != 2)
		return NIL;
	left = linitial(op->args);
	right = lsecond(op->args);
	if ((col = var_column(t, left, &attnum)) == NULL)
	{
		/* constant op column: the commutator's column op constant */
		if ((col = var_column(t, right, &attnum)) == NULL || !OidIsValid(opno = get_commutator(opno)))
			return NIL;
		right = left;
	}
	if (!IsA(right, Const) || ((Const *) right)->constisnull)
		return NIL;
	c = (Const *) right;
	if (!filterable_type(col, &basetype))
		return NIL;
	fop = classify_op(opno, basetype, getBaseType(c->consttype), op->inputcollid);
	if (fop < 0)
		return NIL;
	leaf = make_leaf(fop, attnum);
	if (!add_value(leaf, col, basetype, c->constvalue, getBaseType(c->consttype)))
		return NIL;
	return list_make1(leaf);
}

/* column = ANY (constant array) */
static List *
translate_saop(ChTable *t, ScalarArrayOpExpr *saop)
{
	const ChColumn *col;
	AttrNumber	attnum;
	Const	   *c;
	Oid			basetype;
	Oid			elemtype;
	ArrayType  *array;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Leaf	   *leaf;

	if (!saop->useOr || list_length(saop->args) != 2)
		return NIL;
	if ((col = var_column(t, linitial(saop->args), &attnum)) == NULL || !filterable_type(col, &basetype))
		return NIL;
	if (!IsA(lsecond(saop->args), Const) || ((Const *) lsecond(saop->args))->constisnull)
		return NIL;
	c = lsecond(saop->args);
	elemtype = get_element_type(c->consttype);
	if (!OidIsValid(elemtype) ||
		classify_op(saop->opno, basetype, getBaseType(elemtype), saop->inputcollid) != CHTE_FILTER_EQ)
		return NIL;
	array = DatumGetArrayTypeP(c->constvalue);
	get_typlenbyvalalign(elemtype, &typlen, &typbyval, &typalign);
	deconstruct_array(array, elemtype, typlen, typbyval, typalign, &elems, &nulls, &nelems);
	if (nelems > CH_FILTER_MAX_VALUES)
		return NIL;
	leaf = make_leaf(CHTE_FILTER_IN, attnum);
	for (int i = 0; i < nelems; i++)
		/* a NULL element equals no row; one the column cannot hold equals none either */
		if (!nulls[i])
			(void) add_value(leaf, col, basetype, elems[i], getBaseType(elemtype));
	if (leaf->node.nvalues == 0)
		return NIL;
	return list_make1(leaf);
}

/* A bool column as itself (true) or under NOT (false) */
static List *
translate_bool_var(ChTable *t, Node *node, bool value)
{
	const ChColumn *col;
	AttrNumber	attnum;
	Oid			basetype;
	Leaf	   *leaf;

	if ((col = var_column(t, node, &attnum)) == NULL || !filterable_type(col, &basetype) || basetype != BOOLOID)
		return NIL;
	leaf = make_leaf(CHTE_FILTER_EQ, attnum);
	(void) add_value(leaf, col, basetype, BoolGetDatum(value), BOOLOID);
	return list_make1(leaf);
}

/* A qual's filter, as nodes in prefix order, implied by it; NIL: none */
static List *
translate_qual(ChTable *t, Node *node)
{
	if (node == NULL)
		return NIL;
	if (IsA(node, RestrictInfo))
		return translate_qual(t, (Node *) ((RestrictInfo *) node)->clause);
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *b = (BoolExpr *) node;
		List	   *out = NIL;
		int			n = 0;
		ListCell   *lc;

		switch (b->boolop)
		{
			case AND_EXPR:
				foreach(lc, b->args)
				{
					List	   *arg = translate_qual(t, lfirst(lc));

					if (arg != NIL)
					{
						out = list_concat(out, arg);
						n++;
					}
				}
				if (n <= 1)
					return out;
				return lcons(make_bool_node(CHTE_FILTER_AND, n), out);
			case OR_EXPR:
				foreach(lc, b->args)
				{
					List	   *arg = translate_qual(t, lfirst(lc));

					if (arg == NIL)
						return NIL;
					out = list_concat(out, arg);
					n++;
				}
				return lcons(make_bool_node(CHTE_FILTER_OR, n), out);
			case NOT_EXPR:
				return translate_bool_var(t, linitial(b->args), false);
		}
		return NIL;
	}
	if (IsA(node, OpExpr))
		return translate_opexpr(t, (OpExpr *) node);
	if (IsA(node, ScalarArrayOpExpr))
		return translate_saop(t, (ScalarArrayOpExpr *) node);
	if (IsA(node, NullTest))
	{
		NullTest   *nt = (NullTest *) node;
		const ChColumn *col;
		AttrNumber	attnum;

		if (nt->argisrow || (col = var_column(t, (Node *) nt->arg, &attnum)) == NULL ||
			col->kind == CH_COL_DROPPED)
			return NIL;
		return list_make1(make_leaf(nt->nulltesttype == IS_NULL ? CHTE_FILTER_IS_NULL : CHTE_FILTER_IS_NOT_NULL,
									attnum));
	}
	return translate_bool_var(t, node, true);
}

/* Whether attnum is one of the table's primary key's columns -- its order's where it names none apart */
static bool
key_column(ChTable *t, AttrNumber attnum)
{
	const char *keys = t->options.primary_key ? t->options.primary_key : t->options.order_by;
	char	   *copy;
	List	   *names;
	ListCell   *lc;
	bool		found = false;

	if (keys == NULL)
		return false;
	copy = pstrdup(keys);
	if (SplitIdentifierString(copy, ',', &names))
		foreach(lc, names)
			if (get_attnum(t->relid, lfirst(lc)) == attnum)
			{
				found = true;
				break;
			}
	list_free(names);
	pfree(copy);
	return found;
}

static int
int64_cmp(const void *a, const void *b)
{
	int64		x = *(const int64 *) a;
	int64		y = *(const int64 *) b;

	return x < y ? -1 : x > y ? 1 : 0;
}

/* A gap between two values: the index of the first, and the distance to the next */
typedef struct Gap
{
	int			at;
	uint64		size;
} Gap;

static int
gap_cmp(const void *a, const void *b)
{
	const Gap  *x = a;
	const Gap  *y = b;

	if (x->size != y->size)
		return x->size > y->size ? -1 : 1;
	return x->at - y->at;
}

static Datum
int_datum(Oid type, int64 v)
{
	return type == INT2OID ? Int16GetDatum((int16) v) :
		type == INT4OID ? Int32GetDatum((int32) v) : Int64GetDatum(v);
}

/*
 * VC3: a join's values of a key column as at most CH_JOIN_KEY_RANGES ranges
 * -- runs of consecutive values each one, then the largest gaps kept apart
 * and the others closed -- ORed: a range of one value its equality, any
 * other `>=` its least AND `<=` its greatest.  NIL where one range holds
 * them all, which the keys' least and greatest already say, or a value the
 * column cannot hold.
 */
static List *
values_as_ranges(const ChColumn *col, Oid basetype, AttrNumber attnum, Oid type, int64 *v, int n)
{
	Gap		   *gaps = palloc(sizeof(Gap) * Max(n, 1));
	bool	   *cut = palloc0(sizeof(bool) * Max(n, 1));
	int			ngaps = 0;
	int			nranges;
	List	   *out = NIL;
	int			lo = 0;

	qsort(v, n, sizeof(int64), int64_cmp);
	for (int i = 0; i + 1 < n; i++)
		if ((uint64) v[i + 1] - (uint64) v[i] > 1)
		{
			gaps[ngaps].at = i;
			gaps[ngaps++].size = (uint64) v[i + 1] - (uint64) v[i];
		}
	if (ngaps > CH_JOIN_KEY_RANGES - 1)
	{
		qsort(gaps, ngaps, sizeof(Gap), gap_cmp);
		ngaps = CH_JOIN_KEY_RANGES - 1;
	}
	if (ngaps == 0)
		return NIL;
	for (int g = 0; g < ngaps; g++)
		cut[gaps[g].at] = true;
	nranges = ngaps + 1;
	out = list_make1(make_bool_node(CHTE_FILTER_OR, nranges));
	for (int i = 0; i < n; i++)
	{
		Leaf	   *a;
		Leaf	   *b;

		if (i + 1 < n && !cut[i])
			continue;
		/* a range: values [lo, i] */
		if (v[lo] == v[i])
		{
			a = make_leaf(CHTE_FILTER_EQ, attnum);
			if (!add_value(a, col, basetype, int_datum(type, v[i]), type))
				return NIL;
			out = lappend(out, a);
		}
		else
		{
			a = make_leaf(CHTE_FILTER_GE, attnum);
			b = make_leaf(CHTE_FILTER_LE, attnum);
			if (!add_value(a, col, basetype, int_datum(type, v[lo]), type) ||
				!add_value(b, col, basetype, int_datum(type, v[i]), type))
				return NIL;
			out = lappend(out, make_bool_node(CHTE_FILTER_AND, 2));
			out = lappend(out, a);
			out = lappend(out, b);
		}
		lo = i + 1;
	}
	return out;
}

/*
 * A hash join's key, as vexec lends it (VC3): `column >= v`, `column <= v` or
 * `column = ANY (array)`, of an integer column, as its filter -- the values
 * as ranges of a key column alone; NIL where it is of another form, or
 * values of another column or more than an IN list takes.
 */
static List *
translate_key(ChTable *t, const ScanKeyData *key)
{
	const ChColumn *col;
	Oid			basetype;
	Leaf	   *leaf;

	if (key->sk_attno < 1 || key->sk_attno > t->natts || (key->sk_flags & SK_ISNULL))
		return NIL;
	col = &t->columns[key->sk_attno - 1];
	if (!filterable_type(col, &basetype) || !is_int_type(basetype) || !is_int_type(key->sk_subtype))
		return NIL;
	if (key->sk_flags & SK_SEARCHARRAY)
	{
		ArrayType  *array = DatumGetArrayTypeP(key->sk_argument);
		Datum	   *elems;
		bool	   *nulls;
		int			nelems;

		int16		typlen;
		bool		typbyval;
		char		typalign;

		if (key->sk_strategy != BTEqualStrategyNumber || ARR_ELEMTYPE(array) != key->sk_subtype)
			return NIL;
		get_typlenbyvalalign(key->sk_subtype, &typlen, &typbyval, &typalign);
		deconstruct_array(array, key->sk_subtype, typlen, typbyval, typalign, &elems, &nulls, &nelems);
		if (nelems == 0 || nelems > CH_FILTER_MAX_VALUES || !key_column(t, key->sk_attno))
			return NIL;
		{
			int64	   *v = palloc(sizeof(int64) * nelems);
			int			n = 0;

			for (int i = 0; i < nelems; i++)
				if (!nulls[i])
					v[n++] = key->sk_subtype == INT2OID ? DatumGetInt16(elems[i]) :
						key->sk_subtype == INT4OID ? DatumGetInt32(elems[i]) : DatumGetInt64(elems[i]);
			return values_as_ranges(col, basetype, key->sk_attno, key->sk_subtype, v, n);
		}
	}
	if (key->sk_strategy != BTGreaterEqualStrategyNumber && key->sk_strategy != BTLessEqualStrategyNumber)
		return NIL;
	leaf = make_leaf(key->sk_strategy == BTGreaterEqualStrategyNumber ? CHTE_FILTER_GE : CHTE_FILTER_LE,
					 key->sk_attno);
	if (!add_value(leaf, col, basetype, key->sk_argument, key->sk_subtype))
		return NIL;
	return list_make1(leaf);
}

/*
 * Whether a table's units carry what a filter prunes them by: an engine's granules and parts, a Parquet
 * file's row groups their statistics; an Arrow file's record batches carry none (§2.17)
 */
static bool
prunable(const ChTable *t)
{
	return !t->external || t->ext_kind != CH_EXTERNAL_ARROW;
}

/* The quals of a list a filter is made from */
List *
ch_filter_quals(ChTable *t, List *quals)
{
	List	   *out = NIL;
	ListCell   *lc;

	if (!prunable(t))
		return NIL;
	foreach(lc, quals)
		if (translate_qual(t, lfirst(lc)) != NIL)
			out = lappend(out, lfirst(lc));
	return out;
}

chte_filter *
ch_filter_make(ChTable *t, List *quals)
{
	return ch_filter_make_keys(t, quals, 0, NULL);
}

/* The quals' filter, ANDed with a hash join's keys (VC3) */
chte_filter *
ch_filter_make_keys(ChTable *t, List *quals, int nkeys, const ScanKeyData *keys)
{
	MemoryContext cxt;
	MemoryContext old;
	List	   *nodes = NIL;
	int			nquals = 0;
	ListCell   *lc;
	chte_filter_node *array;
	chte_filter *filter = NULL;
	int			i = 0;

	if (!prunable(t))
		return NULL;
	cxt = AllocSetContextCreate(CurrentMemoryContext, "ch filter", ALLOCSET_SMALL_SIZES);
	old = MemoryContextSwitchTo(cxt);
	foreach(lc, quals)
	{
		List	   *q = translate_qual(t, lfirst(lc));

		if (q != NIL)
		{
			nodes = list_concat(nodes, q);
			nquals++;
		}
	}
	for (int k = 0; k < nkeys; k++)
	{
		List	   *q = translate_key(t, &keys[k]);

		if (q != NIL)
		{
			nodes = list_concat(nodes, q);
			nquals++;
		}
	}
	if (nquals > 1)
		nodes = lcons(make_bool_node(CHTE_FILTER_AND, nquals), nodes);
	if (nodes != NIL)
	{
		array = palloc(sizeof(chte_filter_node) * list_length(nodes));
		foreach(lc, nodes)
		{
			Leaf	   *leaf = lfirst(lc);

			array[i] = leaf->node;
			if (leaf->node.nvalues > 0)
			{
				array[i].values.values = leaf->values.data;
				array[i].values.offsets = leaf->offsets;
				array[i].values.nulls = NULL;
			}
			i++;
		}
		MemoryContextSwitchTo(old);
		CH_CALL("making a filter", ch_engine()->filter_create(t->handle, array, i, &filter, CH_ERR));
	}
	else
		MemoryContextSwitchTo(old);
	MemoryContextDelete(cxt);
	return filter;
}

/*
 * A part's statistics of a column: its summary's (VC20's B6) where it has
 * one, else the part's, loaded.  The part's summary is read into it once
 * (ch_part_summary()), its list's own copy of the part's row.
 */
static bool
part_stats(ChTable *t, const ChPart *part, AttrNumber attnum, chte_column_stats *st)
{
	const ChEngine *eng = ch_engine();
	char		name[16];
	int			found = 0;

	snprintf(name, sizeof(name), "c%d", attnum);
	if (ch_enable_part_summaries && eng->summary_stats != NULL && part->summary != NULL)
	{
		bytea	   *summary = ch_part_summary((ChPart *) part);

		CH_CALL("reading a part's statistics",
				eng->summary_stats(t->handle, part->name, VARDATA_ANY(summary), VARSIZE_ANY_EXHDR(summary), name,
								   st, &found, CH_ERR));
	}
	if (!found)
	{
		if (t->options.storage == CH_STORAGE_PAGES)
			ch_pages_attach(t, part);
		/* a part's, or an external table's file's (§2.17) */
		ch_unit_stats(t, part, name, st);
	}
	return st->has_stats && st->has_null_count;
}

bool
ch_part_nulls(ChTable *t, const ChPart *part, AttrNumber attnum, int64 *nulls)
{
	chte_column_stats st;

	if (!part_stats(t, part, attnum, &st))
		return false;
	*nulls = (int64) st.null_count;
	return true;
}

/*
 * The types whose least and greatest values ClickHouse keeps as PostgreSQL
 * orders them: floats' leave NaN out, and "char" and the other by-value
 * types kept as signed integers order otherwise.
 */
static bool
minmax_type(const ChColumn *col)
{
	Oid			typid = getBaseType(col->typid);

	if (col->kind == CH_COL_NUMERIC)
		return true;
	return col->kind == CH_COL_FIXED &&
		(typid == INT2OID || typid == INT4OID || typid == INT8OID || typid == OIDOID || typid == BOOLOID ||
		 typid == DATEOID || typid == TIMEOID || typid == TIMESTAMPOID || typid == TIMESTAMPTZOID);
}

/* Whether a part's statistics keep a column's least and greatest values */
bool
ch_column_minmax_kept(const ChColumn *col)
{
	return minmax_type(col);
}

bool
ch_part_minmax(ChTable *t, const ChPart *part, AttrNumber attnum, bool *has_values,
			   Datum *min, Datum *max, int64 *nulls)
{
	chte_column_stats st;
	bool		isnull;

	if (attnum < 1 || attnum > t->natts || !minmax_type(&t->columns[attnum - 1]))
		return false;
	if (!part_stats(t, part, attnum, &st))
		return false;
	*nulls = (int64) st.null_count;
	*has_values = st.null_count < st.rows;
	if (!*has_values)
		return true;			/* every value NULL: no least or greatest */
	if (!st.has_minmax)
		return false;
	ch_vector_to_datum(&t->columns[attnum - 1], &st.min, 0, min, &isnull, NULL);
	ch_vector_to_datum(&t->columns[attnum - 1], &st.max, 0, max, &isnull, NULL);
	return true;
}
