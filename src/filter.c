/*-------------------------------------------------------------------------
 *
 * filter.c
 *	  Quals as ClickHouse's filter for its key and statistics analysis
 *	  (ch_table_engines.md §2.8, "Key conditions"): `column op constant`
 *	  under a type whose order in ClickHouse is PostgreSQL's btree order
 *	  (§2.5), `= ANY (array)`, `IS [NOT] NULL`, a bool column, and AND and
 *	  OR of them.
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

#include "chte.h"
#include "scan.h"

#define CH_FILTER_MAX_VALUES 1024

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

/* The quals of a list a filter is made from */
List *
ch_filter_quals(ChTable *t, List *quals)
{
	List	   *out = NIL;
	ListCell   *lc;

	foreach(lc, quals)
		if (translate_qual(t, lfirst(lc)) != NIL)
			out = lappend(out, lfirst(lc));
	return out;
}

chte_filter *
ch_filter_make(ChTable *t, List *quals)
{
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "ch filter", ALLOCSET_SMALL_SIZES);
	MemoryContext old = MemoryContextSwitchTo(cxt);
	List	   *nodes = NIL;
	int			nquals = 0;
	ListCell   *lc;
	chte_filter_node *array;
	chte_filter *filter = NULL;
	int			i = 0;

	foreach(lc, quals)
	{
		List	   *q = translate_qual(t, lfirst(lc));

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

static bool
part_stats(ChTable *t, const ChPart *part, AttrNumber attnum, chte_column_stats *st)
{
	char		name[16];

	snprintf(name, sizeof(name), "c%d", attnum);
	if (t->options.storage == CH_STORAGE_PAGES)
		ch_pages_attach(t, part);
	CH_CALL("reading a part's statistics", ch_engine()->part_stats(t->handle, part->name, name, st, CH_ERR));
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
