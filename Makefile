# SPDX-License-Identifier: Apache-2.0
# ch_table_engines (ch_table_engines.md §2.1): the extension, built by PGXS.
# The engine library is built apart, by test/engine.sh in the builder image.

MODULE_big = ch_table_engines
OBJS = \
	src/am.o \
	src/buffer.o \
	src/catalog.o \
	src/chscan.o \
	src/convert.o \
	src/engine.o \
	src/filter.o \
	src/functions.o \
	src/module.o \
	src/options.o \
	src/pages.o \
	src/scan.o \
	src/store.o \
	src/table.o \
	src/types.o \
	src/vexec.o

EXTENSION = ch_table_engines
DATA = sql/ch_table_engines--0.1.sql
PG_CPPFLAGS = -I$(srcdir)/include -I$(srcdir)/src
SHLIB_LINK = -ldl

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
