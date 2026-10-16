#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# The identifiers the Cloudberry port's core series adds to PostgreSQL 19's
# headers (ch_table_engines.md §1.2, rule 1): what `git diff REL_19_STABLE
# REL_19_STABLE_CLOUDBERRY -- src/include` adds and vanilla's headers do not
# have.  Run in a container against a checkout of the port's fork:
#   core_identifiers.py <checkout> [<vanilla ref> <port ref>]
# prints them one a line; checks/core.sh keeps the list it made.
import re, subprocess, sys

repo = sys.argv[1]
vanilla = sys.argv[2] if len(sys.argv) > 2 else 'origin/REL_19_STABLE'
port = sys.argv[3] if len(sys.argv) > 3 else 'origin/REL_19_STABLE_CLOUDBERRY'

def git(*args):
    return subprocess.run(['git', '-C', repo] + list(args), check=True,
                          capture_output=True, text=True).stdout

diff = git('diff', vanilla, port, '--', 'src/include')
added = [l[1:] for l in diff.splitlines() if l.startswith('+') and not l.startswith('+++')]
text = '\n'.join(added)
# comments out, so that prose names nothing
text = re.sub(r'/\*.*?\*/', ' ', text, flags=re.S)
text = re.sub(r'//[^\n]*', ' ', text)

names = set()
for m in re.finditer(r'^\s*#\s*define\s+([A-Za-z_]\w*)', text, re.M):
    names.add(m.group(1))
for m in re.finditer(r'\bextern\b[^;(]*?\b([A-Za-z_]\w*)\s*(\(|;|\[)', text):
    names.add(m.group(1))
for m in re.finditer(r'\btypedef\b[^;]*?\b([A-Za-z_]\w*)\s*;', text):
    names.add(m.group(1))
for m in re.finditer(r'\b(?:struct|enum|union)\s+([A-Za-z_]\w*)\s*\{', text):
    names.add(m.group(1))
for m in re.finditer(r'\}\s*([A-Za-z_]\w*)\s*;', text):
    names.add(m.group(1))
for m in re.finditer(r'\(\s*\*\s*([A-Za-z_]\w*)\s*\)\s*\(', text):
    names.add(m.group(1))
# enum members: an uppercase name at the start of a line, before = or ,
for m in re.finditer(r'^\s*([A-Z][A-Z0-9_]+)\s*(=[^,\n]*)?,?\s*$', text, re.M):
    names.add(m.group(1))

keywords = {'const', 'static', 'inline', 'void', 'int', 'bool', 'char', 'extern',
            'PGDLLIMPORT', 'struct', 'typedef', 'unsigned', 'signed', 'enum'}
names -= keywords

# what vanilla's headers already name is not the series'
vanilla_text = git('grep', '-h', '-o', '-w', '-E', '|'.join(sorted(names)) or 'x^',
                   vanilla, '--', 'src/include') if names else ''
present = set(l.split(':')[-1] for l in vanilla_text.splitlines())
for n in sorted(names - present):
    print(n)
