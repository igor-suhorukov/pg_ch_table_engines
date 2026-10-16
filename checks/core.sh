#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
#
# ch_table_engines.md §1.2, rule 1: the extension uses only what REL_19_STABLE
# exports.  Fails where a source under src/ names one of the identifiers the
# port's core series adds (checks/core_identifiers.txt), comments aside: a
# comment may cite one.  Runs in a container (rule 8): test/run.sh core.
set -eu
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$here/.." && pwd)"

python3 - "$ROOT" "$here/core_identifiers.txt" <<'PY'
import os, re, sys
root, listfile = sys.argv[1], sys.argv[2]
ids = [l.strip() for l in open(listfile) if l.strip() and not l.startswith('#')]
pat = re.compile(r'\b(' + '|'.join(map(re.escape, ids)) + r')\b')

def strip(src):
    # C comments and string literals out, line structure kept
    out, i, n = [], 0, len(src)
    while i < n:
        if src.startswith('/*', i):
            j = src.find('*/', i + 2)
            j = n if j < 0 else j + 2
            out.append('\n' * src.count('\n', i, j))
            i = j
        elif src.startswith('//', i):
            j = src.find('\n', i)
            i = n if j < 0 else j
        elif src[i] in '"\'':
            q, j = src[i], i + 1
            while j < n and src[j] != q:
                j += 2 if src[j] == '\\' else 1
            out.append(q + q)
            i = j + 1
        else:
            out.append(src[i])
            i += 1
    return ''.join(out)

bad = 0
for dirpath, _, files in os.walk(os.path.join(root, 'src')):
    for f in sorted(files):
        if not f.endswith(('.c', '.h')):
            continue
        path = os.path.join(dirpath, f)
        for no, line in enumerate(strip(open(path).read()).splitlines(), 1):
            for m in pat.finditer(line):
                print(f"{os.path.relpath(path, root)}:{no}: names {m.group(1)}, of the port's core series")
                bad += 1
print(f"core: {len(ids)} identifiers of the port's core series, {bad} named in src/")
sys.exit(1 if bad else 0)
PY
