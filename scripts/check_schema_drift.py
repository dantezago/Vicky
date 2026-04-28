#!/usr/bin/env python3
"""Confere que toda coluna adicionada por migration SQLite (storage.py) tem um
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS ...` correspondente no
schema_pg.sql. É a salvaguarda da convenção descrita no CLAUDE.md:

    Toda mudança de schema vira (a) migration SQLite em storage.py + (b)
    coluna no CREATE TABLE do schema_pg.sql + (c) ALTER ... IF NOT EXISTS
    no schema_pg.sql logo abaixo do CREATE.

Saída: 0 se schemas batem, 1 + diff legível se há drift. Rode via
`make check-schema` ou direto: `python scripts/check_schema_drift.py`.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STORAGE = ROOT / "src" / "vicky" / "storage.py"
SCHEMA = ROOT / "src" / "vicky" / "schema_pg.sql"

SQLITE_ALTER_RE = re.compile(
    r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)",
    re.IGNORECASE,
)
PG_ALTER_RE = re.compile(
    r"ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+(\w+)",
    re.IGNORECASE,
)


def extract(path: Path, pattern: re.Pattern[str]) -> set[tuple[str, str]]:
    text = path.read_text()
    return {(m.group(1).lower(), m.group(2).lower()) for m in pattern.finditer(text)}


def main() -> int:
    sqlite_alters = extract(STORAGE, SQLITE_ALTER_RE)
    pg_alters = extract(SCHEMA, PG_ALTER_RE)

    missing = sqlite_alters - pg_alters
    if missing:
        print("✗ schema drift: colunas adicionadas no SQLite mas sem")
        print("  ALTER TABLE ... ADD COLUMN IF NOT EXISTS no schema_pg.sql:")
        for table, col in sorted(missing):
            print(f"    - {table}.{col}")
        print()
        print(f"  Adicione em {SCHEMA.relative_to(ROOT)} logo abaixo do")
        print("  CREATE TABLE correspondente.")
        return 1

    print(f"✓ {len(sqlite_alters)} colunas de migration SQLite cobertas no schema_pg.sql")
    return 0


if __name__ == "__main__":
    sys.exit(main())
