"""ETL: copia dados de um SQLite legado pra um Postgres com schema_pg.sql aplicado.

Uso:
    DATABASE_URL=postgresql://vicky:vicky_dev@localhost:5433/vicky \
        .venv/bin/python scripts/migrate_to_pg.py [--sqlite PATH] [--truncate]

Default: SQLite em ~/.local/share/vicky/vicky.db.

Estratégia:
- Lê em chunks (10k rows) e insere em ordem topológica das FKs.
- Usa COPY (binary) onde possível pra velocidade — fallback pra executemany.
- Reseta sequences depois pra IDs futuros não colidirem com migrados.
- Idempotente com --truncate (limpa antes de copiar).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from pathlib import Path

import psycopg

# Ordem topológica respeitando FKs.
TABLE_ORDER = [
    "users",
    "workspaces",
    "projects",
    "articles",
    "analyses",
    "double_checks",
    "user_decisions",
    "jobs",
    "llm_usage",
]

# Colunas explícitas garantem ordem consistente (e ignoram colunas legadas
# que possam ter ficado em backups antigos do SQLite).
COLUMNS = {
    "users": [
        "id", "email", "password_hash", "name", "role", "status",
        "credits", "created_at",
    ],
    "workspaces": [
        "id", "name", "owner_user_id", "rayyan_email", "rayyan_password",
        "rayyan_review_id", "openrouter_model", "openrouter_api_key", "created_at",
    ],
    "projects": [
        "id", "workspace_id", "topic", "objective", "years_window",
        "target_articles", "review_type", "criteria_md", "search_strings",
        "sources", "status", "error", "created_by", "created_at", "updated_at",
    ],
    "articles": [
        "workspace_id", "project_id", "source", "external_id", "title",
        "authors", "year", "journal", "abstract", "doi", "external_url",
        "raw_json", "is_duplicate", "scraped_at",
    ],
    "analyses": [
        "project_id", "source", "external_id", "decision", "reason",
        "summary_pt", "criteria_matched", "criteria_violated",
        "quality_score", "score_breakdown", "in_top_n",
        "model", "raw_response", "analyzed_at",
    ],
    "double_checks": [
        "project_id", "source", "external_id", "agrees", "final_decision",
        "explanation", "model", "raw_response", "checked_at",
    ],
    "user_decisions": [
        "project_id", "source", "external_id", "decision", "note",
        "decided_by", "decided_at",
    ],
    "jobs": [
        "id", "project_id", "step", "status", "progress", "message", "error",
        "started_at", "finished_at", "created_at",
    ],
    "llm_usage": [
        "id", "workspace_id", "project_id", "user_id", "pipeline_step", "model",
        "prompt_tokens", "completion_tokens", "total_tokens", "cost_usd",
        "duration_ms", "article_source", "article_external_id", "article_title",
        "request_metadata", "generation_id", "cost_source", "created_at",
    ],
}

# Tabelas com BIGSERIAL — sequence precisa ser resetada pro próximo id.
TABLES_WITH_SERIAL_ID = ["users", "workspaces", "projects", "jobs", "llm_usage"]

CHUNK_SIZE = 5_000


def _sqlite_columns(sconn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in sconn.execute(f"PRAGMA table_info({table})").fetchall()}


def _row_values(row: sqlite3.Row, columns: list[str], existing: set[str]):
    """Resolve cada coluna do alvo PG a partir do row SQLite (None se faltar)."""
    return tuple(row[c] if c in existing else None for c in columns)


def _copy_table(sconn: sqlite3.Connection, pg: psycopg.Connection,
                table: str, *, truncate: bool) -> int:
    cols = COLUMNS[table]
    existing = _sqlite_columns(sconn, table)
    if not existing:
        print(f"  · {table}: tabela ausente no SQLite, pulando")
        return 0
    if truncate:
        with pg.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
    total = sconn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if total == 0:
        print(f"  · {table}: vazio")
        return 0

    select_cols = ", ".join(c for c in cols if c in existing)
    cur_src = sconn.execute(f"SELECT {select_cols} FROM {table}")

    copy_cols = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({copy_cols}) VALUES ({placeholders})"

    inserted = 0
    t0 = time.time()
    with pg.cursor() as cur_dst:
        while True:
            rows = cur_src.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            batch = [_row_values(r, cols, existing) for r in rows]
            cur_dst.executemany(sql, batch)
            inserted += len(batch)
            print(f"  · {table}: {inserted}/{total} ({(inserted/total)*100:.1f}%)",
                  end="\r", flush=True)
    dt = time.time() - t0
    print(f"  ✓ {table}: {inserted}/{total} em {dt:.1f}s" + " " * 20)
    return inserted


def _reset_sequences(pg: psycopg.Connection) -> None:
    print("\nResetando sequences:")
    with pg.cursor() as cur:
        for table in TABLES_WITH_SERIAL_ID:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence(%s, 'id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 0) + 1, false)",
                (table,),
            )
            row = cur.fetchone()
            new_val = row[0] if row else "?"
            print(f"  · {table}_id_seq → {new_val}")


def _validate_counts(sconn: sqlite3.Connection, pg: psycopg.Connection) -> bool:
    print("\nValidação (SQLite vs Postgres):")
    ok = True
    with pg.cursor() as cur:
        for table in TABLE_ORDER:
            existing = _sqlite_columns(sconn, table)
            if not existing:
                continue
            sq = sconn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row = cur.fetchone()
            pg_count = row[0] if row else 0
            mark = "✓" if sq == pg_count else "✗"
            if sq != pg_count:
                ok = False
            print(f"  {mark} {table}: sqlite={sq} pg={pg_count}")
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL SQLite → Postgres pra Vicky")
    default_sqlite = Path.home() / ".local" / "share" / "vicky" / "vicky.db"
    parser.add_argument("--sqlite", type=Path, default=default_sqlite,
                        help=f"Caminho do .db (default: {default_sqlite})")
    parser.add_argument("--truncate", action="store_true",
                        help="TRUNCATE alvo antes de copiar (idempotente)")
    args = parser.parse_args()

    pg_url = os.getenv("DATABASE_URL")
    if not pg_url or not pg_url.startswith(("postgres://", "postgresql://")):
        print("ERRO: defina DATABASE_URL=postgresql://...")
        return 2
    if not args.sqlite.exists():
        print(f"ERRO: SQLite não encontrado em {args.sqlite}")
        return 2

    print(f"SQLite : {args.sqlite}")
    print(f"Postgres: {pg_url}")
    print(f"Truncate: {args.truncate}")
    print()

    sconn = sqlite3.connect(args.sqlite)
    sconn.row_factory = sqlite3.Row
    sconn.execute("PRAGMA foreign_keys = OFF")

    with psycopg.connect(pg_url) as pg:
        pg.execute("SET session_replication_role = replica")  # FKs OFF temporariamente
        try:
            for table in TABLE_ORDER:
                _copy_table(sconn, pg, table, truncate=args.truncate)
            pg.execute("SET session_replication_role = DEFAULT")
            _reset_sequences(pg)
            pg.commit()
        except Exception:
            pg.rollback()
            raise
        ok = _validate_counts(sconn, pg)

    sconn.close()
    print()
    if ok:
        print("✅ Migração concluída com contagens batendo.")
        return 0
    print("⚠ Contagens não batem — investigue antes de cortar SQLite.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
