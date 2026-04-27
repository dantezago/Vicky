"""Adapter de DB — escolhe SQLite ou Postgres via DATABASE_URL.

Por padrão (sem DATABASE_URL setada): usa SQLite em ~/.local/share/vicky/vicky.db
(comportamento legacy preservado).

Postgres: defina `DATABASE_URL=postgresql://user:pass@host:port/dbname` no .env.

API pública:
    from vicky.db import connect, is_postgres

    with connect() as conn:
        cur = conn.execute("SELECT * FROM users WHERE id=?", (1,))
        row = cur.fetchone()
        print(row["name"])    # acesso por nome
        print(row[0])         # acesso por índice (compat com .fetchone()[0] do COUNT)

    # INSERT que precisa do id da row criada (uso só em tabelas com PK `id`):
    new_id = conn.execute_returning_id(
        "INSERT INTO jobs (project_id, step, status) VALUES (?, ?, 'running')",
        (project_id, step),
    )

O adapter traduz no momento da execução (apenas no backend Postgres):
    ?                       → %s
    datetime('now')         → to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
    unaccent(               → unaccent_ci(
    strftime('%s', x)       → EXTRACT(EPOCH FROM (x)::timestamp)::bigint
    julianday('now')        → (EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC'))/86400.0)
    julianday(x)            → (EXTRACT(EPOCH FROM (x)::timestamp)/86400.0)
    INSERT OR REPLACE INTO  → erro explícito (use ON CONFLICT ... DO UPDATE)

No SQLite, nenhuma tradução acontece — comportamento idêntico ao legado.
"""

from __future__ import annotations

import os
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .config import DB_PATH

# ─── Detecção de backend ────────────────────────────────────────────────────


def _database_url() -> str:
    return os.getenv("DATABASE_URL") or f"sqlite:///{DB_PATH}"


def is_postgres() -> bool:
    return _database_url().startswith(("postgres://", "postgresql://"))


def backend() -> str:
    return "postgres" if is_postgres() else "sqlite"


# ─── Função de busca tolerante (compartilhada SQLite ↔ Postgres) ───────────


def _unaccent_lower(text: Any) -> str | None:
    """Mesma semântica de unaccent_ci(text) no Postgres: lowercase + sem diacríticos."""
    if text is None:
        return None
    nfd = unicodedata.normalize("NFD", str(text))
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn").lower()


# ─── Row híbrida: dict + indexável por inteiro ─────────────────────────────


class _HybridRow(dict):
    """Suporta r['x'] (dict) E r[0] (índice posicional).

    Necessário pra que código existente como `cur.fetchone()[0]` (COUNT(*))
    e `row["name"]` continuem funcionando independente do backend.
    """

    __slots__ = ("_values",)

    def __init__(self, mapping, values):
        super().__init__(mapping)
        self._values = list(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError):
            return default


def _pg_row_factory(cursor):
    """Factory de row pra psycopg3 que devolve _HybridRow."""
    desc = cursor.description
    if not desc:
        return lambda values: values
    cols = [d.name for d in desc]

    def make(values):
        return _HybridRow(zip(cols, values), values)

    return make


# ─── Tradução SQLite → Postgres ────────────────────────────────────────────

_RE_DATETIME_NOW = re.compile(r"datetime\(\s*'now'\s*\)", re.IGNORECASE)
_RE_UNACCENT = re.compile(r"\bunaccent\s*\(", re.IGNORECASE)
_RE_INSERT_OR_REPLACE = re.compile(r"\bINSERT\s+OR\s+REPLACE\b", re.IGNORECASE)
_RE_PRAGMA = re.compile(r"\bPRAGMA\s+\w+", re.IGNORECASE)


def _replace_func_calls(sql: str, fname: str, transform) -> str:
    """Substitui chamadas `fname(...)` por `transform(arg_string)`.

    Faz balanceamento de parênteses respeitando aspas simples — assim casos
    como `strftime('%s', MAX(j.x))` ou `julianday('now')` são suportados,
    diferente de regex com `[^)]+` que parava no primeiro `)`.
    """
    pat = re.compile(rf"\b{re.escape(fname)}\s*\(", re.IGNORECASE)
    out: list[str] = []
    i = 0
    while i < len(sql):
        m = pat.search(sql, i)
        if not m:
            out.append(sql[i:])
            break
        out.append(sql[i:m.start()])
        depth = 1
        j = m.end()
        in_q = False
        while j < len(sql) and depth > 0:
            ch = sql[j]
            if ch == "'":
                in_q = not in_q
            elif not in_q:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        break
            j += 1
        if j >= len(sql):
            out.append(sql[m.start():])
            break
        out.append(transform(sql[m.end():j]))
        i = j + 1
    return "".join(out)


def _strftime_transform(inner: str) -> str:
    """`strftime('%s', X)` → `EXTRACT(EPOCH FROM (X)::timestamp)::bigint`.

    Só `'%s'` (epoch) é traduzido — outros formatos não são usados no projeto.
    """
    m = re.match(r"\s*'%s'\s*,\s*", inner)
    if not m:
        return f"strftime({inner})"
    expr = inner[m.end():].strip()
    return f"EXTRACT(EPOCH FROM ({expr})::timestamp)::bigint"


def _julianday_transform(inner: str) -> str:
    """`julianday(X)` → fração de dias derivada do epoch.

    Só usamos diferenças entre dois `julianday()`, então qualquer função
    monótona com unidade de dia funciona — não precisa ser o Julian Day real.
    """
    expr = inner.strip()
    if expr.lower() == "'now'":
        return "(EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC'))/86400.0)"
    return f"(EXTRACT(EPOCH FROM ({expr})::timestamp)/86400.0)"


def translate_sql_pg(sql: str) -> str:
    """Reescreve SQL escrito em dialeto SQLite pra dialeto Postgres."""
    if _RE_INSERT_OR_REPLACE.search(sql):
        raise ValueError(
            "INSERT OR REPLACE não é suportado em Postgres. "
            "Use INSERT … ON CONFLICT (cols) DO UPDATE SET …"
        )
    sql = _replace_func_calls(sql, "strftime", _strftime_transform)
    sql = _replace_func_calls(sql, "julianday", _julianday_transform)
    sql = _RE_DATETIME_NOW.sub(
        "to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')", sql
    )
    sql = _RE_UNACCENT.sub("unaccent_ci(", sql)
    # Placeholder: ? → %s. % literal vira %% pra escapar do paramstyle pyformat.
    out_chars: list[str] = []
    in_squote = False
    for ch in sql:
        if ch == "'":
            in_squote = not in_squote
            out_chars.append(ch)
        elif ch == "%" and not in_squote:
            out_chars.append("%%")
        elif ch == "?" and not in_squote:
            out_chars.append("%s")
        else:
            out_chars.append(ch)
    return "".join(out_chars)


# ─── Wrappers de Cursor e Connection ──────────────────────────────────────


class _CursorWrapper:
    """Camada fina sobre cursores nativos para uniformizar a API."""

    __slots__ = ("_cur", "_backend")

    def __init__(self, raw, backend: str):
        self._cur = raw
        self._backend = backend

    @property
    def lastrowid(self):
        # Só significa algo no SQLite. No Postgres use execute_returning_id().
        return getattr(self._cur, "lastrowid", None)

    @property
    def rowcount(self):
        return getattr(self._cur, "rowcount", -1)

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def __iter__(self):
        return iter(self._cur)


class _ConnWrapper:
    """Conexão com API uniforme pra SQLite e Postgres."""

    __slots__ = ("_raw", "_backend")

    def __init__(self, raw, backend: str):
        self._raw = raw
        self._backend = backend

    @property
    def backend(self) -> str:
        return self._backend

    def execute(self, sql: str, params: Any = ()) -> _CursorWrapper:
        if self._backend == "postgres":
            translated = translate_sql_pg(sql)
            cur = self._raw.cursor()
            cur.execute(translated, tuple(params) if params else None)
            return _CursorWrapper(cur, self._backend)
        cur = self._raw.execute(sql, params)
        return _CursorWrapper(cur, self._backend)

    def executescript(self, script: str) -> None:
        if self._backend == "postgres":
            translated = translate_sql_pg(script)
            with self._raw.cursor() as cur:
                cur.execute(translated)
            return
        self._raw.executescript(script)

    def execute_returning_id(self, sql: str, params: Any = ()) -> int:
        """Executa um INSERT e retorna o id da row criada.

        Funciona em ambos backends. No Postgres adiciona `RETURNING id` se o
        SQL não trouxer. Use só em tabelas cuja PK seja a coluna `id`.
        """
        if self._backend == "postgres":
            translated = translate_sql_pg(sql)
            if "returning" not in translated.lower():
                translated = translated.rstrip(" ;\n") + " RETURNING id"
            cur = self._raw.cursor()
            cur.execute(translated, tuple(params) if params else None)
            row = cur.fetchone()
            return int(row[0]) if row else 0
        cur = self._raw.execute(sql, params)
        return int(cur.lastrowid or 0)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()


# ─── Conexão SQLite ────────────────────────────────────────────────────────


def _connect_sqlite(url: str) -> _ConnWrapper:
    # url = "sqlite:///<absolute_path>" ou "sqlite://<relative>"
    path_str = url.replace("sqlite:///", "/").replace("sqlite://", "")
    if not path_str:
        path_str = str(DB_PATH)
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = sqlite3.connect(path, timeout=10.0)
    raw.row_factory = sqlite3.Row
    raw.execute("PRAGMA journal_mode = WAL")
    raw.execute("PRAGMA foreign_keys = ON")
    raw.create_function("unaccent", 1, _unaccent_lower, deterministic=True)
    raw.create_function("unaccent_ci", 1, _unaccent_lower, deterministic=True)
    return _ConnWrapper(raw, "sqlite")


# ─── Conexão Postgres ──────────────────────────────────────────────────────


_PG_ADAPTERS_REGISTERED = False


def _ensure_pg_adapters() -> None:
    """Registra dumpers/loaders globais pra que NUMERIC/DOUBLE PRECISION
    venham como float (compatível com `json.dumps` usado em endpoints JSON).

    Sem isso, psycopg3 retorna `Decimal` que quebra `json.dumps` em rotas
    como /projetos/{id}/status.
    """
    global _PG_ADAPTERS_REGISTERED
    if _PG_ADAPTERS_REGISTERED:
        return
    import psycopg
    from psycopg.types.numeric import NumericLoader

    class _FloatNumericLoader(NumericLoader):
        def load(self, data):  # type: ignore[override]
            return float(super().load(data))

    psycopg.adapters.register_loader("numeric", _FloatNumericLoader)
    _PG_ADAPTERS_REGISTERED = True


def _connect_postgres(url: str) -> _ConnWrapper:
    import psycopg

    _ensure_pg_adapters()
    raw = psycopg.connect(url, autocommit=False, row_factory=_pg_row_factory)
    return _ConnWrapper(raw, "postgres")


# ─── API pública: connect() ────────────────────────────────────────────────


@contextmanager
def connect() -> Iterator[_ConnWrapper]:
    url = _database_url()
    if url.startswith(("postgres://", "postgresql://")):
        wrapper = _connect_postgres(url)
        try:
            yield wrapper
            wrapper.commit()
        except Exception:
            wrapper.rollback()
            raise
        finally:
            wrapper.close()
        return

    # SQLite — preserva fluxo legado (migrations + SCHEMA idempotente).
    wrapper = _connect_sqlite(url)
    try:
        from . import storage as _storage  # circular import controlado
        _storage.run_migrations(wrapper._raw)
        wrapper._raw.executescript(_storage.SCHEMA)
        yield wrapper
        wrapper.commit()
    except Exception:
        wrapper.rollback()
        raise
    finally:
        wrapper.close()


# ─── Bootstrap do schema Postgres ──────────────────────────────────────────


def init_pg_schema() -> None:
    """Executa schema_pg.sql num banco Postgres vazio. Idempotente."""
    if not is_postgres():
        raise RuntimeError(
            "init_pg_schema() só faz sentido com DATABASE_URL apontando pra Postgres."
        )
    schema_path = Path(__file__).parent / "schema_pg.sql"
    sql = schema_path.read_text(encoding="utf-8")
    import psycopg

    with psycopg.connect(_database_url(), autocommit=True) as raw:
        with raw.cursor() as cur:
            cur.execute(sql)


# ─── Tratamento uniforme de erros ──────────────────────────────────────────


def is_unique_violation(exc: BaseException) -> bool:
    """True se `exc` indica violação de UNIQUE/PK em qualquer backend.

    Use no lugar de `except sqlite3.IntegrityError` em código que precisa
    rodar tanto em SQLite quanto em Postgres.
    """
    if isinstance(exc, sqlite3.IntegrityError):
        return True
    try:
        from psycopg import errors as _pgerr
    except ImportError:
        return False
    return isinstance(exc, _pgerr.UniqueViolation)
