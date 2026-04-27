"""SQLite — multitenancy + multi-project. Cada projeto tem seus dados isolados."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .config import DB_PATH

# ─── Schema (idempotente) ──────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    name          TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('admin','operacional','visualizador')),
    status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive')),
    credits       INTEGER NOT NULL DEFAULT 0,    -- 1 crédito = 1 pipeline. Admin recarrega.
    created_at    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS workspaces (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    owner_user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    rayyan_email        TEXT,
    rayyan_password     TEXT,
    rayyan_review_id    TEXT,
    openrouter_model    TEXT DEFAULT 'openai/gpt-4o-mini',
    openrouter_api_key  TEXT,    -- chave própria do workspace (multi-tenant: cada user usa
                                  -- sua própria quota OpenRouter, sem competir por rate limit).
                                  -- NULL = fallback pra chave global do .env do servidor.
    created_at          TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_user_id);

CREATE TABLE IF NOT EXISTS projects (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id     INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    topic            TEXT NOT NULL,
    objective        TEXT,
    years_window     INTEGER DEFAULT 5,
    target_articles  INTEGER DEFAULT 40,    -- Meta de artigos no Top N final
    review_type      TEXT NOT NULL DEFAULT 'systematic_review',  -- systematic_review | narrative_review
    criteria_md      TEXT,
    search_strings   TEXT,                  -- JSON {pubmed, scielo, scholar}
    sources          TEXT DEFAULT 'pubmed,scielo,scholar',
    status           TEXT NOT NULL DEFAULT 'draft',
                     -- draft | criteria_ready | searching | analyzing | done | failed
    error            TEXT,
    created_by       INTEGER REFERENCES users(id),
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);

-- Migração v3: adicionar target_articles em projetos antigos
-- (executado automaticamente pelo SQLITE quando coluna não existe)
CREATE INDEX IF NOT EXISTS idx_projects_workspace ON projects(workspace_id);

-- Articles agora escopados por (workspace_id, project_id, source, external_id)
CREATE TABLE IF NOT EXISTS articles (
    workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source       TEXT NOT NULL DEFAULT 'rayyan',   -- pubmed | scielo | scholar | rayyan
    external_id  TEXT NOT NULL,                    -- PMID, scielo_id, scholar_id, rayyan_id
    title        TEXT NOT NULL,
    authors      TEXT,
    year         TEXT,
    journal      TEXT,
    abstract     TEXT,
    doi          TEXT,
    external_url TEXT,
    raw_json     TEXT,
    is_duplicate INTEGER DEFAULT 0,                -- 1 se foi marcado como dup de outro
    scraped_at   TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (project_id, source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_articles_workspace ON articles(workspace_id);
CREATE INDEX IF NOT EXISTS idx_articles_project ON articles(project_id);
CREATE INDEX IF NOT EXISTS idx_articles_doi ON articles(doi) WHERE doi IS NOT NULL;

CREATE TABLE IF NOT EXISTS analyses (
    project_id          INTEGER NOT NULL,
    source              TEXT NOT NULL,
    external_id         TEXT NOT NULL,
    decision            TEXT NOT NULL,
    reason              TEXT NOT NULL,
    summary_pt          TEXT NOT NULL,
    criteria_matched    TEXT,
    criteria_violated   TEXT,
    quality_score       INTEGER,
    score_breakdown     TEXT,
    in_top_n            INTEGER DEFAULT 1,    -- 1 se está no Top N final, 0 se foi cortado por score
    model               TEXT NOT NULL,
    raw_response        TEXT,
    analyzed_at         TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (project_id, source, external_id),
    FOREIGN KEY (project_id, source, external_id) REFERENCES articles(project_id, source, external_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS double_checks (
    project_id     INTEGER NOT NULL,
    source         TEXT NOT NULL,
    external_id    TEXT NOT NULL,
    agrees         INTEGER NOT NULL,
    final_decision TEXT NOT NULL,
    explanation    TEXT NOT NULL,
    model          TEXT NOT NULL,
    raw_response   TEXT,
    checked_at     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (project_id, source, external_id),
    FOREIGN KEY (project_id, source, external_id) REFERENCES articles(project_id, source, external_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_decisions (
    project_id   INTEGER NOT NULL,
    source       TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    decision     TEXT NOT NULL CHECK (decision IN ('include','exclude','uncertain')),
    note         TEXT,
    decided_by   INTEGER REFERENCES users(id),
    decided_at   TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (project_id, source, external_id),
    FOREIGN KEY (project_id, source, external_id) REFERENCES articles(project_id, source, external_id) ON DELETE CASCADE
);

-- Jobs: rastreia cada etapa do pipeline (com status + progresso)
CREATE TABLE IF NOT EXISTS jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    step         TEXT NOT NULL,
                 -- discovery | search_pubmed | search_scielo | search_scholar
                 -- | dedup | analyze | double_check | verify
    status       TEXT NOT NULL,            -- queued | running | success | failed
    progress     INTEGER DEFAULT 0,
    message      TEXT,
    error        TEXT,
    started_at   TEXT,
    finished_at  TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_jobs_project ON jobs(project_id);

-- Histórico de chamadas LLM (uso da API OpenRouter)
CREATE TABLE IF NOT EXISTS llm_usage (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id      INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    project_id        INTEGER REFERENCES projects(id) ON DELETE SET NULL,
    user_id           INTEGER REFERENCES users(id) ON DELETE SET NULL,
    pipeline_step     TEXT NOT NULL,    -- discovery | analyze | double_check | rotate_terms
    model             TEXT NOT NULL,
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens      INTEGER NOT NULL DEFAULT 0,
    cost_usd          REAL NOT NULL DEFAULT 0,
    duration_ms       INTEGER,
    article_source    TEXT,             -- nulo em discovery; preenchido em analyze/dc
    article_external_id TEXT,
    article_title     TEXT,             -- snapshot resumido (até 200 chars)
    request_metadata  TEXT,             -- JSON: topic, target_articles, sources, review_type, etc.
    generation_id     TEXT,             -- resp.id da OpenRouter (chave do /api/v1/generation)
    cost_source       TEXT NOT NULL DEFAULT 'table',  -- 'table' (estimado) | 'openrouter' (real)
    created_at        TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_llm_usage_workspace ON llm_usage(workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_usage_project   ON llm_usage(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_usage_user      ON llm_usage(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_usage_step      ON llm_usage(pipeline_step);
"""


# ─── Migrações ─────────────────────────────────────────────────────────────


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(c[1] == column for c in cols)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone())


def _migrate_v1_to_v2_multitenancy(conn: sqlite3.Connection) -> bool:
    """v1 (single-tenant) → v2 (workspace_id em tudo)."""
    if not _table_exists(conn, "articles"):
        return False
    if _table_has_column(conn, "articles", "workspace_id"):
        return False  # já migrado
    conn.execute("PRAGMA foreign_keys = OFF")
    print("⚙ v1→v2: multitenancy…")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL, name TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('admin','operacional','visualizador')),
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive')),
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS workspaces (
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
            owner_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            rayyan_email TEXT, rayyan_password TEXT, rayyan_review_id TEXT,
            openrouter_model TEXT DEFAULT 'openai/gpt-4o-mini',
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    admin = conn.execute("SELECT id, email, name FROM users WHERE role='admin' LIMIT 1").fetchone()
    if not admin:
        raise RuntimeError("Sem admin para herdar dados antigos.")
    existing = conn.execute("SELECT id FROM workspaces WHERE owner_user_id=? LIMIT 1", (admin["id"],)).fetchone()
    if existing:
        ws_id = existing["id"]
    else:
        cur = conn.execute(
            "INSERT INTO workspaces (name, owner_user_id) VALUES (?, ?)",
            (f"Workspace de {admin['name']}", admin["id"]),
        )
        ws_id = cur.lastrowid

    for table, schema in [
        ("articles", """
            workspace_id INTEGER NOT NULL, rayyan_id TEXT NOT NULL,
            title TEXT NOT NULL, authors TEXT, year TEXT, journal TEXT,
            abstract TEXT, doi TEXT, rayyan_url TEXT, raw_json TEXT, scraped_at TEXT,
            PRIMARY KEY (workspace_id, rayyan_id)
        """),
        ("analyses", """
            workspace_id INTEGER NOT NULL, rayyan_id TEXT NOT NULL,
            decision TEXT NOT NULL, reason TEXT NOT NULL, summary_pt TEXT NOT NULL,
            criteria_matched TEXT, criteria_violated TEXT, quality_score INTEGER,
            score_breakdown TEXT, model TEXT NOT NULL, raw_response TEXT, analyzed_at TEXT,
            PRIMARY KEY (workspace_id, rayyan_id)
        """),
        ("double_checks", """
            workspace_id INTEGER NOT NULL, rayyan_id TEXT NOT NULL,
            agrees INTEGER NOT NULL, final_decision TEXT NOT NULL,
            explanation TEXT NOT NULL, model TEXT NOT NULL, raw_response TEXT, checked_at TEXT,
            PRIMARY KEY (workspace_id, rayyan_id)
        """),
    ]:
        if _table_exists(conn, table):
            conn.execute(f"ALTER TABLE {table} RENAME TO {table}_v1")
            conn.execute(f"CREATE TABLE {table} ({schema})")
            cols = ", ".join(c.split()[0] for c in schema.split(",") if not c.strip().startswith("PRIMARY"))
            cols_no_ws = cols.replace("workspace_id, ", "").strip()
            conn.execute(f"INSERT INTO {table} (workspace_id, {cols_no_ws}) SELECT {ws_id}, {cols_no_ws} FROM {table}_v1")
            conn.execute(f"DROP TABLE {table}_v1")
    conn.execute("PRAGMA foreign_keys = ON")
    print("✓ v1→v2 ok")
    return True


def _migrate_v2_to_v3_projects(conn: sqlite3.Connection) -> bool:
    """v2 (workspace) → v3 (workspace + project + source). Cria projeto default e migra dados."""
    if not _table_exists(conn, "articles"):
        return False
    if _table_has_column(conn, "articles", "project_id"):
        return False  # já migrado
    if not _table_has_column(conn, "articles", "workspace_id"):
        return False  # ainda em v1, deixa v1→v2 rodar antes

    conn.execute("PRAGMA foreign_keys = OFF")
    print("⚙ v2→v3: adicionando projects…")
    # Garantir tabela projects
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
            topic TEXT NOT NULL, objective TEXT, years_window INTEGER DEFAULT 5,
            criteria_md TEXT, search_strings TEXT,
            sources TEXT DEFAULT 'pubmed,scielo,scholar',
            status TEXT NOT NULL DEFAULT 'draft', error TEXT,
            created_by INTEGER REFERENCES users(id),
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
    """)
    # Para cada workspace existente, criar um projeto "default" e migrar artigos
    workspaces_rows = conn.execute("SELECT id, owner_user_id FROM workspaces").fetchall()
    project_map: dict[int, int] = {}  # workspace_id → project_id
    for ws in workspaces_rows:
        existing = conn.execute(
            "SELECT id FROM projects WHERE workspace_id=? AND topic='Projeto importado (Rayyan)' LIMIT 1",
            (ws["id"],),
        ).fetchone()
        if existing:
            project_map[ws["id"]] = existing["id"]
            continue
        cur = conn.execute(
            """INSERT INTO projects (workspace_id, topic, objective, criteria_md, status,
                                     sources, created_by)
               VALUES (?, ?, ?, ?, 'done', 'rayyan', ?)""",
            (ws["id"], "Projeto importado (Rayyan)",
             "Dados raspados do Rayyan na versão anterior.",
             "Critérios herdados do projeto anterior — ver docs/criterios-inclusao-exclusao.md",
             ws["owner_user_id"]),
        )
        project_map[ws["id"]] = cur.lastrowid

    # Migrar articles
    conn.execute("ALTER TABLE articles RENAME TO articles_v2")
    conn.executescript("""
        CREATE TABLE articles (
            workspace_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            source TEXT NOT NULL DEFAULT 'rayyan',
            external_id TEXT NOT NULL,
            title TEXT NOT NULL, authors TEXT, year TEXT, journal TEXT,
            abstract TEXT, doi TEXT, external_url TEXT, raw_json TEXT,
            is_duplicate INTEGER DEFAULT 0,
            scraped_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (project_id, source, external_id)
        );
    """)
    for ws_id, proj_id in project_map.items():
        conn.execute(
            f"""INSERT INTO articles (workspace_id, project_id, source, external_id,
                                       title, authors, year, journal, abstract,
                                       doi, external_url, raw_json, scraped_at)
                SELECT workspace_id, {proj_id}, 'rayyan', rayyan_id,
                       title, authors, year, journal, abstract,
                       doi, rayyan_url, raw_json, scraped_at
                FROM articles_v2 WHERE workspace_id={ws_id}"""
        )
    conn.execute("DROP TABLE articles_v2")

    # Migrar analyses
    if _table_exists(conn, "analyses"):
        conn.execute("ALTER TABLE analyses RENAME TO analyses_v2")
        conn.executescript("""
            CREATE TABLE analyses (
                project_id INTEGER NOT NULL, source TEXT NOT NULL, external_id TEXT NOT NULL,
                decision TEXT NOT NULL, reason TEXT NOT NULL, summary_pt TEXT NOT NULL,
                criteria_matched TEXT, criteria_violated TEXT, quality_score INTEGER,
                score_breakdown TEXT, model TEXT NOT NULL, raw_response TEXT, analyzed_at TEXT,
                PRIMARY KEY (project_id, source, external_id)
            );
        """)
        for ws_id, proj_id in project_map.items():
            conn.execute(
                f"""INSERT INTO analyses SELECT {proj_id}, 'rayyan', rayyan_id, decision, reason,
                    summary_pt, criteria_matched, criteria_violated, quality_score,
                    score_breakdown, model, raw_response, analyzed_at
                    FROM analyses_v2 WHERE workspace_id={ws_id}"""
            )
        conn.execute("DROP TABLE analyses_v2")

    # Migrar double_checks
    if _table_exists(conn, "double_checks"):
        conn.execute("ALTER TABLE double_checks RENAME TO dc_v2")
        conn.executescript("""
            CREATE TABLE double_checks (
                project_id INTEGER NOT NULL, source TEXT NOT NULL, external_id TEXT NOT NULL,
                agrees INTEGER NOT NULL, final_decision TEXT NOT NULL, explanation TEXT NOT NULL,
                model TEXT NOT NULL, raw_response TEXT, checked_at TEXT,
                PRIMARY KEY (project_id, source, external_id)
            );
        """)
        for ws_id, proj_id in project_map.items():
            conn.execute(
                f"""INSERT INTO double_checks SELECT {proj_id}, 'rayyan', rayyan_id, agrees,
                    final_decision, explanation, model, raw_response, checked_at
                    FROM dc_v2 WHERE workspace_id={ws_id}"""
            )
        conn.execute("DROP TABLE dc_v2")

    # Migrar user_decisions
    if _table_exists(conn, "user_decisions"):
        conn.execute("ALTER TABLE user_decisions RENAME TO ud_v2")
        conn.executescript("""
            CREATE TABLE user_decisions (
                project_id INTEGER NOT NULL, source TEXT NOT NULL, external_id TEXT NOT NULL,
                decision TEXT NOT NULL CHECK (decision IN ('include','exclude','uncertain')),
                note TEXT, decided_by INTEGER, decided_at TEXT,
                PRIMARY KEY (project_id, source, external_id)
            );
        """)
        for ws_id, proj_id in project_map.items():
            conn.execute(
                f"""INSERT INTO user_decisions SELECT {proj_id}, 'rayyan', rayyan_id, decision,
                    note, decided_by, decided_at
                    FROM ud_v2 WHERE workspace_id={ws_id}"""
            )
        conn.execute("DROP TABLE ud_v2")

    conn.execute("PRAGMA foreign_keys = ON")
    print(f"✓ v2→v3 ok ({len(project_map)} projetos default criados)")
    return True


def _migrate_v3_to_v4_target_articles(conn: sqlite3.Connection) -> bool:
    """v3 → v4: adiciona coluna target_articles em projects."""
    if not _table_exists(conn, "projects"):
        return False
    if _table_has_column(conn, "projects", "target_articles"):
        return False
    conn.execute("ALTER TABLE projects ADD COLUMN target_articles INTEGER DEFAULT 40")
    conn.execute("UPDATE projects SET target_articles=40 WHERE target_articles IS NULL")
    print("✓ v3→v4: target_articles adicionado")
    return True


def _migrate_v4_to_v5_in_top_n(conn: sqlite3.Connection) -> bool:
    """v4 → v5: adiciona coluna in_top_n em analyses (cutoff por score)."""
    if not _table_exists(conn, "analyses"):
        return False
    if _table_has_column(conn, "analyses", "in_top_n"):
        return False
    conn.execute("ALTER TABLE analyses ADD COLUMN in_top_n INTEGER DEFAULT 1")
    # Para projetos antigos, marca todos os incluídos como in_top_n=1
    conn.execute("UPDATE analyses SET in_top_n=1 WHERE decision='include'")
    print("✓ v4→v5: in_top_n adicionado")
    return True


def _migrate_v6_to_v7_review_type(conn: sqlite3.Connection) -> bool:
    """v6 → v7: adiciona coluna review_type em projects (systematic_review | narrative_review)."""
    if not _table_exists(conn, "projects"):
        return False
    if _table_has_column(conn, "projects", "review_type"):
        return False
    conn.execute(
        "ALTER TABLE projects ADD COLUMN review_type TEXT NOT NULL DEFAULT 'systematic_review'"
    )
    conn.execute("UPDATE projects SET review_type='systematic_review' WHERE review_type IS NULL")
    print("✓ v6→v7: review_type adicionado (default=systematic_review)")
    return True


def _migrate_v7_to_v8_real_cost(conn: sqlite3.Connection) -> bool:
    """v7 → v8: adiciona generation_id + cost_source em llm_usage para reconciliação real."""
    if not _table_exists(conn, "llm_usage"):
        return False
    if _table_has_column(conn, "llm_usage", "generation_id"):
        return False
    conn.execute("ALTER TABLE llm_usage ADD COLUMN generation_id TEXT")
    conn.execute("ALTER TABLE llm_usage ADD COLUMN cost_source TEXT NOT NULL DEFAULT 'table'")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_usage_genid ON llm_usage(generation_id)")
    print("✓ v7→v8: generation_id + cost_source")
    return True


def _migrate_v5_to_v6_credits(conn: sqlite3.Connection) -> bool:
    """v5 → v6: adiciona coluna credits em users (gestão de créditos por pipeline)."""
    if not _table_exists(conn, "users"):
        return False
    if _table_has_column(conn, "users", "credits"):
        return False
    conn.execute("ALTER TABLE users ADD COLUMN credits INTEGER NOT NULL DEFAULT 0")
    # Admins existentes recebem créditos ilimitados (9999) por padrão.
    # Outros usuários recebem 5 créditos iniciais — admin pode ajustar.
    conn.execute("UPDATE users SET credits=9999 WHERE role='admin'")
    conn.execute("UPDATE users SET credits=5 WHERE role!='admin'")
    print("✓ v5→v6: credits adicionado (admins=9999, demais=5)")
    return True


def _migrate_v8_to_v9_workspace_api_key(conn: sqlite3.Connection) -> bool:
    """v8 → v9: adiciona openrouter_api_key em workspaces (multi-tenant: cada
    workspace pode ter sua própria chave OpenRouter, evitando que pipelines de
    usuários diferentes compitam pelo mesmo rate-limit). NULL = fallback à
    chave global do servidor."""
    if not _table_exists(conn, "workspaces"):
        return False
    if _table_has_column(conn, "workspaces", "openrouter_api_key"):
        return False
    conn.execute("ALTER TABLE workspaces ADD COLUMN openrouter_api_key TEXT")
    print("✓ v8→v9: openrouter_api_key em workspaces (multi-tenant rate limit)")
    return True


def run_migrations(conn: sqlite3.Connection) -> None:
    _migrate_v1_to_v2_multitenancy(conn)
    conn.commit()
    _migrate_v2_to_v3_projects(conn)
    conn.commit()
    _migrate_v3_to_v4_target_articles(conn)
    conn.commit()
    _migrate_v4_to_v5_in_top_n(conn)
    conn.commit()
    _migrate_v5_to_v6_credits(conn)
    conn.commit()
    _migrate_v6_to_v7_review_type(conn)
    conn.commit()
    _migrate_v7_to_v8_real_cost(conn)
    conn.commit()
    _migrate_v8_to_v9_workspace_api_key(conn)
    conn.commit()


# ─── Connection helper ──────────────────────────────────────────────────────


def _unaccent(text):
    """Remove diacríticos (mamografía → mamografia) para busca tolerante."""
    if text is None:
        return None
    import unicodedata
    nfd = unicodedata.normalize("NFD", str(text))
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn").lower()


@contextmanager
def connect(db_path: Path = DB_PATH) -> Iterator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    # Função SQL custom: unaccent(text) — usada nas buscas tolerantes
    conn.create_function("unaccent", 1, _unaccent, deterministic=True)
    try:
        run_migrations(conn)
    except Exception:
        conn.rollback()
        raise
    conn.executescript(SCHEMA)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


# ─── Dataclasses ────────────────────────────────────────────────────────────


@dataclass
class Article:
    source: str
    external_id: str
    title: str
    authors: str | None = None
    year: str | None = None
    journal: str | None = None
    abstract: str | None = None
    doi: str | None = None
    external_url: str | None = None


@dataclass
class Analysis:
    source: str
    external_id: str
    decision: str
    reason: str
    summary_pt: str
    criteria_matched: list[str]
    criteria_violated: list[str]
    model: str
    quality_score: int | None = None
    score_breakdown: dict | None = None


@dataclass
class DoubleCheck:
    source: str
    external_id: str
    agrees: bool
    final_decision: str
    explanation: str
    model: str


# ─── CRUD: tudo escopado por (project_id, source, external_id) ──────────────


def upsert_article(conn: sqlite3.Connection, *, workspace_id: int, project_id: int,
                   art: Article, raw: dict) -> None:
    conn.execute(
        """
        INSERT INTO articles (workspace_id, project_id, source, external_id,
                              title, authors, year, journal, abstract, doi,
                              external_url, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(project_id, source, external_id) DO UPDATE SET
            title=excluded.title, authors=excluded.authors, year=excluded.year,
            journal=excluded.journal, abstract=excluded.abstract, doi=excluded.doi,
            external_url=excluded.external_url, raw_json=excluded.raw_json
        """,
        (workspace_id, project_id, art.source, art.external_id,
         art.title, art.authors, art.year, art.journal, art.abstract,
         art.doi, art.external_url, json.dumps(raw, ensure_ascii=False)),
    )


def insert_analysis(conn: sqlite3.Connection, project_id: int,
                    a: Analysis, raw_response: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO analyses
        (project_id, source, external_id, decision, reason, summary_pt,
         criteria_matched, criteria_violated, quality_score, score_breakdown,
         model, raw_response)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (project_id, a.source, a.external_id, a.decision, a.reason, a.summary_pt,
         json.dumps(a.criteria_matched, ensure_ascii=False),
         json.dumps(a.criteria_violated, ensure_ascii=False),
         a.quality_score,
         json.dumps(a.score_breakdown, ensure_ascii=False) if a.score_breakdown else None,
         a.model, raw_response),
    )


def insert_double_check(conn: sqlite3.Connection, project_id: int,
                        dc: DoubleCheck, raw_response: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO double_checks
        (project_id, source, external_id, agrees, final_decision, explanation, model, raw_response)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (project_id, dc.source, dc.external_id, int(dc.agrees),
         dc.final_decision, dc.explanation, dc.model, raw_response),
    )


def upsert_user_decision(conn: sqlite3.Connection, project_id: int,
                         source: str, external_id: str, decision: str,
                         *, note: str | None = None, user_id: int | None = None) -> None:
    if decision not in ("include", "exclude", "uncertain"):
        raise ValueError(f"Decisão inválida: {decision}")
    conn.execute(
        """
        INSERT INTO user_decisions (project_id, source, external_id, decision, note, decided_by, decided_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(project_id, source, external_id) DO UPDATE SET
            decision=excluded.decision, note=excluded.note,
            decided_by=excluded.decided_by, decided_at=datetime('now')
        """,
        (project_id, source, external_id, decision, note, user_id),
    )


def clear_user_decision(conn: sqlite3.Connection, project_id: int,
                        source: str, external_id: str) -> None:
    conn.execute(
        "DELETE FROM user_decisions WHERE project_id=? AND source=? AND external_id=?",
        (project_id, source, external_id),
    )


def articles_without_analysis(conn: sqlite3.Connection, project_id: int) -> list[Article]:
    rows = conn.execute(
        """
        SELECT a.* FROM articles a
        LEFT JOIN analyses an ON an.project_id=a.project_id AND an.source=a.source AND an.external_id=a.external_id
        WHERE a.project_id=? AND a.is_duplicate=0 AND an.external_id IS NULL
        """,
        (project_id,),
    ).fetchall()
    return [_row_to_article(r) for r in rows]


def excluded_without_double_check(
    conn: sqlite3.Connection,
    project_id: int,
    score_floor: int | None = None,
    score_ceiling: int | None = None,
    max_audits: int | None = None,
) -> list[tuple[Article, Analysis]]:
    """Retorna exclusões que ainda não foram auditadas por double-check.

    Se `score_floor` e `score_ceiling` forem dados, só audita exclusões com
    quality_score na faixa [floor, ceiling] (limítrofes — onde a IA pode ter
    errado). Score < floor são exclusões claramente ruins (não vale auditar);
    score ≥ ceiling teoricamente não deveriam ter sido excluídas.

    Se `max_audits` for dado, retorna no máximo N artigos, priorizando os MAIS
    PRÓXIMOS DO CORTE (score mais alto dentro da faixa) — esses são onde o
    risco de falso negativo é maior. NULLs vão por último.
    """
    score_clause = ""
    params: list = [project_id]
    if score_floor is not None and score_ceiling is not None:
        score_clause = " AND (an.quality_score IS NULL OR (an.quality_score >= ? AND an.quality_score < ?))"
        params.extend([score_floor, score_ceiling])
    limit_clause = ""
    if max_audits is not None and max_audits > 0:
        limit_clause = " LIMIT ?"
        params.append(max_audits)
    rows = conn.execute(
        f"""
        SELECT a.*, an.decision, an.reason, an.summary_pt, an.criteria_matched,
               an.criteria_violated, an.model AS an_model, an.quality_score
        FROM articles a
        JOIN analyses an ON an.project_id=a.project_id AND an.source=a.source AND an.external_id=a.external_id
        LEFT JOIN double_checks dc ON dc.project_id=a.project_id AND dc.source=a.source AND dc.external_id=a.external_id
        WHERE a.project_id=? AND an.decision='exclude' AND dc.external_id IS NULL{score_clause}
        ORDER BY an.quality_score DESC NULLS LAST, an.analyzed_at ASC{limit_clause}
        """,
        tuple(params),
    ).fetchall()
    out = []
    for r in rows:
        art = _row_to_article(r)
        an = Analysis(
            source=r["source"], external_id=r["external_id"],
            decision=r["decision"], reason=r["reason"], summary_pt=r["summary_pt"],
            criteria_matched=json.loads(r["criteria_matched"] or "[]"),
            criteria_violated=json.loads(r["criteria_violated"] or "[]"),
            model=r["an_model"], quality_score=r["quality_score"], score_breakdown=None,
        )
        out.append((art, an))
    return out


def project_stats(conn: sqlite3.Connection, project_id: int) -> dict[str, int]:
    return {
        "articles": conn.execute("SELECT COUNT(*) FROM articles WHERE project_id=? AND is_duplicate=0", (project_id,)).fetchone()[0],
        "duplicates": conn.execute("SELECT COUNT(*) FROM articles WHERE project_id=? AND is_duplicate=1", (project_id,)).fetchone()[0],
        "analyzed": conn.execute("SELECT COUNT(*) FROM analyses WHERE project_id=?", (project_id,)).fetchone()[0],
        "included": conn.execute("SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='include'", (project_id,)).fetchone()[0],
        "excluded": conn.execute("SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='exclude'", (project_id,)).fetchone()[0],
        "uncertain": conn.execute("SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='uncertain'", (project_id,)).fetchone()[0],
        "double_checked": conn.execute("SELECT COUNT(*) FROM double_checks WHERE project_id=?", (project_id,)).fetchone()[0],
        "disagreements": conn.execute("SELECT COUNT(*) FROM double_checks WHERE project_id=? AND agrees=0", (project_id,)).fetchone()[0],
        "user_overrides": conn.execute("SELECT COUNT(*) FROM user_decisions WHERE project_id=?", (project_id,)).fetchone()[0],
    }


def workspace_stats(conn: sqlite3.Connection, workspace_id: int) -> dict[str, int]:
    """Métricas agregadas do workspace inteiro (todos os projetos)."""
    return {
        "projects": conn.execute("SELECT COUNT(*) FROM projects WHERE workspace_id=?", (workspace_id,)).fetchone()[0],
        "articles": conn.execute("SELECT COUNT(*) FROM articles WHERE workspace_id=? AND is_duplicate=0", (workspace_id,)).fetchone()[0],
    }


# Compat: alguns módulos antigos chamam stats(conn, workspace_id) — mantém comportamento agregado
def stats(conn: sqlite3.Connection, workspace_id: int) -> dict[str, int]:
    return workspace_stats(conn, workspace_id)


def _row_to_article(r: sqlite3.Row) -> Article:
    return Article(
        source=r["source"], external_id=r["external_id"],
        title=r["title"], authors=r["authors"], year=r["year"],
        journal=r["journal"], abstract=r["abstract"],
        doi=r["doi"], external_url=r["external_url"],
    )


# ─── Jobs (status do pipeline) ─────────────────────────────────────────────


def create_job(conn: sqlite3.Connection, project_id: int, step: str) -> int:
    cur = conn.execute(
        """INSERT INTO jobs (project_id, step, status, started_at)
           VALUES (?, ?, 'running', datetime('now'))""",
        (project_id, step),
    )
    return cur.lastrowid


def update_job(conn: sqlite3.Connection, job_id: int, *,
               status: str | None = None, progress: int | None = None,
               message: str | None = None, error: str | None = None,
               finish: bool = False) -> None:
    fields, params = [], []
    if status: fields.append("status=?"); params.append(status)
    if progress is not None: fields.append("progress=?"); params.append(progress)
    if message is not None: fields.append("message=?"); params.append(message)
    if error is not None: fields.append("error=?"); params.append(error)
    if finish: fields.append("finished_at=datetime('now')")
    if fields:
        params.append(job_id)
        conn.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id=?", params)


def jobs_for_project(conn: sqlite3.Connection, project_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM jobs WHERE project_id=? ORDER BY id ASC", (project_id,)
    ).fetchall()
    return [dict(r) for r in rows]


# ─── LLM Usage (custos OpenRouter) ─────────────────────────────────────────


def insert_llm_usage(
    conn: sqlite3.Connection, *,
    workspace_id: int, project_id: int | None, user_id: int | None,
    pipeline_step: str, model: str,
    prompt_tokens: int, completion_tokens: int,
    cost_usd: float,
    duration_ms: int | None = None,
    article_source: str | None = None,
    article_external_id: str | None = None,
    article_title: str | None = None,
    request_metadata: dict | None = None,
    generation_id: str | None = None,
    cost_source: str = "table",
) -> int:
    cur = conn.execute(
        """INSERT INTO llm_usage
           (workspace_id, project_id, user_id, pipeline_step, model,
            prompt_tokens, completion_tokens, total_tokens, cost_usd, duration_ms,
            article_source, article_external_id, article_title, request_metadata,
            generation_id, cost_source)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (workspace_id, project_id, user_id, pipeline_step, model,
         int(prompt_tokens), int(completion_tokens),
         int(prompt_tokens) + int(completion_tokens),
         float(cost_usd), duration_ms,
         article_source, article_external_id,
         (article_title or "")[:200] or None,
         json.dumps(request_metadata, ensure_ascii=False) if request_metadata else None,
         generation_id or None, cost_source),
    )
    return int(cur.lastrowid or 0)


def update_llm_usage_real_cost(
    usage_id: int, *,
    cost_usd: float,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
) -> bool:
    """Reconcilia uma row de llm_usage com o gasto REAL vindo de /api/v1/generation.

    Sobrescreve cost_usd e (se fornecidos) os tokens nativos cobrados, e marca
    cost_source='openrouter'. Idempotente: se já está como 'openrouter', no-op.
    """
    try:
        with connect() as conn:
            row = conn.execute(
                "SELECT cost_source FROM llm_usage WHERE id=?", (usage_id,)
            ).fetchone()
            if not row or row["cost_source"] == "openrouter":
                return False
            if prompt_tokens is not None and completion_tokens is not None:
                conn.execute(
                    """UPDATE llm_usage
                       SET cost_usd=?, prompt_tokens=?, completion_tokens=?,
                           total_tokens=?, cost_source='openrouter'
                       WHERE id=?""",
                    (float(cost_usd), int(prompt_tokens), int(completion_tokens),
                     int(prompt_tokens) + int(completion_tokens), usage_id),
                )
            else:
                conn.execute(
                    "UPDATE llm_usage SET cost_usd=?, cost_source='openrouter' WHERE id=?",
                    (float(cost_usd), usage_id),
                )
        return True
    except Exception as e:
        print(f"  ⚠ update_llm_usage_real_cost falhou (não-fatal): {e}")
        return False


def record_llm_call(
    *, project_id: int, pipeline_step: str, model: str,
    prompt_tokens: int, completion_tokens: int,
    duration_ms: int | None = None,
    article: "Article | None" = None,
    extra_metadata: dict | None = None,
    generation_id: str | None = None,
) -> int | None:
    """Helper de alto nível: deriva workspace_id/user_id, calcula custo (estimado pela
    tabela), grava a row inicial, e retorna o id para que o pipeline possa reconciliar
    com o gasto real vindo de OpenRouter `/api/v1/generation` posteriormente.

    Idempotente em relação a falhas do próprio logging — qualquer exceção é
    swallowed pra nunca quebrar o pipeline (logging é secundário).
    """
    try:
        from .pricing import calc_cost_usd
        with connect() as conn:
            row = conn.execute(
                """SELECT p.workspace_id, p.created_by, p.topic, p.objective,
                          p.target_articles, p.review_type, p.sources, p.years_window,
                          w.owner_user_id
                   FROM projects p
                   JOIN workspaces w ON w.id = p.workspace_id
                   WHERE p.id = ?""",
                (project_id,),
            ).fetchone()
            if not row:
                return None
            workspace_id = row["workspace_id"]
            user_id = row["created_by"] or row["owner_user_id"]
            metadata = {
                "topic": row["topic"],
                "objective": row["objective"],
                "target_articles": row["target_articles"],
                "review_type": row["review_type"],
                "sources": row["sources"],
                "years_window": row["years_window"],
            }
            if extra_metadata:
                metadata.update(extra_metadata)
            cost = calc_cost_usd(model, prompt_tokens, completion_tokens)
            usage_id = insert_llm_usage(
                conn,
                workspace_id=workspace_id, project_id=project_id, user_id=user_id,
                pipeline_step=pipeline_step, model=model,
                prompt_tokens=prompt_tokens, completion_tokens=completion_tokens,
                cost_usd=cost, duration_ms=duration_ms,
                article_source=article.source if article else None,
                article_external_id=article.external_id if article else None,
                article_title=article.title if article else None,
                request_metadata=metadata,
                generation_id=generation_id,
                cost_source="table",
            )
            return usage_id
    except Exception as e:
        print(f"  ⚠ record_llm_call falhou (não-fatal): {e}")
        return None
