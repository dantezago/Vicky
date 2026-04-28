-- Vicky — Schema Postgres equivalente ao SQLite final (v9).
-- Tipos mantidos quase 1:1 com o SQLite pra que o código compartilhado funcione
-- sem transformações por linha:
--   * INTEGER pra "boolean" (0/1) — preserva comparações `=0`/`=1` do código.
--   * TEXT pra JSON serializado — preserva `json.loads(r["x"])` do código.
--   * TEXT pra timestamps no formato ISO sem fuso (string "YYYY-MM-DD HH24:MI:SS")
--     — preserva o display textual existente. Default usa `to_char(now(),...)`.
-- Pra rodar: psql $DATABASE_URL -f schema_pg.sql

BEGIN;

CREATE EXTENSION IF NOT EXISTS unaccent;

-- Equivalente do nosso `_unaccent` do SQLite (lowercase + sem diacríticos).
-- Nome distinto da função `unaccent` da extensão pra evitar shadow recursivo.
-- O adapter `vicky.db` traduz `unaccent(` → `unaccent_ci(` em SQL ao usar Postgres.
CREATE OR REPLACE FUNCTION unaccent_ci(t text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT AS
$$ SELECT lower(public.unaccent(t)) $$;

-- ─── users ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            BIGSERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    name          TEXT NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('admin','operacional','visualizador')),
    status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive')),
    credits       INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
);

-- ─── workspaces ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workspaces (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT NOT NULL,
    owner_user_id       BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    rayyan_email        TEXT,
    rayyan_password     TEXT,
    rayyan_review_id    TEXT,
    openrouter_model    TEXT DEFAULT 'openai/gpt-4o-mini',
    openrouter_api_key  TEXT,
    created_at          TEXT DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
);
CREATE INDEX IF NOT EXISTS idx_workspaces_owner ON workspaces(owner_user_id);

-- ─── projects ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS projects (
    id               BIGSERIAL PRIMARY KEY,
    workspace_id     BIGINT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    topic            TEXT NOT NULL,
    objective        TEXT,
    years_window     INTEGER DEFAULT 5,
    target_articles  INTEGER DEFAULT 40,
    review_type      TEXT NOT NULL DEFAULT 'systematic_review',
    rigidity_mode    TEXT NOT NULL DEFAULT 'padrao',  -- padrao | elite (só sistemática)
    topic_maturity   TEXT,                            -- high | moderate | emerging
    criteria_md      TEXT,
    search_strings   TEXT,
    sources          TEXT DEFAULT 'pubmed,scielo,scholar',
    status           TEXT NOT NULL DEFAULT 'draft',
    error            TEXT,
    created_by       BIGINT REFERENCES users(id),
    created_at       TEXT DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
    updated_at       TEXT DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
);
-- Idempotência pra DBs pré-v11
ALTER TABLE projects ADD COLUMN IF NOT EXISTS rigidity_mode TEXT NOT NULL DEFAULT 'padrao';
ALTER TABLE projects ADD COLUMN IF NOT EXISTS topic_maturity TEXT;
CREATE INDEX IF NOT EXISTS idx_projects_workspace ON projects(workspace_id);

-- ─── articles ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS articles (
    workspace_id BIGINT  NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    project_id   BIGINT  NOT NULL REFERENCES projects(id)   ON DELETE CASCADE,
    source       TEXT    NOT NULL DEFAULT 'rayyan',
    external_id  TEXT    NOT NULL,
    title        TEXT    NOT NULL,
    authors      TEXT,
    year         TEXT,
    journal      TEXT,
    abstract     TEXT,
    doi          TEXT,
    external_url TEXT,
    raw_json     TEXT,
    is_duplicate INTEGER DEFAULT 0,
    search_string_id BIGINT,    -- qual substring trouxe este artigo (estratégia multi-string)
    scraped_at   TEXT    DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
    PRIMARY KEY (project_id, source, external_id)
);
-- Idempotência: ALTER ADD COLUMN IF NOT EXISTS pra DBs que já existiam pré-v10
ALTER TABLE articles ADD COLUMN IF NOT EXISTS search_string_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_articles_workspace ON articles(workspace_id);
CREATE INDEX IF NOT EXISTS idx_articles_project   ON articles(project_id);
CREATE INDEX IF NOT EXISTS idx_articles_doi       ON articles(doi) WHERE doi IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_articles_search_string ON articles(search_string_id) WHERE search_string_id IS NOT NULL;

-- ─── search_string_stats ──────────────────────────────────────────────────
-- 1 linha por substring de busca. Estratégia multi-string: discovery gera N
-- substrings por source; após análise, top-K com maior inclusion_rate
-- ganham budget extra; strings sem inclusão são "burned".
CREATE TABLE IF NOT EXISTS search_string_stats (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    string_text TEXT NOT NULL,
    position INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    collected_count INTEGER NOT NULL DEFAULT 0,
    analyzed_count INTEGER NOT NULL DEFAULT 0,
    included_count INTEGER NOT NULL DEFAULT 0,
    max_results_budget INTEGER NOT NULL,
    expanded INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
    UNIQUE (project_id, source, position)
);
CREATE INDEX IF NOT EXISTS idx_sss_project_source_status ON search_string_stats(project_id, source, status);

-- ─── analyses ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analyses (
    project_id        BIGINT  NOT NULL,
    source            TEXT    NOT NULL,
    external_id       TEXT    NOT NULL,
    decision          TEXT    NOT NULL,
    reason            TEXT    NOT NULL,
    summary_pt        TEXT    NOT NULL,
    criteria_matched  TEXT,
    criteria_violated TEXT,
    quality_score     INTEGER,
    score_breakdown   TEXT,
    in_top_n          INTEGER DEFAULT 1,
    model             TEXT    NOT NULL,
    raw_response      TEXT,
    analyzed_at       TEXT    DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
    PRIMARY KEY (project_id, source, external_id),
    FOREIGN KEY (project_id, source, external_id)
        REFERENCES articles(project_id, source, external_id) ON DELETE CASCADE
);

-- ─── double_checks ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS double_checks (
    project_id      BIGINT  NOT NULL,
    source          TEXT    NOT NULL,
    external_id     TEXT    NOT NULL,
    agrees          INTEGER NOT NULL,
    final_decision  TEXT    NOT NULL,
    explanation     TEXT    NOT NULL,
    model           TEXT    NOT NULL,
    raw_response    TEXT,
    checked_at      TEXT    DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
    PRIMARY KEY (project_id, source, external_id),
    FOREIGN KEY (project_id, source, external_id)
        REFERENCES articles(project_id, source, external_id) ON DELETE CASCADE
);

-- ─── user_decisions ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_decisions (
    project_id   BIGINT NOT NULL,
    source       TEXT   NOT NULL,
    external_id  TEXT   NOT NULL,
    decision     TEXT   NOT NULL CHECK (decision IN ('include','exclude','uncertain')),
    note         TEXT,
    decided_by   BIGINT REFERENCES users(id),
    decided_at   TEXT   DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'),
    PRIMARY KEY (project_id, source, external_id),
    FOREIGN KEY (project_id, source, external_id)
        REFERENCES articles(project_id, source, external_id) ON DELETE CASCADE
);

-- ─── jobs ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobs (
    id           BIGSERIAL PRIMARY KEY,
    project_id   BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    step         TEXT NOT NULL,
    status       TEXT NOT NULL,
    progress     INTEGER DEFAULT 0,
    message      TEXT,
    error        TEXT,
    started_at   TEXT,
    finished_at  TEXT,
    created_at   TEXT DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
);
CREATE INDEX IF NOT EXISTS idx_jobs_project ON jobs(project_id);

-- ─── llm_usage ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS llm_usage (
    id                  BIGSERIAL PRIMARY KEY,
    workspace_id        BIGINT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    project_id          BIGINT REFERENCES projects(id) ON DELETE SET NULL,
    user_id             BIGINT REFERENCES users(id)    ON DELETE SET NULL,
    pipeline_step       TEXT NOT NULL,
    model               TEXT NOT NULL,
    prompt_tokens       INTEGER NOT NULL DEFAULT 0,
    completion_tokens   INTEGER NOT NULL DEFAULT 0,
    total_tokens        INTEGER NOT NULL DEFAULT 0,
    cost_usd            DOUBLE PRECISION NOT NULL DEFAULT 0,
    duration_ms         INTEGER,
    article_source      TEXT,
    article_external_id TEXT,
    article_title       TEXT,
    request_metadata    TEXT,
    generation_id       TEXT,
    cost_source         TEXT NOT NULL DEFAULT 'table',
    created_at          TEXT NOT NULL DEFAULT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
);
CREATE INDEX IF NOT EXISTS idx_llm_usage_workspace ON llm_usage(workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_usage_project   ON llm_usage(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_usage_user      ON llm_usage(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_llm_usage_step      ON llm_usage(pipeline_step);
CREATE INDEX IF NOT EXISTS idx_llm_usage_genid     ON llm_usage(generation_id);

COMMIT;
