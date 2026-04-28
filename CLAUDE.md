# Vicky

Plataforma multi-tenant de triagem assistida por IA para revisões sistemáticas de literatura científica.
Pega um tema de pesquisa, gera critérios PICO automaticamente, busca em PubMed/SciELO/Google Scholar,
triage com LLM e ranqueia o Top N final por Quality Score (0–100).

> **Alvo de usuário:** estudantes de iniciação científica e pesquisadoras que precisam reduzir 500+ artigos a uma lista de elite metodológica.

---

## Quick start

```bash
# 1. Setup (uma vez) — usa uv (Python 3.12)
uv venv --python 3.12
uv pip install -e .
.venv/bin/playwright install chromium    # opcional: só pra Scholar

# 2. Credenciais — edite ~/.config/vicky/.env
mkdir -p ~/.config/vicky && chmod 700 ~/.config/vicky
cat > ~/.config/vicky/.env <<'EOF'
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openai/gpt-4o-mini
RAYYAN_EMAIL=admin@example.com
RAYYAN_PASSWORD=anything
RAYYAN_REVIEW_ID=0
DATABASE_URL=postgresql://vicky:vicky_dev@localhost:5433/vicky
EOF
chmod 600 ~/.config/vicky/.env

# 3. Subir Postgres local (Docker) e aplicar schema
make pg-up        # sobe container e aguarda pg_isready
make pg-init      # aplica src/vicky/schema_pg.sql (idempotente)

# 4. Criar admin inicial
.venv/bin/vicky create-user --email vickyangel@gmail.com --name Victoria --role admin --password vicky2026

# 5. Subir o servidor
.venv/bin/vicky serve              # http://127.0.0.1:8000
.venv/bin/vicky serve --port 8765  # outra porta
```

**Smoke test rápido após mudanças:** `.venv/bin/vicky doctor` (testa config + chamada OpenRouter).

> **Sem `DATABASE_URL` no `.env`?** O app cai pro fallback SQLite em `~/.local/share/vicky/vicky.db`. Útil pra dev offline, mas todos os ambientes oficiais rodam Postgres.

---

## Identidade visual

- **Marca:** rosa/magenta vibrante. Tailwind extends `brand-50..900` em [base.html](src/vicky/web/templates/base.html).
- **Cor principal:** `brand-500` (#ec4899) · CTAs em `brand-600` (#db2777) com hover `brand-700` (#be185d)
- **Background:** `slate-50` (cinza muito claro)
- **Cards:** brancos, `rounded-xl`, `border-slate-200/70`, `shadow-card` (sombra sutil)
- **Fonte:** Inter (Google Fonts), com `tabular-nums` em todos os números
- **Ícones:** SVG inline (Heroicons-style), não usa font-icon library
- **Status:** Emerald (incluir/sucesso), Rose (excluir/erro), Amber (incerto/atenção), Blue (info), Violet (premium/score 100)
- **Logo:** quadrado arredondado com gradient `from-brand-500 to-brand-700` e letra "V" branca bold
- **Layout:** sidebar fixa 256px à esquerda, header 64px com avatar dropdown, main com padding generoso (`px-6 lg:px-10`)
- **Tom:** SaaS profissional tipo Linear/Notion, em português brasileiro técnico mas acolhedor

---

## Arquitetura em 1 minuto

```
[1. discovery]   ── LLM gera critérios PICO + 3 search strings
     ↓
[2. search]      ── PubMed (E-utilities) + SciELO (HTML) + Scholar (Playwright)
     ↓
[3. expand_search] ── fallback: estrita → ampliada → tema cru se vier pouco resultado
     ↓
[4. dedup]       ── colapsa por DOI / external_id
     ↓
[5. analyze]     ── LLM decide include/exclude/uncertain + Quality Score 0–100
     ↓
[6. double_check] ── 2ª passada: audita exclusões, marca discordâncias
     ↓
[7. finalize]    ── corte por quality_score; marca `in_top_n` nos sobreviventes
     ↓
[8. verify]      ── checklist final que confere que tudo rodou

Top N final = MIN(target_articles, total_incluídos) ranqueado por quality_score
```

**Concorrência multi-usuário:** Postgres (default) ou SQLite em modo WAL (fallback) + 1 task asyncio por projeto. Múltiplos usuários podem rodar pipelines em paralelo no mesmo processo uvicorn.

---

## Schema do banco

Backend é escolhido em runtime por `DATABASE_URL`:
- **Postgres** (default em prod/dev): schema vivo em [src/vicky/schema_pg.sql](src/vicky/schema_pg.sql), aplicado via `make pg-init`.
- **SQLite** (fallback): `~/.local/share/vicky/vicky.db`, schema declarativo em [storage.py](src/vicky/storage.py) + migrations v1→v9 aplicadas no `connect()`.

O adapter [src/vicky/db.py](src/vicky/db.py) traduz dialetos no momento do `execute` (`?` → `%s`, `datetime('now')`, `strftime('%s', X)`, `julianday(X)`, `unaccent(...)`) — código de negócio escreve SQL "estilo SQLite" e funciona nos dois.

```
users (id, email, password_hash, name, role[admin|operacional|visualizador], status, created_at)
  └─ workspaces (id, name, owner_user_id, rayyan_email/password/review_id, openrouter_model)
       └─ projects (id, workspace_id, topic, objective, years_window, target_articles=40,
                    criteria_md, search_strings[JSON], sources, status, created_by)
            ├─ articles      (PK: project_id, source, external_id) ─ workspace_id (denorm),
            │                  title, authors, year, journal, abstract, doi, external_url,
            │                  raw_json, is_duplicate, scraped_at
            ├─ analyses      (FK→articles, decision[include|exclude|uncertain],
            │                  reason, summary_pt, criteria_matched/violated[JSON],
            │                  quality_score 0-100, score_breakdown[JSON], model,
            │                  in_top_n[bool] ── 1 se sobreviveu ao corte por quality_score)
            ├─ double_checks (FK→articles, agrees BOOL, final_decision, explanation, model)
            ├─ user_decisions (FK→articles, decision, note, decided_by) ── overrides manuais
            └─ jobs          (step, status, progress 0-100, message, error, started/finished_at)
```

**Convenção de isolamento:** TODA query precisa filtrar por `workspace_id` ou `project_id`. Nunca confie em foreign key sem WHERE explícito. A função `get_project_for_workspace()` em `app.py` é o único caminho seguro para carregar projeto na rota.

**Migrações automáticas (SQLite):** rodam ao primeiro `connect()`. Detectam versão do schema via `_table_has_column()` e aplicam só o que falta. Backup manual antes de mudanças destrutivas:
```bash
cp ~/.local/share/vicky/vicky.db{,.backup-$(date +%s)}
```

**Postgres:** schema é estático em [schema_pg.sql](src/vicky/schema_pg.sql) — não tem migration runtime, mas o `make deploy` aplica `init_pg_schema()` em prod automaticamente após o rebuild (idempotente). Ver "Convenção de mudanças de schema" logo abaixo.

### Convenção de mudanças de schema (SQLite ↔ Postgres)

Toda mudança **aditiva** (ADD COLUMN/TABLE/INDEX) requer 3 coisas em PR único:

1. **Migration SQLite** em [storage.py](src/vicky/storage.py): nova `_migrate_vN_to_vN+1_*` registrada em `_run_migrations()`.
2. **Coluna no `CREATE TABLE`** correspondente em [schema_pg.sql](src/vicky/schema_pg.sql).
3. **`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`** logo abaixo do `CREATE TABLE` em `schema_pg.sql`. Sem isso, DBs já criados em prod não recebem a coluna.

Antes de commitar: `make check-schema` confere que toda migration SQLite tem o ALTER IF NOT EXISTS correspondente. `make deploy` roda `init_pg_schema()` em prod, então mudanças aditivas merge'd na main refletem em produção no próximo deploy automaticamente.

**Mudanças destrutivas/estruturais** (DROP/RENAME COLUMN, mudar tipo, adicionar NOT NULL exigindo backfill, mudar PK/UNIQUE): não cobertas por `IF NOT EXISTS`. Caso a caso: SQL manual aplicado via `make psql` (local) e `make prod-pg-init` ou psql direto em prod com autorização explícita. Documentar a mudança no commit.

Versões SQLite já aplicadas:
- v1→v2: adicionou `workspace_id` (multitenancy)
- v2→v3: adicionou `project_id` + composite PK + `source/external_id` (multi-source)
- v3→v4: adicionou `target_articles` (meta de Top N)
- v4→v5: adicionou `in_top_n` em `analyses` (marca quais incluídos sobrevivem ao corte por quality_score)
- v5→v6: adicionou `credits` em `users`
- v6→v7: adicionou `review_type` em `projects`
- v7→v8: adicionou `cost_source`/`generation_id` em `llm_usage` (custo real OpenRouter)
- v8→v9: adicionou `openrouter_api_key` em `workspaces`
- v9→v10: adicionou `search_string_id` em `articles` (estratégia multi-string)
- v10→v11: adicionou `rigidity_mode` + `topic_maturity` em `projects`

---

## Postgres — setup, seed e workflow

Backend default em runtime. Container roda via [docker-compose.yml](docker-compose.yml) na porta **5433** (não 5432, pra não colidir com Postgres do host se houver).

### Conexão

```
host:    localhost
port:    5433
user:    vicky
pass:    vicky_dev
db:      vicky
URL:     postgresql://vicky:vicky_dev@localhost:5433/vicky
JDBC:    jdbc:postgresql://localhost:5433/vicky   (DataGrip/IntelliJ)
```

### Comandos `make`

| Comando | O que faz |
|---|---|
| `make help` | Lista todos os targets |
| `make pg-up` | Sobe container + espera `pg_isready` |
| `make pg-down` | Para o container (preserva volume) |
| `make pg-status` | Mostra estado/health |
| `make pg-logs` | Tail dos logs |
| `make psql` | Abre shell `psql` interativo |
| `make pg-init` | Aplica `schema_pg.sql` (idempotente) |
| `make seed` | ETL de `vicky/vicky.db` (SQLite) → Postgres |
| `make seed SQLITE=caminho/x.db` | ETL de outro arquivo |
| `make seed-truncate` | Como `seed`, mas TRUNCATE antes |
| `make pg-drop` | TRUNCATE em todas as tabelas (preserva schema) |
| `make pg-reset` | **Apaga volume** e sobe banco zerado (pede confirmação) |
| `make pg-fresh` | `pg-reset` + `pg-init` + `seed` (workflow "do zero") |

### Roteiro pra um dev novo (clone fresco)

```bash
# 1. venv + deps
uv venv --python 3.12
uv pip install -e .

# 2. .env (precisa de OPENROUTER_API_KEY no mínimo)
mkdir -p ~/.config/vicky && chmod 700 ~/.config/vicky
cat > ~/.config/vicky/.env <<'EOF'
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openai/gpt-4o-mini
RAYYAN_EMAIL=admin@example.com
RAYYAN_PASSWORD=anything
RAYYAN_REVIEW_ID=0
DATABASE_URL=postgresql://vicky:vicky_dev@localhost:5433/vicky
EOF
chmod 600 ~/.config/vicky/.env

# 3. Postgres + schema
make pg-up
make pg-init

# 4a. Sem dado de seed (banco zerado): cria admin manualmente
.venv/bin/vicky create-user --email teste@example.com --name Teste --role admin --password senha1234

# 4b. OU se receber um vicky.db por canal seguro fora do git (NÃO é commitado):
make seed SQLITE=caminho/recebido.db

# 5. Servidor
.venv/bin/vicky serve --port 8765
```

> **`vicky/*.db` não vai no repo** — `.gitignore` cobre `*.db` e `*.db.backup-*`. Quem precisar do dado real recebe por canal seguro fora do git.

### ETL SQLite → Postgres ([scripts/migrate_to_pg.py](scripts/migrate_to_pg.py))

- Copia em ordem topológica respeitando FKs (users → workspaces → projects → articles → ...).
- `SET session_replication_role = replica` desliga FKs durante o load (volta no final).
- Reseta sequences (`BIGSERIAL`) pra `MAX(id)+1` depois.
- Valida contagens em SQLite vs Postgres ao final.
- `--truncate` torna idempotente.

### Adapter de dialeto ([src/vicky/db.py](src/vicky/db.py))

Tradução acontece só no backend Postgres. Código de negócio escreve "estilo SQLite":

| Escreve | Vira no PG |
|---|---|
| `?` | `%s` (paramstyle pyformat) |
| `datetime('now')` | `to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')` |
| `strftime('%s', X)` | `EXTRACT(EPOCH FROM (X)::timestamp)::bigint` |
| `julianday('now')` / `julianday(X)` | `(EXTRACT(EPOCH FROM ...)/86400.0)` |
| `unaccent(...)` | `unaccent_ci(...)` (lowercase + sem diacríticos) |
| `INSERT OR REPLACE` | **erro explícito** — use `ON CONFLICT (...) DO UPDATE` |
| `cur.lastrowid` (PK `id`) | `conn.execute_returning_id(sql, params)` |

Função `unaccent_ci` é registrada como SQL function em ambos os backends — busca tolerante (`mamografía` = `mamografia`) funciona em qualquer um.

Pra detectar erros de unique violation portavelmente: `db.is_unique_violation(exc)` (cobre `sqlite3.IntegrityError` e `psycopg.errors.UniqueViolation`).

### Conectar pelo DataGrip / IntelliJ

1. **Data Sources → New → PostgreSQL**
2. Host `localhost`, **Port `5433`**, User `vicky`, Password `vicky_dev`, Database `vicky`
3. Test Connection (baixa driver na primeira vez) → OK

---

## Layout do código

```
src/vicky/
├── cli.py            # Typer CLI: serve, create-user, scrape, analyze, double-check,
│                     #   report, run-all, stats, list-workspaces, doctor
├── config.py         # Carrega ~/.config/vicky/.env, define DB_PATH e DATA_DIR
├── storage.py        # Schema + migrations + connect() + CRUD helpers
│                     #   IMPORTANTE: registra função SQL custom unaccent() para busca
├── discovery.py      # LLM agent que gera critérios PICO + search strings (JSON output)
│                     #   Injeta ano atual no prompt pra evitar dates obsoletos
├── pipeline.py       # Orquestrador: 8 steps async, jobs em SQL, fallback de queries
│                     #   schedule_pipeline() roda em loop principal via run_coroutine_threadsafe
├── llm.py            # Cliente OpenRouter (OpenAI SDK compatible). Aceita criteria opcional
│                     #   pra cada chamada — projetos diferentes têm critérios diferentes!
├── prompts.py        # System prompts do analyzer e double-check (recebem critérios)
├── scraper.py        # Legacy Rayyan scraper (Playwright). Mantido pra projetos importados
├── report.py         # Gera relatório markdown a partir do banco
├── pdf_report.py     # Gera relatório PDF do projeto (export via /projetos/{id}/export)
├── sources/
│   ├── pubmed.py     # E-utilities API (esearch + efetch XML). Sem login. Rate limit 3/s.
│   ├── scielo.py     # HTML scrape de search.scielo.org. User-Agent de browser obrigatório
│   └── scholar.py    # Playwright. Sujeito a CAPTCHA, max ~50 results
└── web/
    ├── app.py            # FastAPI app + rotas + RBAC + sessões cookie assinadas
    ├── users.py          # CRUD de users + auth bcrypt + auto-cria workspace
    ├── workspaces.py     # CRUD de workspaces (1:1 com user, mas modelo permite N:1 futuro)
    ├── projects.py       # CRUD de projects + isolation enforcement (belongs_to_workspace)
    ├── queries.py        # Queries da UI: search_records, dashboard, top_n, etc.
    │                     #   TUDO escopado por project_id ou workspace_id
    ├── static/app.css    # Pequenos overrides (x-cloak, line-clamp, etc.)
    └── templates/
        ├── base.html               # HTML root: Tailwind via CDN + Alpine.js + Inter
        ├── app_layout.html         # Layout autenticado: sidebar + header + main + footer
        ├── login.html              # Tela de login isolada
        ├── dashboard.html          # Workspace overview (lista de projetos)
        ├── projects/
        │   ├── list.html           # Cards de projetos
        │   ├── new.html            # Wizard de criação
        │   └── detail.html         # Pipeline ao vivo + Top N + sidebar com meta editável
        ├── records/
        │   ├── list.html           # Tabela com filtros, chips, busca tolerante
        │   └── detail.html         # Detalhe do artigo + override + score breakdown
        ├── users/list.html         # Admin: gestão de usuários
        ├── workspace_settings.html # Configs do workspace do user logado
        ├── settings.html           # Configs globais (read-only)
        ├── errors/{403,404}.html
        └── components/
            ├── sidebar.html             # Nav com filtro de permissão
            ├── status_badge.html        # Macros: decision_badge, score_pill, user_status
            ├── link_button.html         # Macro: "Abrir Link" com target=_blank/rel=noopener
            ├── empty_state.html         # Macros: empty_state, error_state, loading_state
            ├── decision_actions.html    # Botões de override (Incluir/Excluir/Incerto/Limpar)
            ├── pipeline_banner.html     # Banner sticky com spinner + polling 3s + tempo
            └── project_status.html      # Macros: status_badge (project), source_badge, job_status_icon
```

---

## Como rodar pipelines

### Via UI (jeito normal)
1. Login → **Projetos** → **Novo projeto**
2. Preencha tema + meta de Top N (default 40)
3. **Criar e iniciar pipeline** → banner gigante aparece com progresso ao vivo
4. ~3–8 min depois (depende do tema): página recarrega mostrando Top N

### Via CLI (debug)
```bash
# Ver workspaces e projetos
.venv/bin/vicky list-workspaces
.venv/bin/vicky stats --user vickyangel@gmail.com

# Pipeline legacy (scrape do Rayyan — só pra projetos com creds Rayyan no workspace)
.venv/bin/vicky scrape --user EMAIL
.venv/bin/vicky analyze --user EMAIL
.venv/bin/vicky double-check --user EMAIL
.venv/bin/vicky report --user EMAIL --output relatorio.md
```

CLI é principalmente para administração e debug — o caminho normal é UI.

---

## Como adicionar uma nova source de busca

1. Crie `src/vicky/sources/nome.py` exportando:
   ```python
   SOURCE_NAME = "nome"
   async def search(query: str, *, max_results: int = 100,
                    progress: Callable[[int, str], None] | None = None
                    ) -> list[tuple[Article, dict]]:
       ...
   ```
2. Registre em `src/vicky/sources/__init__.py`:
   ```python
   REGISTRY = {"pubmed": pubmed, "scielo": scielo, "scholar": scholar, "nome": nome}
   ```
3. Adicione limites em `pipeline.py`:
   ```python
   MAX_RESULTS_PER_SOURCE = {..., "nome": 200}
   TARGET_MIN_PER_SOURCE = {..., "nome": 50}
   ```
4. Adicione no form `templates/projects/new.html` (lista de sources com checkbox)
5. Adicione `source_badge` em `components/project_status.html` (cor + label)
6. Critério obrigatório: `Article.external_id` deve ser único dentro da source (PMID, DOI, scholar_cid…)

---

## Discovery Agent — gotchas

- **Sempre injetar ano atual no prompt** (`discovery.py`). LLMs com cutoff antigo geram filtros tipo `"2018"[Date - Publication]` que retornam 0 resultados em 2026.
- **Validar o JSON de saída** com `verify_discovery()` — checa que `criteria_md` tem as 3 seções obrigatórias e que cada search string tem ≥10 chars.
- **MeSH terms são frequentemente inventados** pelo LLM. O pipeline mitiga isso com 3 estratégias: estrita → ampliada (sem filtros de Publication Type) → tema cru (palavras-chave puras).
- O **system prompt** está em [discovery.py](src/vicky/discovery.py). Critérios viram parte do `analyzer_system_prompt(criteria=...)` no [prompts.py](src/vicky/prompts.py).

---

## Search (lista de registros) — busca inteligente

Tokenização + tolerância a acentos via função SQL custom `unaccent()`:
- Query do user é split por whitespace (cada palavra precisa aparecer em ALGUM campo)
- Compara com `unaccent(LOWER(...))` em title, authors, journal, abstract, doi
- "mamografia" = "mamografía" = "MAMOGRAFIA" = "Mamografia"
- "heart failure" = "failure heart" (ordem não importa)

Implementação: [storage.py](src/vicky/storage.py) registra função, [queries.py:search_records](src/vicky/web/queries.py) usa.

---

## Form pitfall: `min_score` (e similares)

FastAPI rejeita string vazia como `int` com 422 (`int_parsing` error). Quando o form HTML envia `name=""`, NÃO use `int | None = None` no handler — use `str = ""` e converta manualmente. Ver `app.py:records_list` como referência.

---

## Rotas (mapa rápido)

**GET (telas):**
- `/login`, `/logout`
- `/dashboard` — overview do workspace
- `/projetos`, `/projetos/novo`, `/projetos/{id}`, `/projetos/{id}/registros`, `/projetos/{id}/registros/{rid}`
- `/projetos/{id}/status` — JSON de polling do pipeline (3s)
- `/projetos/{id}/export` — download do relatório PDF (gerado por `pdf_report.py`)
- `/usuarios` (admin), `/workspace` (settings do workspace logado), `/configuracoes` (read-only global)

**POST (ações):**
- `/projetos/novo` — cria projeto (com flag opcional pra iniciar pipeline)
- `/projetos/{id}/iniciar` — dispara `schedule_pipeline()`
- `/projetos/{id}/atualizar` — edita metadados (incluindo `target_articles`)
- `/projetos/{id}/criterios` — re-grava `criteria_md` editado manualmente
- `/projetos/{id}/excluir` — apaga projeto e dados associados
- `/projetos/{id}/registros/{rid}` — grava override manual (`user_decisions`)

Toda rota com `{project_id}` passa por `get_project_for_workspace()` antes de tocar dados. Ver seção "Multitenancy".

---

## RBAC (controle de permissões)

Definido em `web/users.py:PERMISSIONS`:
```python
admin:        {view_records, view_users, manage_users, edit_records}
operacional:  {view_records, edit_records}
visualizador: {view_records}
```

Uso nas rotas: `Depends(require_perm("manage_users"))` retorna 403 se faltar.
Uso em templates: `{% if user.can("edit_records") %}` esconde botões.
Sidebar: filtra items por permissão automaticamente.

---

## Multitenancy: garantia de isolamento

A regra de ouro: **toda rota que recebe `project_id` deve passar por `get_project_for_workspace(project_id, ws)`**, que retorna 404 se o projeto não pertence ao workspace do user logado. Sem essa verificação, vaza dado entre tenants.

Verificado por testes E2E (26/26 ok no commit da multitenancy):
- User A não vê dados de User B no dashboard
- User A recebe 404 ao tentar URL de artigo de User B
- User A recebe 404 ao tentar override em artigo de User B
- Workspace settings de A não aparecem na UI de B

---

## Pipeline runner — concorrência

`pipeline.schedule_pipeline(project_id)` precisa funcionar dentro E fora do event loop:
- Dentro: `asyncio.get_running_loop().create_task(...)`
- Fora (worker thread do FastAPI): `asyncio.run_coroutine_threadsafe(coro, _MAIN_LOOP)`

`_MAIN_LOOP` é capturado no startup do FastAPI via `@app.on_event("startup")` em `app.py:create_app`.

Cada step do pipeline grava em `jobs` table com status (`queued|running|success|failed`). UI faz polling em `GET /projetos/{id}/status` (JSON) a cada 3s.

---

## Testes manuais que você deve rodar antes de commitar

```bash
# 1. Smoke das 12 rotas principais (todas devem dar 200)
.venv/bin/python -c "
import urllib.request, urllib.parse, http.cookiejar
cj = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
op.open(urllib.request.Request('http://127.0.0.1:8765/login',
    data=urllib.parse.urlencode({'email':'vickyangel@gmail.com','password':'vicky2026'}).encode(),
    method='POST'))
for path in ['/dashboard','/projetos','/projetos/novo','/projetos/1','/projetos/1/registros',
             '/projetos/1/status','/projetos/1/export',
             '/usuarios','/workspace','/configuracoes']:
    r = op.open(f'http://127.0.0.1:8765{path}')
    print(f'{path}: {r.status}')
"

# 2. Filtros (regression)
# - decision=exclude com min_score vazio
# - busca com acento (mamografia / mamografía)
# - filtros combinados

# 3. Override manual
# - Marcar include/exclude/uncertain num registro
# - Verificar que aparece na contagem dos chips

# 4. E2E completo (5 min)
# - Criar projeto novo com tema simples
# - Aguardar até status=done
# - Verificar Top N no detalhe
```

---

## Custos (OpenRouter, default `gpt-4o-mini`)

- Discovery: ~$0.001 por projeto
- Analyze: ~$0.0008 por artigo (~$0.40 para 500)
- Double-check: ~$0.0006 por exclusão (~$0.20 para 350)
- **Total típico: ~$0.50–$1.00 por projeto**

Pra economizar mais: `OPENROUTER_MODEL=google/gemini-2.0-flash-001` no `.env` (~3× mais barato).

---

## Limitações conhecidas

| Item | Status | Notas |
|---|---|---|
| Google Scholar bloqueado por CAPTCHA | Aceitável | Use só PubMed+SciELO em produção |
| Senha Rayyan plaintext no DB | Aceitável (single-user local) | Pra produção: cifrar com Fernet |
| Sessão cookie sem CSRF token | Aceitável (SameSite=Lax) | Adequado pra uso interno |
| Pipeline morre se uvicorn cair durante execução | Conhecido | Pra produção: Celery/RQ + worker separado |
| Postgres sem connection pool — 1 socket por request | Conhecido | Adicionar `psycopg_pool.ConnectionPool` se aparecer latência |
| Postgres não tem migration runtime — schema é estático | Aceitável | Aditivos cobertos por `ALTER IF NOT EXISTS` aplicados pelo `make deploy`. Destrutivos: caso a caso |
| bcrypt warning não-fatal sobre `__about__` | Cosmético | Incompatibilidade passlib + bcrypt 5.x. Pinei bcrypt<5 |
| IDE pyright reclama de imports do venv | Falso-positivo | Imports funcionam em runtime; é só o IDE não vendo `.venv/` |

---

## Convenções de código

- **Sem comentários redundantes.** Se o nome da função explica, não comente.
- **Português brasileiro** em strings de UI, mensagens de erro, prompts.
- **Inglês** em nomes de variáveis, funções, docstrings.
- **Type hints** em funções públicas. `Annotated[T, Depends(...)]` para FastAPI.
- **Dataclasses** para modelos (não Pydantic — overhead desnecessário aqui).
- **Sem ORM** — SQL bruto em `storage.py` e `queries.py`. Mais previsível.
- **Templates Jinja2** com macros para componentes reusáveis (não duplicar HTML).
- **Sem build step** no frontend — Tailwind via CDN, Alpine.js via CDN. Stack zero-npm.

---

## Comandos `.venv/bin/vicky` disponíveis

| Comando | Para que |
|---|---|
| `serve [--port 8000]` | Sobe servidor HTTP |
| `doctor` | Smoke test de config + OpenRouter |
| `create-user --email X --role admin` | Cria user (auto-cria workspace) |
| `list-workspaces` | Lista workspaces e contagem de artigos |
| `stats --user EMAIL` | Métricas do workspace |
| `scrape --user EMAIL` | (legacy Rayyan) |
| `analyze --user EMAIL` | (legacy) |
| `double-check --user EMAIL` | (legacy) |
| `report --user EMAIL` | Gera relatório markdown |
| `run-all --user EMAIL` | Pipeline legacy completo |

Pipelines novos (de projetos) rodam **automaticamente** quando você cria projeto na UI com "Criar e iniciar pipeline" — não precisa do CLI.

---

## Arquivos de credenciais e backup

- **Credenciais:** `~/.config/vicky/.env` (perm 600, fora do repo, NUNCA commitar). Symlink opcional `Vicky/.env` → `~/.config/vicky/.env` pra abrir no IDE; coberto pelo `.gitignore`.
- **DB Postgres:** volume Docker `vicky_pgdata`. Dump com `docker compose exec postgres pg_dump -U vicky vicky > dump.sql`.
- **DB SQLite (fallback):** `~/.local/share/vicky/vicky.db` (modo WAL, backup com `cp`).
- **Backups SQLite locais:** ficam em `vicky/` no root do repo (pasta gitignored). Servem só de seed pra `make seed`.

---

## Documentação adicional

- [docs/criterios-inclusao-exclusao.md](docs/criterios-inclusao-exclusao.md) — exemplo de critérios PICO no formato esperado pelo discovery agent
- [docs/discovery-pipeline.md](docs/discovery-pipeline.md) — spec arquitetural completa do pipeline
- [resultados/](resultados/) — outputs de pipelines anteriores (CSV, JSON, MD)

---

## Glossário

- **Workspace** — escopo de dados de 1 usuário (1:1 hoje, mas o modelo permite N usuários por workspace no futuro)
- **Project** — uma revisão sistemática individual dentro de um workspace, com seu próprio tema e critérios
- **Source** — base científica de busca (`pubmed`, `scielo`, `scholar`)
- **Discovery agent** — LLM que gera critérios PICO + search strings a partir do tema
- **Analyzer** — LLM que decide include/exclude/uncertain por artigo, atribuindo Quality Score 0–100
- **Double-check** — segunda passada do LLM auditando todas as exclusões (mitiga falsos negativos)
- **Verify** — checklist final que confere que todas as etapas rodaram corretamente
- **User decision (override)** — decisão manual do usuário que sobrescreve a da IA (`COALESCE(ud.decision, an.decision)`)
- **Effective decision** — decisão final = override ou IA (usado em todos os filtros e contagens)
- **target_articles** — meta do usuário para o tamanho do Top N final (1–50, default 40 — teto fixo para preservar curadoria minuciosa em duas passadas)
- **Top N** — `MIN(target_articles, total_incluídos_pela_IA)` ranqueado por quality_score
