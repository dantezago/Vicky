# Discovery Pipeline — Arquitetura Multi-Source

Substituição do scraper Rayyan por busca direta nas bases científicas + agente de descoberta de critérios.

## Visão geral

```
[Usuário] ─→ Tema + Objetivo
              ↓
         [Discovery Agent]   ── LLM gera critérios PICO + search strings
              ↓
         (revisão humana opcional)
              ↓
       [Pipeline Runner]      ── job assíncrono persistido em SQL
              ↓
   ┌──────────┼──────────┐
   ↓          ↓          ↓
[PubMed]  [SciELO]  [Scholar]   ── 3 sources em paralelo
   ↓          ↓          ↓
   └──────────┼──────────┘
              ↓
        [Dedup por DOI]
              ↓
        [Analyze (LLM)]
              ↓
        [Double-check]
              ↓
        [Verify checklist]
              ↓
          [Top 40]
```

## Modelo de dados

### Tabela `projects`
```sql
CREATE TABLE projects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id    INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    topic           TEXT NOT NULL,           -- "IA aplicada à mamografia"
    objective       TEXT,                    -- "Avaliar eficácia clínica"
    criteria_md     TEXT,                    -- markdown gerado pelo discovery agent
    search_strings  TEXT,                    -- JSON {pubmed: "...", scielo: "...", scholar: "..."}
    sources         TEXT DEFAULT 'pubmed,scielo,scholar',
    status          TEXT NOT NULL DEFAULT 'draft',
                    -- draft → criteria_ready → searching → analyzing → done | failed
    error           TEXT,
    created_by      INTEGER REFERENCES users(id),
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
```

### Articles agora pertencem a um Projeto
```sql
ALTER TABLE articles ADD COLUMN project_id INTEGER REFERENCES projects(id);
ALTER TABLE articles ADD COLUMN source TEXT DEFAULT 'rayyan';
ALTER TABLE articles ADD COLUMN external_id TEXT;
-- PK passa a ser (workspace_id, project_id, source, external_id)
-- rayyan_id vira external_id
```

Idem para `analyses`, `double_checks`, `user_decisions`.

### Tabela `jobs` (status do pipeline)
```sql
CREATE TABLE jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id   INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    step         TEXT NOT NULL,    -- discovery | search_pubmed | search_scielo | search_scholar | dedup | analyze | double_check | verify | done
    status       TEXT NOT NULL,    -- queued | running | success | failed
    progress     INTEGER DEFAULT 0,    -- 0-100
    message      TEXT,
    error        TEXT,
    started_at   TEXT,
    finished_at  TEXT
);
```

## Discovery Agent

**Input:** `topic` (texto livre do usuário) + opcional `objective` + `years_window` (default 5).

**Prompt:** (versão completa em `src/vicky/discovery.py`)
```
Atue como um especialista em metodologia de pesquisa clínica.
A partir do tema "{topic}" gere:

1. Critérios PICO (População, Intervenção, Comparação, Outcome)
2. Filtro de qualidade metodológica (Validação Externa, N mínimo)
3. Filtro de atualidade ({years_window} anos)
4. Filtro de relevância clínica
5. Quality Score 0-100 (rubrica)
6. Search strings prontas para:
   - PubMed (com MeSH terms)
   - SciELO (com operadores)
   - Google Scholar (frases exatas + termos no título)

SAÍDA: JSON com {criteria_md, search_strings: {pubmed, scielo, scholar}}
```

**Saída persistida:** `projects.criteria_md` e `projects.search_strings`.

## Source scrapers

### `sources/pubmed.py` — PubMed via E-utilities (oficial)
- Endpoint: `https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`
- `esearch.fcgi?db=pubmed&term=...&retmax=200&retmode=json` → IDs
- `efetch.fcgi?db=pubmed&id=...&retmode=xml` → metadados completos (title, abstract, authors, journal, doi, year)
- **Rate limit:** 3 req/s sem API key, 10 req/s com NCBI_API_KEY no .env (opcional)
- **Sem login.** Sem fragilidade.

### `sources/scielo.py` — SciELO via search API
- Endpoint: `https://search.scielo.org/?q=...&output=json&count=50`
- Retorna JSON com `documents: [...]`
- Filtros via parâmetros: `filter[year]`, `filter[la]` (idioma), `filter[in]` (rede)

### `sources/scholar.py` — Google Scholar via Playwright
- Sem API oficial. Scrape com Playwright (já temos infra).
- **Limitações:** rate limit agressivo do Google (~30 req/min antes de CAPTCHA).
- Estratégia: 1 página por vez, delays randômicos, user-agent rotativo, sessão persistente em `~/.cache/vicky/scholar/`.
- Se hit CAPTCHA: pausa o job 5 min e retoma.
- Limite por busca: 100 resultados (5 páginas × 20).

### Dedup
- Após coletar de todas as fontes, deduplicar por `doi` (case-insensitive). Quando não houver DOI, manter ambos.
- Marcar `articles.duplicates` com lista de IDs duplicados removidos.

## Pipeline Runner

**Localização:** `src/vicky/pipeline.py`

```python
async def run_project(project_id: int) -> None:
    """Roda discovery → search → analyze → double-check → verify → done."""
    p = projects.get(project_id)
    update_status(p, "searching")

    for source in p.sources:
        job = create_job(p.id, f"search_{source}")
        try:
            articles = await SOURCES[source].search(p.search_strings[source])
            ingest(p.id, articles)
            mark_job_done(job)
        except Exception as e:
            mark_job_failed(job, e)

    dedup(p.id)

    update_status(p, "analyzing")
    await run_analyze_for_project(p.id)
    await run_double_check_for_project(p.id)
    await run_verify(p.id)  # checklist interno

    update_status(p, "done")
```

**Concorrência multi-usuário:**
- Cada projeto roda em uma task asyncio independente
- SQLite em modo WAL (journal_mode=WAL) permite reads concorrentes
- Writes serializados pelo SQLite — OK para nosso volume

## Sistema de Verificação (dupla checagem)

Após cada etapa, `verify` confere:

| Checklist | Critério |
|---|---|
| Discovery agent rodou | `projects.criteria_md` ≠ NULL e contém seções esperadas |
| Search PubMed rodou | `jobs` tem linha success para `search_pubmed` E `articles` tem ≥ N rows com source=pubmed |
| Search SciELO rodou | idem |
| Search Scholar rodou | idem |
| Dedup rodou | `articles.dedup_at` preenchido |
| Todos os artigos têm análise | `COUNT(articles)` == `COUNT(analyses)` para o projeto |
| Exclusões têm double-check | `COUNT(analyses where decision=exclude)` == `COUNT(double_checks)` |
| Top 40 produzido | `COUNT(included with quality_score)` ≥ 1 |

Se alguma falha → `projects.status = 'failed'`, registra em `jobs[step=verify].error`.

## Front-end

### `/projetos` — Lista
Cards horizontais por projeto, com badge de status colorido.

### `/projetos/novo` — Wizard de criação
1. **Step 1:** Tema + Objetivo + Anos (form simples)
2. **Step 2:** Discovery agent gera critérios + search strings, mostra para revisão (textarea editável)
3. **Step 3:** Confirma fontes a buscar (checkboxes PubMed/SciELO/Scholar)
4. **Submit:** Cria `projects` row + dispara pipeline em background

### `/projetos/{id}` — Detalhe
- Cabeçalho: tema, status, badge
- **Painel de Pipeline:** mostra cada step com seu status (query SQL, polling 3s)
- **Critérios usados:** colapsável, com search strings de cada fonte
- **Top 40 final:** quando status=done, tabela ranqueada (mesma view de antes)
- **Logs de verificação:** o que foi checado e passou ou falhou

### Sidebar
Adiciona "Projetos" como item principal. "Registros" passa a ser scopado pelo projeto ativo (selecionável no header) — ou removemos e fazemos tudo dentro de cada projeto.

## CLI

| Comando | O que faz |
|---|---|
| `vicky discovery --topic "X" --user vicky@email` | Roda só o agent, mostra critérios + search strings (não persiste) |
| `vicky new-project --topic "X" --user vicky@email --auto-run` | Cria projeto + roda pipeline completo |
| `vicky projects --user vicky@email` | Lista projetos do user |
| `vicky run-project <id>` | Re-executa pipeline de um projeto |

CLI antigo (`vicky scrape`) continua funcionando se workspace tem credenciais Rayyan, mas vira **legacy**.

## Multi-user / concorrência

- Cada projeto pertence a um workspace, que pertence a um user
- Multiple users podem ter projetos rodando em paralelo
- SQLite em WAL mode + transactions curtas = OK pra nossa carga
- Pipeline runner usa `asyncio.create_task` no event loop do uvicorn — múltiplos rodam em paralelo
- Estado persistido em `jobs` table → polling do front busca pelo project_id

## Resiliência

- Cada source falha **independentemente**: se Google Scholar derrubar, PubMed e SciELO continuam
- Cada job tem retry com backoff (até 3 tentativas)
- Se discovery agent retornar JSON inválido, retry com prompt mais estrito
- DB em WAL mode + foreign keys ON
- Cada operação crítica logada em `jobs.message`
