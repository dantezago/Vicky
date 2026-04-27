"""Queries da UI — escopadas por project_id (ou workspace_id quando agregado)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from ..storage import connect


# ─── LLM Usage (admin) ─────────────────────────────────────────────────────


def list_llm_usage(*, workspace_id: int | None = None,
                   project_id: int | None = None,
                   user_id: int | None = None,
                   step: str | None = None,
                   model: str | None = None,
                   since: str | None = None,
                   limit: int = 200, offset: int = 0) -> list[dict]:
    """Admin: lista chamadas LLM com filtros opcionais. workspace_id=None = TODOS."""
    where = ["1=1"]
    params: list[Any] = []
    if workspace_id is not None:
        where.append("u.workspace_id=?"); params.append(workspace_id)
    if project_id is not None:
        where.append("u.project_id=?"); params.append(project_id)
    if user_id is not None:
        where.append("u.user_id=?"); params.append(user_id)
    if step:
        where.append("u.pipeline_step=?"); params.append(step)
    if model:
        where.append("u.model=?"); params.append(model)
    if since:
        where.append("u.created_at >= ?"); params.append(since)
    sql = f"""
        SELECT u.*,
               us.name AS user_name, us.email AS user_email,
               p.topic AS project_topic, p.review_type AS project_review_type
        FROM llm_usage u
        LEFT JOIN users us ON us.id = u.user_id
        LEFT JOIN projects p ON p.id = u.project_id
        WHERE {' AND '.join(where)}
        ORDER BY u.created_at DESC, u.id DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def llm_usage_totals(*, workspace_id: int | None = None,
                     project_id: int | None = None,
                     user_id: int | None = None,
                     step: str | None = None,
                     model: str | None = None,
                     since: str | None = None) -> dict[str, Any]:
    """Agregados: total tokens, custo, # requests, breakdown por step e por modelo."""
    where = ["1=1"]
    params: list[Any] = []
    if workspace_id is not None:
        where.append("workspace_id=?"); params.append(workspace_id)
    if project_id is not None:
        where.append("project_id=?"); params.append(project_id)
    if user_id is not None:
        where.append("user_id=?"); params.append(user_id)
    if step:
        where.append("pipeline_step=?"); params.append(step)
    if model:
        where.append("model=?"); params.append(model)
    if since:
        where.append("created_at >= ?"); params.append(since)
    where_sql = " AND ".join(where)
    with connect() as conn:
        totals = conn.execute(
            f"""SELECT COUNT(*) AS n_requests,
                       COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,
                       COALESCE(SUM(completion_tokens),0) AS completion_tokens,
                       COALESCE(SUM(total_tokens),0) AS total_tokens,
                       COALESCE(SUM(cost_usd),0) AS cost_usd
                FROM llm_usage WHERE {where_sql}""",
            params,
        ).fetchone()
        by_step = conn.execute(
            f"""SELECT pipeline_step,
                       COUNT(*) AS n,
                       COALESCE(SUM(total_tokens),0) AS tokens,
                       COALESCE(SUM(cost_usd),0) AS cost
                FROM llm_usage WHERE {where_sql}
                GROUP BY pipeline_step ORDER BY cost DESC""",
            params,
        ).fetchall()
        by_model = conn.execute(
            f"""SELECT model,
                       COUNT(*) AS n,
                       COALESCE(SUM(total_tokens),0) AS tokens,
                       COALESCE(SUM(cost_usd),0) AS cost
                FROM llm_usage WHERE {where_sql}
                GROUP BY model ORDER BY cost DESC""",
            params,
        ).fetchall()
        return {
            "totals": dict(totals) if totals else {},
            "by_step": [dict(r) for r in by_step],
            "by_model": [dict(r) for r in by_model],
        }


def get_llm_usage(usage_id: int) -> dict | None:
    with connect() as conn:
        row = conn.execute(
            """SELECT u.*,
                      us.name AS user_name, us.email AS user_email,
                      p.topic AS project_topic, p.objective AS project_objective,
                      p.review_type AS project_review_type,
                      p.target_articles AS project_target_articles,
                      p.sources AS project_sources,
                      w.name AS workspace_name
               FROM llm_usage u
               LEFT JOIN users us ON us.id = u.user_id
               LEFT JOIN projects p ON p.id = u.project_id
               LEFT JOIN workspaces w ON w.id = u.workspace_id
               WHERE u.id=?""",
            (usage_id,),
        ).fetchone()
        if not row:
            return None
        out = dict(row)
        try:
            out["request_metadata_parsed"] = json.loads(out["request_metadata"]) if out.get("request_metadata") else {}
        except Exception:
            out["request_metadata_parsed"] = {}
        return out


def llm_usage_avg_per_project_by_review_type() -> dict[str, Any]:
    """Médias de tokens e custo USD por projeto inteiro, agrupadas por tipo de revisão.

    Considera projetos com pelo menos 1 chamada LLM registrada. Calcula primeiro
    o total por projeto (sum tokens, sum cost), depois a média desses totais
    dentro de cada tipo. Retorna 3 buckets:
      - systematic_review (revisão sistemática)
      - narrative_review (artigo de resumo)
      - all (média combinada, todos os projetos com LLM usage)
    """
    sql = """
        SELECT review_type,
               COUNT(*)        AS n_projects,
               AVG(tokens)     AS avg_tokens,
               AVG(cost)       AS avg_cost,
               SUM(tokens)     AS sum_tokens,
               SUM(cost)       AS sum_cost
        FROM (
            SELECT p.id,
                   COALESCE(p.review_type, 'systematic_review') AS review_type,
                   SUM(COALESCE(u.total_tokens, 0)) AS tokens,
                   SUM(COALESCE(u.cost_usd, 0))    AS cost
            FROM projects p
            JOIN llm_usage u ON u.project_id = p.id
            GROUP BY p.id
        )
        GROUP BY review_type
    """
    sql_all = """
        SELECT COUNT(*) AS n_projects,
               AVG(tokens) AS avg_tokens,
               AVG(cost)   AS avg_cost,
               SUM(tokens) AS sum_tokens,
               SUM(cost)   AS sum_cost
        FROM (
            SELECT p.id,
                   SUM(COALESCE(u.total_tokens, 0)) AS tokens,
                   SUM(COALESCE(u.cost_usd, 0))    AS cost
            FROM projects p
            JOIN llm_usage u ON u.project_id = p.id
            GROUP BY p.id
        )
    """
    out: dict[str, dict] = {
        "systematic_review": {"n_projects": 0, "avg_tokens": 0, "avg_cost": 0,
                              "sum_tokens": 0, "sum_cost": 0},
        "narrative_review":  {"n_projects": 0, "avg_tokens": 0, "avg_cost": 0,
                              "sum_tokens": 0, "sum_cost": 0},
        "all":               {"n_projects": 0, "avg_tokens": 0, "avg_cost": 0,
                              "sum_tokens": 0, "sum_cost": 0},
    }
    with connect() as conn:
        for r in conn.execute(sql).fetchall():
            rt = r["review_type"] or "systematic_review"
            if rt not in out:
                continue
            out[rt] = {
                "n_projects": r["n_projects"] or 0,
                "avg_tokens": float(r["avg_tokens"] or 0),
                "avg_cost":   float(r["avg_cost"] or 0),
                "sum_tokens": int(r["sum_tokens"] or 0),
                "sum_cost":   float(r["sum_cost"] or 0),
            }
        a = conn.execute(sql_all).fetchone()
        if a:
            out["all"] = {
                "n_projects": a["n_projects"] or 0,
                "avg_tokens": float(a["avg_tokens"] or 0),
                "avg_cost":   float(a["avg_cost"] or 0),
                "sum_tokens": int(a["sum_tokens"] or 0),
                "sum_cost":   float(a["sum_cost"] or 0),
            }
    return out


def pipeline_duration_avg_by_review_type() -> dict[str, Any]:
    """Tempo médio total de pipeline (do primeiro job ao último) por tipo de revisão.

    Calcula `MAX(finished_at) - MIN(started_at)` por projeto e tira média dentro
    de cada bucket. Retorna duração em segundos.
    """
    sql_per_project = """
        SELECT p.id,
               COALESCE(p.review_type, 'systematic_review') AS review_type,
               (CAST(strftime('%s', MAX(j.finished_at)) AS INTEGER) -
                CAST(strftime('%s', MIN(j.started_at)) AS INTEGER)) AS duration_sec
        FROM projects p
        JOIN jobs j ON j.project_id = p.id
        WHERE j.started_at IS NOT NULL AND j.finished_at IS NOT NULL
        GROUP BY p.id
        HAVING duration_sec IS NOT NULL AND duration_sec > 0
    """
    out: dict[str, dict] = {
        "systematic_review": {"n_projects": 0, "avg_seconds": 0.0},
        "narrative_review":  {"n_projects": 0, "avg_seconds": 0.0},
        "all":               {"n_projects": 0, "avg_seconds": 0.0},
    }
    with connect() as conn:
        rows = conn.execute(sql_per_project).fetchall()
    if not rows:
        return out
    by_type: dict[str, list[int]] = {"systematic_review": [], "narrative_review": []}
    all_durations: list[int] = []
    for r in rows:
        rt = r["review_type"] or "systematic_review"
        d = int(r["duration_sec"])
        if rt in by_type:
            by_type[rt].append(d)
        all_durations.append(d)
    for rt, durations in by_type.items():
        if durations:
            out[rt] = {"n_projects": len(durations),
                       "avg_seconds": sum(durations) / len(durations)}
    if all_durations:
        out["all"] = {"n_projects": len(all_durations),
                      "avg_seconds": sum(all_durations) / len(all_durations)}
    return out


def llm_usage_filter_options() -> dict[str, list]:
    with connect() as conn:
        steps = [r[0] for r in conn.execute(
            "SELECT DISTINCT pipeline_step FROM llm_usage ORDER BY pipeline_step"
        ).fetchall()]
        models = [r[0] for r in conn.execute(
            "SELECT DISTINCT model FROM llm_usage ORDER BY model"
        ).fetchall()]
        users_rows = conn.execute(
            """SELECT DISTINCT u.user_id, us.name, us.email
               FROM llm_usage u JOIN users us ON us.id = u.user_id
               WHERE u.user_id IS NOT NULL ORDER BY us.name"""
        ).fetchall()
        projects_rows = conn.execute(
            """SELECT DISTINCT u.project_id, p.topic
               FROM llm_usage u JOIN projects p ON p.id = u.project_id
               WHERE u.project_id IS NOT NULL ORDER BY p.topic"""
        ).fetchall()
        return {
            "steps": steps, "models": models,
            "users": [dict(r) for r in users_rows],
            "projects": [dict(r) for r in projects_rows],
        }


@dataclass
class PageResult:
    items: list[dict]
    total: int
    page: int
    per_page: int
    total_pages: int


# Decisão efetiva = override do usuário > decisão da IA
EFFECTIVE_JOIN = """
    LEFT JOIN analyses an ON an.project_id=a.project_id AND an.source=a.source AND an.external_id=a.external_id
    LEFT JOIN user_decisions ud ON ud.project_id=a.project_id AND ud.source=a.source AND ud.external_id=a.external_id
"""

EFFECTIVE_DECISION_SQL = "COALESCE(ud.decision, an.decision)"


def get_workspace_dashboard(ws_id: int) -> dict[str, Any]:
    """Métricas agregadas do workspace (todos os projetos)."""
    with connect() as conn:
        projects = conn.execute(
            """SELECT p.id, p.topic, p.status, p.updated_at,
                      COUNT(DISTINCT a.external_id) AS n_articles
               FROM projects p
               LEFT JOIN articles a ON a.project_id=p.id AND a.is_duplicate=0
               WHERE p.workspace_id=?
               GROUP BY p.id ORDER BY p.updated_at DESC LIMIT 6""",
            (ws_id,),
        ).fetchall()
        n_projects = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE workspace_id=?", (ws_id,)
        ).fetchone()[0]
        n_articles = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE workspace_id=? AND is_duplicate=0", (ws_id,)
        ).fetchone()[0]
        n_done = conn.execute(
            "SELECT COUNT(*) FROM projects WHERE workspace_id=? AND status='done'", (ws_id,)
        ).fetchone()[0]
        n_running = conn.execute(
            """SELECT COUNT(*) FROM projects WHERE workspace_id=?
               AND status IN ('searching','analyzing','criteria_ready')""",
            (ws_id,),
        ).fetchone()[0]
    return {
        "n_projects": n_projects, "n_articles": n_articles,
        "n_done": n_done, "n_running": n_running,
        "recent_projects": [dict(p) for p in projects],
    }


def get_project_dashboard(project_id: int) -> dict[str, Any]:
    """Métricas de um projeto específico."""
    with connect() as conn:
        articles = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE project_id=? AND is_duplicate=0", (project_id,)
        ).fetchone()[0]
        analyzed = conn.execute(
            "SELECT COUNT(*) FROM analyses WHERE project_id=?", (project_id,)
        ).fetchone()[0]

        # Counts por source
        by_source = conn.execute(
            """SELECT source, COUNT(*) AS n FROM articles
               WHERE project_id=? AND is_duplicate=0 GROUP BY source""",
            (project_id,),
        ).fetchall()

        # Decisões efetivas
        eff = conn.execute(
            f"""SELECT {EFFECTIVE_DECISION_SQL} AS decision, COUNT(*) AS n
                FROM articles a {EFFECTIVE_JOIN}
                WHERE a.project_id=? AND a.is_duplicate=0
                GROUP BY {EFFECTIVE_DECISION_SQL}""",
            (project_id,),
        ).fetchall()
        by_decision = {r["decision"] or "not_analyzed": r["n"] for r in eff}

        double_checked = conn.execute(
            "SELECT COUNT(*) FROM double_checks WHERE project_id=?", (project_id,)
        ).fetchone()[0]
        disagreements = conn.execute(
            "SELECT COUNT(*) FROM double_checks WHERE project_id=? AND agrees=0", (project_id,)
        ).fetchone()[0]
        user_overrides = conn.execute(
            "SELECT COUNT(*) FROM user_decisions WHERE project_id=?", (project_id,)
        ).fetchone()[0]
        # Top N final vs incluídos abaixo do corte
        included_top = conn.execute(
            "SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='include' AND in_top_n=1",
            (project_id,)).fetchone()[0]
        below_cutoff = conn.execute(
            "SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='include' AND in_top_n=0",
            (project_id,)).fetchone()[0]
        top40_avg = conn.execute(
            f"""SELECT AVG(quality_score) FROM (
                  SELECT an.quality_score
                  FROM articles a {EFFECTIVE_JOIN}
                  WHERE a.project_id=? AND a.is_duplicate=0
                    AND {EFFECTIVE_DECISION_SQL}='include'
                    AND an.quality_score IS NOT NULL
                  ORDER BY an.quality_score DESC LIMIT 40)""",
            (project_id,),
        ).fetchone()[0]

    return {
        "total_articles": articles,
        "analyzed": analyzed,
        "by_source": [dict(r) for r in by_source],
        "by_decision": by_decision,
        "included": by_decision.get("include", 0),
        "excluded": by_decision.get("exclude", 0),
        "uncertain": by_decision.get("uncertain", 0),
        "double_checked": double_checked,
        "disagreements": disagreements,
        "user_overrides": user_overrides,
        "included_top": included_top,
        "below_cutoff": below_cutoff,
        "top40_avg": round(top40_avg, 1) if top40_avg else None,
    }


def search_records(
    project_id: int,
    *,
    q: str = "",
    decision: str = "",
    source: str = "",
    min_score: int | None = None,
    year: str = "",
    page: int = 1,
    per_page: int = 20,
    sort: str = "score_desc",
) -> PageResult:
    where = ["a.project_id = ?", "a.is_duplicate = 0"]
    params: list[Any] = [project_id]

    if q and q.strip():
        # Tokeniza por palavra; cada palavra precisa aparecer em ALGUM dos campos
        # (busca tolerante a acentos via unaccent() registrada no SQLite)
        import re as _re
        tokens = [t for t in _re.split(r"\s+", q.strip()) if t]
        for tok in tokens:
            where.append(
                "(unaccent(a.title) LIKE unaccent(?) OR "
                "unaccent(a.authors) LIKE unaccent(?) OR "
                "unaccent(a.journal) LIKE unaccent(?) OR "
                "unaccent(a.abstract) LIKE unaccent(?) OR "
                "unaccent(a.doi) LIKE unaccent(?))"
            )
            like = f"%{tok}%"
            params += [like, like, like, like, like]
    if decision:
        if decision == "not_analyzed":
            where.append(f"{EFFECTIVE_DECISION_SQL} IS NULL")
        elif decision == "include_top":
            # Incluídos no top N final
            where.append(f"{EFFECTIVE_DECISION_SQL} = 'include' AND COALESCE(an.in_top_n, 1) = 1")
        elif decision == "below_cutoff":
            # Incluídos pela IA mas fora do top N
            where.append(f"{EFFECTIVE_DECISION_SQL} = 'include' AND COALESCE(an.in_top_n, 1) = 0")
        else:
            where.append(f"{EFFECTIVE_DECISION_SQL} = ?")
            params.append(decision)
    if source:
        where.append("a.source = ?"); params.append(source)
    if min_score is not None:
        where.append("an.quality_score >= ?"); params.append(min_score)
    if year:
        where.append("a.year = ?"); params.append(year)

    where_sql = "WHERE " + " AND ".join(where)
    sort_map = {
        "score_desc": "an.quality_score DESC NULLS LAST, a.year DESC",
        "score_asc": "an.quality_score ASC NULLS LAST",
        "year_desc": "a.year DESC",
        "year_asc": "a.year ASC",
        "title_asc": "a.title ASC",
        "title_desc": "a.title DESC",
    }
    order_sql = sort_map.get(sort, sort_map["score_desc"])

    with connect() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM articles a {EFFECTIVE_JOIN} {where_sql}", params
        ).fetchone()[0]
        offset = (page - 1) * per_page
        rows = conn.execute(
            f"""SELECT a.source, a.external_id, a.title, a.authors, a.year, a.journal,
                       a.doi, a.external_url,
                       an.decision AS ai_decision, an.quality_score, an.reason,
                       COALESCE(an.in_top_n, 1) AS in_top_n,
                       ud.decision AS user_decision,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                {where_sql} ORDER BY {order_sql} LIMIT ? OFFSET ?""",
            params + [per_page, offset],
        ).fetchall()

    return PageResult(
        items=[dict(r) for r in rows], total=total, page=page,
        per_page=per_page, total_pages=max(1, (total + per_page - 1) // per_page),
    )


def get_record_detail(project_id: int, source: str, external_id: str) -> dict | None:
    with connect() as conn:
        row = conn.execute(
            f"""SELECT a.*, an.decision AS ai_decision, an.reason, an.summary_pt,
                       an.criteria_matched, an.criteria_violated,
                       an.quality_score, an.score_breakdown,
                       COALESCE(an.in_top_n, 1) AS in_top_n,
                       dc.agrees, dc.final_decision, dc.explanation,
                       ud.decision AS user_decision, ud.note AS user_note,
                       ud.decided_at AS user_decided_at,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                LEFT JOIN double_checks dc ON dc.project_id=a.project_id AND dc.source=a.source AND dc.external_id=a.external_id
                WHERE a.project_id=? AND a.source=? AND a.external_id=?""",
            (project_id, source, external_id),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        for f in ("criteria_matched", "criteria_violated", "score_breakdown"):
            if d.get(f):
                try: d[f] = json.loads(d[f])
                except Exception: pass
        return d


def get_year_options(project_id: int) -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            """SELECT DISTINCT year FROM articles
               WHERE project_id=? AND year IS NOT NULL AND year != ''
               ORDER BY year DESC""",
            (project_id,),
        ).fetchall()
        return [r["year"] for r in rows]


def get_source_options(project_id: int) -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT source FROM articles WHERE project_id=? ORDER BY source",
            (project_id,),
        ).fetchall()
        return [r["source"] for r in rows]


def get_top_n(project_id: int, n: int = 40) -> list[dict]:
    """Top N — usa in_top_n=1 marcado pelo step_finalize do pipeline.

    Garantias:
      - Nunca retorna artigos com decision != 'include'
      - Nunca retorna mais que `n` (cap aplicado pelo finalize)
    """
    n = max(1, min(200, int(n)))
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT a.source, a.external_id, a.title, a.authors, a.year, a.journal,
                       a.doi, a.external_url, an.quality_score, an.summary_pt,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                WHERE a.project_id=? AND a.is_duplicate=0
                  AND an.in_top_n = 1 AND an.decision='include'
                ORDER BY an.quality_score DESC NULLS LAST, a.year DESC LIMIT ?""",
            (project_id, n),
        ).fetchall()
        return [dict(r) for r in rows]


def get_below_cutoff(project_id: int) -> list[dict]:
    """Incluídos pela IA mas FORA do top N (cortados pelo cutoff de score)."""
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT a.source, a.external_id, a.title, a.authors, a.year, a.journal,
                       a.doi, a.external_url, an.quality_score, an.summary_pt, an.reason,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                WHERE a.project_id=? AND a.is_duplicate=0
                  AND an.in_top_n = 0 AND an.decision='include'
                ORDER BY an.quality_score DESC NULLS LAST""",
            (project_id,),
        ).fetchall()
        return [dict(r) for r in rows]


# Alias compatível
get_top_40 = get_top_n


def get_included_for_report(project_id: int, *, only_top: bool = False) -> list[dict]:
    """Incluídos, com resumo + motivo. Se only_top=True, só os do Top N (in_top_n=1)."""
    extra_filter = "AND COALESCE(an.in_top_n, 1) = 1" if only_top else ""
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT a.source, a.external_id, a.title, a.authors, a.year, a.journal,
                       a.doi, a.external_url, a.abstract,
                       an.quality_score, an.summary_pt, an.reason,
                       an.criteria_matched, an.criteria_violated, an.score_breakdown,
                       COALESCE(an.in_top_n, 1) AS in_top_n,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                WHERE a.project_id=? AND a.is_duplicate=0
                  AND {EFFECTIVE_DECISION_SQL}='include' {extra_filter}
                ORDER BY an.quality_score DESC NULLS LAST, a.year DESC""",
            (project_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for f in ("criteria_matched", "criteria_violated", "score_breakdown"):
                if d.get(f):
                    try: d[f] = json.loads(d[f])
                    except Exception: pass
            result.append(d)
        return result


def get_below_cutoff_for_report(project_id: int) -> list[dict]:
    """Incluídos pela IA mas FORA do top N — para o relatório PDF."""
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT a.source, a.external_id, a.title, a.authors, a.year, a.journal,
                       a.doi, a.external_url, a.abstract,
                       an.quality_score, an.summary_pt, an.reason,
                       an.criteria_matched, an.criteria_violated, an.score_breakdown,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                WHERE a.project_id=? AND a.is_duplicate=0
                  AND {EFFECTIVE_DECISION_SQL}='include' AND COALESCE(an.in_top_n, 1) = 0
                ORDER BY an.quality_score DESC NULLS LAST""",
            (project_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for f in ("criteria_matched", "criteria_violated", "score_breakdown"):
                if d.get(f):
                    try: d[f] = json.loads(d[f])
                    except Exception: pass
            result.append(d)
        return result


def get_excluded_for_report(project_id: int) -> list[dict]:
    """Todos os excluídos, com summary_pt + reason + criteria_violated, ano DESC."""
    with connect() as conn:
        rows = conn.execute(
            f"""SELECT a.source, a.external_id, a.title, a.authors, a.year, a.journal,
                       a.doi, a.external_url, a.abstract,
                       an.quality_score, an.summary_pt, an.reason,
                       an.criteria_matched, an.criteria_violated,
                       {EFFECTIVE_DECISION_SQL} AS decision
                FROM articles a {EFFECTIVE_JOIN}
                WHERE a.project_id=? AND a.is_duplicate=0
                  AND {EFFECTIVE_DECISION_SQL}='exclude'
                ORDER BY a.year DESC, a.title ASC""",
            (project_id,),
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            for f in ("criteria_matched", "criteria_violated"):
                if d.get(f):
                    try: d[f] = json.loads(d[f])
                    except Exception: pass
            result.append(d)
        return result


def get_jobs(project_id: int) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE project_id=? ORDER BY id ASC", (project_id,)
        ).fetchall()
        return [dict(r) for r in rows]
