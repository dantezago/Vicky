"""Modelo de Projects + CRUD."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from ..storage import connect


@dataclass
class Project:
    id: int
    workspace_id: int
    topic: str
    objective: str | None
    years_window: int
    target_articles: int           # Meta de artigos no Top N final (default 40)
    review_type: str               # 'systematic_review' | 'narrative_review'
    criteria_md: str | None
    search_strings: dict[str, str]
    sources: list[str]
    status: str
    error: str | None
    created_by: int | None
    created_at: str
    updated_at: str


def _row_to_project(r: sqlite3.Row) -> Project:
    return Project(
        id=r["id"], workspace_id=r["workspace_id"], topic=r["topic"],
        objective=r["objective"], years_window=r["years_window"] or 5,
        target_articles=r["target_articles"] if "target_articles" in r.keys() and r["target_articles"] else 40,
        review_type=(r["review_type"] if "review_type" in r.keys() and r["review_type"] else "systematic_review"),
        criteria_md=r["criteria_md"],
        search_strings=json.loads(r["search_strings"] or "{}"),
        sources=(r["sources"] or "").split(",") if r["sources"] else [],
        status=r["status"], error=r["error"],
        created_by=r["created_by"], created_at=r["created_at"],
        updated_at=r["updated_at"],
    )


def create(*, workspace_id: int, topic: str, objective: str | None = None,
           years_window: int = 5, target_articles: int = 40,
           review_type: str = "systematic_review",
           sources: list[str] | None = None, created_by: int | None = None) -> Project:
    sources = sources or ["pubmed", "scielo", "scholar"]
    # Sanity: target_articles entre 1 e 50 (teto para preservar triagem minuciosa)
    target_articles = max(1, min(50, int(target_articles)))
    if review_type not in ("systematic_review", "narrative_review"):
        review_type = "systematic_review"
    with connect() as conn:
        new_id = conn.execute_returning_id(
            """INSERT INTO projects (workspace_id, topic, objective, years_window,
                                     target_articles, review_type, sources, status, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'draft', ?)""",
            (workspace_id, topic, objective, years_window, target_articles,
             review_type, ",".join(sources), created_by),
        )
        return _row_to_project(
            conn.execute("SELECT * FROM projects WHERE id=?", (new_id,)).fetchone()
        )


def get(project_id: int) -> Project | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
        return _row_to_project(row) if row else None


def list_for_workspace(workspace_id: int) -> list[Project]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM projects WHERE workspace_id=? ORDER BY created_at DESC",
            (workspace_id,),
        ).fetchall()
        return [_row_to_project(r) for r in rows]


def update(project_id: int, **fields) -> Project:
    """Atualiza campos do projeto. Aceita: topic, objective, criteria_md,
    search_strings (dict), sources (list), status, error, years_window,
    target_articles."""
    if "search_strings" in fields and isinstance(fields["search_strings"], dict):
        fields["search_strings"] = json.dumps(fields["search_strings"], ensure_ascii=False)
    if "sources" in fields and isinstance(fields["sources"], list):
        fields["sources"] = ",".join(fields["sources"])
    # Defesa em profundidade: target_articles SEMPRE clampado em [1, 50],
    # mesmo se um caller esquecer de validar antes (regra do produto).
    if "target_articles" in fields and fields["target_articles"] is not None:
        fields["target_articles"] = max(1, min(50, int(fields["target_articles"])))

    cols, vals = [], []
    for k, v in fields.items():
        cols.append(f"{k}=?"); vals.append(v)
    cols.append("updated_at=datetime('now')")
    vals.append(project_id)

    with connect() as conn:
        conn.execute(f"UPDATE projects SET {', '.join(cols)} WHERE id=?", vals)
        return _row_to_project(
            conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
        )


def belongs_to_workspace(project_id: int, workspace_id: int) -> bool:
    """Garantia de isolamento: projeto pertence ao workspace?"""
    with connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM projects WHERE id=? AND workspace_id=?",
            (project_id, workspace_id),
        ).fetchone()
        return bool(row)
