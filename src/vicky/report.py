"""Geração do relatório Markdown final para a Victoria revisar."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from .storage import connect, stats


def generate(workspace_id: int, output_path: Path = Path("relatorio.md")) -> Path:
    with connect() as conn:
        s = stats(conn, workspace_id)
        rows = conn.execute(
            """
            SELECT a.*, an.decision, an.reason, an.summary_pt, an.criteria_violated, an.criteria_matched,
                   an.quality_score, an.score_breakdown,
                   dc.agrees, dc.final_decision, dc.explanation
            FROM articles a
            LEFT JOIN analyses an ON an.workspace_id=a.workspace_id AND an.rayyan_id=a.rayyan_id
            LEFT JOIN double_checks dc ON dc.workspace_id=a.workspace_id AND dc.rayyan_id=a.rayyan_id
            WHERE a.workspace_id=?
            ORDER BY an.quality_score DESC NULLS LAST, an.decision, a.year DESC
            """,
            (workspace_id,),
        ).fetchall()
        rows = [dict(r) for r in rows]

    excluded_confirmed = []
    excluded_questioned = []  # double-check discordou
    included = []
    uncertain = []
    not_analyzed = []

    for r in rows:
        if r["decision"] is None:
            not_analyzed.append(r)
            continue
        if r["decision"] == "include":
            included.append(r)
        elif r["decision"] == "uncertain":
            uncertain.append(r)
        elif r["decision"] == "exclude":
            if r["agrees"] == 0:  # double-check discordou
                excluded_questioned.append(r)
            else:
                excluded_confirmed.append(r)

    lines: list[str] = []
    lines.append(f"# Relatório de Triagem — Revisão Sistemática")
    lines.append(f"_Gerado em {datetime.now().strftime('%Y-%m-%d %H:%M')}_\n")

    lines.append("## Sumário\n")
    lines.append(f"- **Total raspado:** {s['articles']}")
    lines.append(f"- **Analisados:** {s['analyzed']}")
    lines.append(f"- **Sugeridos para INCLUIR:** {s['included']}")
    lines.append(f"- **Sugeridos para EXCLUIR:** {s['excluded']}")
    lines.append(f"- **Incertos:** {s['uncertain']}")
    lines.append(f"- **Double-check executado em:** {s['double_checked']}")
    lines.append(f"- **Discordâncias do double-check:** {s['disagreements']}\n")

    if excluded_questioned:
        lines.append("## ⚠️ Discordâncias do Double-Check")
        lines.append(
            "_O auditor automático discordou da exclusão. Revise estes manualmente antes de excluir no Rayyan._\n"
        )
        for r in excluded_questioned:
            _write_article_block(lines, r, show_double_check=True)

    lines.append("## ❌ Excluídos (confirmados pelas 2 passadas)\n")
    if excluded_confirmed:
        for r in excluded_confirmed:
            _write_article_block(lines, r, show_double_check=False)
    else:
        lines.append("_Nenhum._\n")

    lines.append("## ❓ Incertos (revisar manualmente)\n")
    if uncertain:
        for r in uncertain:
            _write_article_block(lines, r, show_double_check=False)
    else:
        lines.append("_Nenhum._\n")

    lines.append("## ✅ Sugeridos para Inclusão\n")
    if included:
        lines.append("| Ano | Título | Motivo |")
        lines.append("|---|---|---|")
        for r in included:
            title = (r["title"] or "").replace("|", "\\|")[:120]
            reason = (r["reason"] or "").replace("|", "\\|")[:120]
            lines.append(f"| {r['year'] or '—'} | {title} | {reason} |")
        lines.append("")
    else:
        lines.append("_Nenhum._\n")

    if not_analyzed:
        lines.append(f"## ⏳ Não analisados ({len(not_analyzed)})\n")
        lines.append("_Rode `vicky analyze` para processar estes._\n")

    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path


def _write_article_block(lines: list[str], r: dict, *, show_double_check: bool) -> None:
    title = r["title"] or "(sem título)"
    lines.append(f"### {title}")
    meta = []
    if r["authors"]:
        meta.append(r["authors"][:200])
    if r["year"]:
        meta.append(str(r["year"]))
    if r["journal"]:
        meta.append(r["journal"])
    if meta:
        lines.append(f"_{' · '.join(meta)}_\n")

    if r["doi"]:
        lines.append(f"**DOI:** [{r['doi']}](https://doi.org/{r['doi']})  ")
    if r["rayyan_url"]:
        lines.append(f"**Rayyan:** [abrir]({r['rayyan_url']})\n")

    if r["summary_pt"]:
        lines.append(f"**Resumo:** {r['summary_pt']}\n")

    lines.append(f"**Motivo da exclusão:** {r['reason']}")

    violated = json.loads(r["criteria_violated"] or "[]")
    if violated:
        lines.append("**Critérios violados:**")
        for c in violated:
            lines.append(f"- {c}")

    if show_double_check and r["explanation"]:
        lines.append(f"\n**🔍 Double-check ({r['final_decision']}):** {r['explanation']}")

    lines.append("\n---\n")
