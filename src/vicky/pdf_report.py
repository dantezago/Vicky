"""Geração de relatório PDF com ReportLab.

Estrutura:
  - Capa: nome do projeto, métricas, critérios
  - Seção INCLUÍDOS (por score DESC): cada artigo com resumo + motivo de inclusão + critérios atendidos
  - Seção EXCLUÍDOS (por ano DESC): cada artigo com resumo + motivo de exclusão + critérios violados
"""

from __future__ import annotations

import io
import json
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, Paragraph, Spacer,
    PageBreak, Table, TableStyle, KeepTogether,
)

# Brand colors do sistema
BRAND_700 = colors.HexColor("#be185d")
BRAND_600 = colors.HexColor("#db2777")
BRAND_500 = colors.HexColor("#ec4899")
BRAND_50 = colors.HexColor("#fdf2f8")
SLATE_900 = colors.HexColor("#0f172a")
SLATE_700 = colors.HexColor("#334155")
SLATE_500 = colors.HexColor("#64748b")
SLATE_300 = colors.HexColor("#cbd5e1")
SLATE_200 = colors.HexColor("#e2e8f0")
SLATE_100 = colors.HexColor("#f1f5f9")
SLATE_50 = colors.HexColor("#f8fafc")
EMERALD_700 = colors.HexColor("#047857")
EMERALD_50 = colors.HexColor("#ecfdf5")
ROSE_700 = colors.HexColor("#be123c")
ROSE_50 = colors.HexColor("#fff1f2")


def _styles():
    """Estilos de parágrafo customizados."""
    base = getSampleStyleSheet()
    s = {
        "title": ParagraphStyle(
            "title", parent=base["Title"],
            fontSize=24, leading=28, textColor=SLATE_900,
            spaceAfter=8, fontName="Helvetica-Bold",
        ),
        "subtitle": ParagraphStyle(
            "subtitle", parent=base["Normal"],
            fontSize=12, leading=16, textColor=SLATE_500, spaceAfter=18,
        ),
        "section_h": ParagraphStyle(
            "section_h", parent=base["Heading1"],
            fontSize=18, leading=22, textColor=BRAND_700,
            spaceBefore=20, spaceAfter=12, fontName="Helvetica-Bold",
        ),
        "article_title": ParagraphStyle(
            "article_title", parent=base["Heading2"],
            fontSize=11, leading=14, textColor=SLATE_900,
            spaceBefore=10, spaceAfter=4, fontName="Helvetica-Bold",
            keepWithNext=True,
        ),
        "meta": ParagraphStyle(
            "meta", parent=base["Normal"],
            fontSize=8, leading=11, textColor=SLATE_500,
            spaceAfter=4, fontName="Helvetica-Oblique",
        ),
        "label": ParagraphStyle(
            "label", parent=base["Normal"],
            fontSize=8, leading=11, textColor=SLATE_700,
            fontName="Helvetica-Bold", spaceAfter=2,
            spaceBefore=4, textTransform="uppercase",
        ),
        "body": ParagraphStyle(
            "body", parent=base["Normal"],
            fontSize=9, leading=12, textColor=SLATE_700,
            spaceAfter=4, alignment=4,  # justify
        ),
        "criterion": ParagraphStyle(
            "criterion", parent=base["Normal"],
            fontSize=8, leading=11, textColor=SLATE_700,
            leftIndent=12, spaceAfter=2,
        ),
        "stat_label": ParagraphStyle(
            "stat_label", parent=base["Normal"],
            fontSize=8, leading=10, textColor=SLATE_500,
            alignment=1, fontName="Helvetica",
        ),
        "stat_value": ParagraphStyle(
            "stat_value", parent=base["Normal"],
            fontSize=20, leading=24, textColor=SLATE_900,
            alignment=1, fontName="Helvetica-Bold",
        ),
        "footer_link": ParagraphStyle(
            "footer_link", parent=base["Normal"],
            fontSize=7, leading=9, textColor=BRAND_700,
            spaceAfter=2,
        ),
    }
    return s


def _esc(text):
    """Escape para reportlab paragraphs (XML-ish)."""
    if text is None:
        return ""
    s = str(text)
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )


def _score_pill_color(score):
    if score is None: return SLATE_200, SLATE_500
    if score >= 75: return EMERALD_50, EMERALD_700
    if score >= 50: return colors.HexColor("#fef3c7"), colors.HexColor("#92400e")
    return SLATE_100, SLATE_500


def _source_label(src):
    return {"pubmed": "PubMed", "scielo": "SciELO", "scholar": "Google Scholar",
            "rayyan": "Rayyan"}.get(src, src or "")


def _article_block(article, styles, *, decision_label, decision_color, criteria_field, criteria_label):
    """Bloco de um artigo no PDF: título + metadados + resumo + decisão + critérios."""
    parts = []

    title_html = _esc(article.get("title") or "(sem título)")
    parts.append(Paragraph(title_html, styles["article_title"]))

    meta_bits = []
    if article.get("authors"): meta_bits.append(_esc(article["authors"][:200]))
    if article.get("year"): meta_bits.append(_esc(str(article["year"])))
    if article.get("journal"): meta_bits.append(_esc(article["journal"]))
    if meta_bits:
        parts.append(Paragraph(" · ".join(meta_bits), styles["meta"]))

    # Mini-tabela: Score · Fonte · Decisão · DOI
    score = article.get("quality_score")
    score_bg, score_fg = _score_pill_color(score)
    pills = [
        [Paragraph(f"<b>Score:</b> {score if score is not None else '—'}", styles["meta"]),
         Paragraph(f"<b>Fonte:</b> {_source_label(article.get('source'))}", styles["meta"]),
         Paragraph(
             f'<font color="{decision_color.hexval()}"><b>Decisão:</b> {decision_label}</font>',
             styles["meta"]),
        ],
    ]
    pills_table = Table(pills, colWidths=[3.5*cm, 4*cm, 4.5*cm])
    pills_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), SLATE_50),
        ("BOX", (0, 0), (-1, -1), 0.5, SLATE_200),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    parts.append(pills_table)
    parts.append(Spacer(1, 4))

    # Resumo
    if article.get("summary_pt"):
        parts.append(Paragraph("RESUMO", styles["label"]))
        parts.append(Paragraph(_esc(article["summary_pt"]), styles["body"]))

    # Motivo da decisão
    if article.get("reason"):
        parts.append(Paragraph(
            "MOTIVO DA INCLUSÃO" if decision_label.lower() == "incluído" else "MOTIVO DA EXCLUSÃO",
            styles["label"]))
        parts.append(Paragraph(_esc(article["reason"]), styles["body"]))

    # Critérios
    crit_raw = article.get(criteria_field)
    crit_list = []
    if isinstance(crit_raw, list):
        crit_list = crit_raw
    elif isinstance(crit_raw, str) and crit_raw:
        try: crit_list = json.loads(crit_raw)
        except Exception: crit_list = []
    if crit_list:
        parts.append(Paragraph(criteria_label, styles["label"]))
        for c in crit_list:
            bullet = "✓" if "atend" in criteria_label.lower() else "✗"
            parts.append(Paragraph(f"{bullet} {_esc(c)}", styles["criterion"]))

    # Link
    if article.get("external_url"):
        parts.append(Paragraph(
            f'<link href="{_esc(article["external_url"])}" color="#be185d">'
            f'→ Abrir artigo: {_esc(article["external_url"])}</link>',
            styles["footer_link"]))

    parts.append(Spacer(1, 8))
    parts.append(Table([[""]], colWidths=[17*cm], rowHeights=[0.5],
                       style=[("LINEABOVE", (0, 0), (-1, 0), 0.3, SLATE_200)]))
    parts.append(Spacer(1, 4))

    # Mantém um artigo junto na mesma página quando possível
    return KeepTogether(parts)


def generate_project_pdf(*, project, metrics, included, excluded,
                         below_cutoff=None, criteria_md=None) -> bytes:
    """Gera PDF do projeto. Retorna bytes prontos pra Response.

    Args:
        project: dict-like com .topic, .objective, .target_articles, .created_at
        metrics: dict de project_dashboard
        included: lista de dicts (ordenada por quality_score DESC)
        excluded: lista de dicts (ordenada por ano DESC)
        criteria_md: critérios PICO (opcional, vai na capa)
    """
    buf = io.BytesIO()
    styles = _styles()

    doc = BaseDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
        title=f"Vicky — {project.topic}",
        author="Vicky · Triagem assistida por IA",
    )

    # Footer com paginação
    def _on_page(canvas, doc):
        canvas.saveState()
        # Header
        canvas.setFont("Helvetica-Bold", 8)
        canvas.setFillColor(BRAND_700)
        canvas.drawString(2*cm, A4[1] - 1*cm, "Vicky")
        canvas.setFillColor(SLATE_500)
        canvas.setFont("Helvetica", 8)
        topic_short = project.topic[:80] + ("…" if len(project.topic) > 80 else "")
        canvas.drawString(3*cm, A4[1] - 1*cm, f"· {topic_short}")
        # Footer
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(SLATE_500)
        canvas.drawString(2*cm, 1*cm, f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        canvas.drawRightString(A4[0] - 2*cm, 1*cm, f"Página {doc.page}")
        canvas.restoreState()

    frame = Frame(doc.leftMargin, doc.bottomMargin,
                  doc.width, doc.height, id="normal")
    doc.addPageTemplates([PageTemplate(id="main", frames=frame, onPage=_on_page)])

    story = []

    # ─── CAPA ───────────────────────────────────────────────────
    story.append(Spacer(1, 1*cm))
    story.append(Paragraph(_esc(project.topic), styles["title"]))
    if project.objective:
        story.append(Paragraph(_esc(project.objective), styles["subtitle"]))

    # Métricas em grid
    stats_data = [[
        [Paragraph(str(metrics.get("total_articles", 0)), styles["stat_value"]),
         Paragraph("Artigos", styles["stat_label"])],
        [Paragraph(str(metrics.get("included", 0)), styles["stat_value"]),
         Paragraph("Incluídos", styles["stat_label"])],
        [Paragraph(str(metrics.get("excluded", 0)), styles["stat_value"]),
         Paragraph("Excluídos", styles["stat_label"])],
        [Paragraph(str(min(metrics.get("included", 0), project.target_articles)), styles["stat_value"]),
         Paragraph(f"Top {project.target_articles}", styles["stat_label"])],
    ]]
    stats_table = Table(stats_data, colWidths=[4.25*cm]*4, rowHeights=[2.5*cm])
    stats_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND_50),
        ("BOX", (0, 0), (-1, -1), 0.5, SLATE_200),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, SLATE_200),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))
    story.append(stats_table)
    story.append(Spacer(1, 0.8*cm))

    # Janela temporal + meta
    info_lines = [
        f"<b>Janela temporal:</b> últimos {project.years_window} anos",
        f"<b>Meta de Top N:</b> {project.target_articles} artigos",
        f"<b>Data do relatório:</b> {datetime.now().strftime('%d/%m/%Y %H:%M')}",
    ]
    for line in info_lines:
        story.append(Paragraph(line, styles["body"]))

    # Critérios
    if criteria_md:
        story.append(PageBreak())
        story.append(Paragraph("Critérios de seleção", styles["section_h"]))
        # Renderiza markdown bruto como texto pré-formatado simples
        for paragraph in criteria_md.split("\n\n"):
            paragraph = paragraph.strip()
            if not paragraph: continue
            # Headers viram bold
            clean = paragraph.replace("\n", "<br/>")
            if paragraph.startswith("# "):
                story.append(Paragraph(f"<b>{_esc(paragraph[2:])}</b>", styles["section_h"]))
            elif paragraph.startswith("## "):
                story.append(Paragraph(f"<b>{_esc(paragraph[3:])}</b>", styles["article_title"]))
            elif paragraph.startswith("### "):
                story.append(Paragraph(f"<b>{_esc(paragraph[4:])}</b>", styles["label"]))
            else:
                story.append(Paragraph(_esc(clean).replace("&lt;br/&gt;", "<br/>"), styles["body"]))

    # ─── TOP N (incluídos no recorte final) ────────────────────
    story.append(PageBreak())
    story.append(Paragraph(
        f"🏆 Top {len(included)} — Incluídos no recorte final (ordenados por Quality Score)",
        styles["section_h"]))
    if included:
        for art in included:
            story.append(_article_block(
                art, styles,
                decision_label="Incluído",
                decision_color=EMERALD_700,
                criteria_field="criteria_matched",
                criteria_label="CRITÉRIOS ATENDIDOS",
            ))
    else:
        story.append(Paragraph("Nenhum artigo incluído.", styles["body"]))

    # ─── INCLUÍDOS FORA DO TOP N ───────────────────────────────
    if below_cutoff:
        story.append(PageBreak())
        story.append(Paragraph(
            f"📋 Incluídos fora do Top {project.target_articles} ({len(below_cutoff)})",
            styles["section_h"]))
        story.append(Paragraph(
            "Artigos que atendem aos critérios da revisão MAS ficaram fora do recorte "
            f"final por terem quality_score abaixo do {project.target_articles}º colocado. "
            "Mantidos como referência ou para você promover manualmente se quiser.",
            styles["body"]))
        story.append(Spacer(1, 8))
        for art in below_cutoff:
            story.append(_article_block(
                art, styles,
                decision_label="Fora do Top",
                decision_color=colors.HexColor("#7c3aed"),  # violet
                criteria_field="criteria_matched",
                criteria_label="CRITÉRIOS ATENDIDOS",
            ))

    # ─── EXCLUÍDOS ──────────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph(
        f"❌ Excluídos ({len(excluded)}) — ordenados por ano",
        styles["section_h"]))
    if excluded:
        for art in excluded:
            story.append(_article_block(
                art, styles,
                decision_label="Excluído",
                decision_color=ROSE_700,
                criteria_field="criteria_violated",
                criteria_label="CRITÉRIOS VIOLADOS",
            ))
    else:
        story.append(Paragraph("Nenhum artigo excluído.", styles["body"]))

    doc.build(story)
    return buf.getvalue()
