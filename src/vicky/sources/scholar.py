"""Google Scholar via Playwright (sem API oficial; scrape com cuidado).

LIMITAÇÕES IMPORTANTES:
- Google Scholar bloqueia agressivamente bots (CAPTCHA após poucas requests)
- Se a env var SERPAPI_KEY estiver setada, faz fallback automático pra SerpAPI
  quando CAPTCHA é detectado (https://serpapi.com/google-scholar-api)
- Sem SerpAPI: pipeline registra resultados parciais e continua

Para minimizar detecção no caminho Playwright:
- Headers de browser real
- Delays randômicos entre requests
- Reusa a mesma sessão Playwright
"""

from __future__ import annotations

import asyncio
import os
import random
import re
import urllib.parse
from typing import Callable

import httpx
from playwright.async_api import async_playwright

from ..storage import Article

SOURCE_NAME = "scholar"
BASE_URL = "https://scholar.google.com/scholar"
SERPAPI_URL = "https://serpapi.com/search.json"


async def search(
    query: str,
    *,
    max_results: int = 50,
    progress: Callable[[int, str], None] | None = None,
    headless: bool = True,
) -> list[tuple[Article, dict]]:
    """Busca no Google Scholar — primeiro via Playwright; se CAPTCHA, fallback SerpAPI."""
    results: list[tuple[Article, dict]] = []
    captcha_hit = False

    if progress: progress(5, "Scholar: abrindo browser…")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()

        page_size = 10
        for offset in range(0, max_results, page_size):
            params = {"q": query, "hl": "en", "as_sdt": "0,5", "start": str(offset)}
            url = BASE_URL + "?" + urllib.parse.urlencode(params)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            except Exception as e:
                if progress: progress(100, f"Scholar erro: {e}")
                break

            html = await page.content()
            # Detectar CAPTCHA / "unusual traffic"
            if "unusual traffic" in html.lower() or "id=\"captcha\"" in html.lower():
                captcha_hit = True
                if progress: progress(50, "Scholar: CAPTCHA detectado, tentando SerpAPI…")
                break

            page_results = _parse_html(html)
            if not page_results:
                break
            results.extend(page_results)

            if progress:
                pct = min(95, int(95 * len(results) / max_results))
                progress(pct, f"Scholar: {len(results)} resultados")
            if len(page_results) < page_size:
                break
            # Delay randômico para não parecer bot
            await asyncio.sleep(random.uniform(2.5, 5.0))

        await browser.close()

    # Fallback: se hit CAPTCHA E SERPAPI_KEY disponível, completa o que falta via SerpAPI
    if captcha_hit and len(results) < max_results:
        api_key = os.getenv("SERPAPI_KEY")
        if api_key:
            try:
                serp_results = await _search_via_serpapi(
                    query, api_key=api_key,
                    max_results=max_results - len(results),
                    progress=progress,
                )
                # Dedup por external_id (cid) caso já tenhamos parte do Playwright
                existing_ids = {a.external_id for a, _ in results}
                for art, raw in serp_results:
                    if art.external_id not in existing_ids:
                        results.append((art, raw))
                        existing_ids.add(art.external_id)
            except Exception as e:
                if progress: progress(95, f"SerpAPI falhou: {type(e).__name__}; mantendo parciais")
        else:
            if progress: progress(100, f"Scholar: bloqueio CAPTCHA, {len(results)} parciais (defina SERPAPI_KEY pra fallback)")

    if progress: progress(100, f"Scholar: {len(results)} artigos prontos")
    return results[:max_results]


async def _search_via_serpapi(
    query: str, *, api_key: str, max_results: int = 50,
    progress: Callable[[int, str], None] | None = None,
) -> list[tuple[Article, dict]]:
    """Fallback via SerpAPI Google Scholar engine. Pago, ~$5 / 1000 queries.

    Docs: https://serpapi.com/google-scholar-api
    """
    results: list[tuple[Article, dict]] = []
    page_size = 20  # SerpAPI suporta até 20 por página
    seen: set[str] = set()

    async with httpx.AsyncClient(timeout=30.0) as client:
        for offset in range(0, max_results, page_size):
            params = {
                "engine": "google_scholar",
                "q": query,
                "api_key": api_key,
                "num": str(min(page_size, max_results - len(results))),
                "start": str(offset),
                "hl": "en",
            }
            try:
                r = await client.get(SERPAPI_URL, params=params)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                if progress: progress(95, f"SerpAPI página {offset}: {type(e).__name__}")
                break

            organic = data.get("organic_results", []) or []
            if not organic:
                break

            for item in organic:
                # SerpAPI dá result_id (estável) ou cid em inline_links.cited_by
                cid = (item.get("result_id")
                       or (item.get("inline_links", {}) or {}).get("cited_by", {}).get("cluster_id")
                       or item.get("link", "")[:64])
                if not cid or cid in seen:
                    continue
                seen.add(cid)

                title = (item.get("title") or "").strip()
                if not title:
                    continue
                url = item.get("link") or ""
                snippet = item.get("snippet") or None

                # publication_info.summary: "Author1, Author2 - Journal, Year - publisher"
                pub_info = item.get("publication_info") or {}
                summary = pub_info.get("summary") or ""
                authors = year = journal = None
                if summary:
                    ym = RE_YEAR.search(summary)
                    if ym:
                        year = ym.group(0)
                    parts = summary.split(" - ")
                    if parts:
                        authors = parts[0].strip() or None
                    if len(parts) >= 2:
                        journal_part = parts[1]
                        if year:
                            journal_part = journal_part.replace(year, "").strip(" ,")
                        journal = journal_part or None
                # Authors também pode vir estruturado em publication_info.authors
                if not authors and pub_info.get("authors"):
                    authors = ", ".join(a.get("name", "") for a in pub_info["authors"] if a.get("name"))

                doi = None
                dm = RE_DOI.search(url + " " + (snippet or ""))
                if dm:
                    doi = dm.group(0)

                article = Article(
                    source=SOURCE_NAME, external_id=str(cid),
                    title=title, authors=authors, year=year, journal=journal,
                    abstract=snippet, doi=doi, external_url=url,
                )
                raw = {"cid": cid, "url": url, "title": title, "authors": authors,
                       "year": year, "journal": journal, "abstract": snippet,
                       "doi": doi, "source_engine": "serpapi"}
                results.append((article, raw))
                if len(results) >= max_results:
                    break

            if progress:
                pct = min(95, int(95 * len(results) / max(max_results, 1)))
                progress(pct, f"SerpAPI: {len(results)} resultados")
            if len(organic) < page_size or len(results) >= max_results:
                break
            await asyncio.sleep(0.5)

    return results


# Scholar usa <div class="gs_r gs_or gs_scl"> como container de resultado
RE_RESULT = re.compile(r'<div class="gs_r gs_or[^"]*"[^>]*data-cid="([^"]+)"[^>]*>(.*?)</div>\s*</div>\s*</div>', re.DOTALL)
RE_TITLE = re.compile(r'<h3 class="gs_rt"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.DOTALL)
RE_AUTHORS_LINE = re.compile(r'<div class="gs_a"[^>]*>(.*?)</div>', re.DOTALL)
RE_SNIPPET = re.compile(r'<div class="gs_rs"[^>]*>(.*?)</div>', re.DOTALL)
RE_YEAR = re.compile(r'\b(19|20)\d{2}\b')
RE_DOI = re.compile(r'10\.\d{4,9}/[\-._;()/:A-Za-z0-9]+', re.IGNORECASE)


def _strip_tags(html: str) -> str:
    return re.sub(r'<[^>]+>', '', html or '').strip()


def _parse_html(html: str) -> list[tuple[Article, dict]]:
    out: list[tuple[Article, dict]] = []
    for m in RE_RESULT.finditer(html):
        cid, body = m.group(1), m.group(2)
        tm = RE_TITLE.search(body)
        if not tm:
            continue
        url, title_html = tm.group(1), tm.group(2)
        title = _strip_tags(title_html)
        if not title:
            continue

        authors = year = journal = None
        am = RE_AUTHORS_LINE.search(body)
        if am:
            authors_line = _strip_tags(am.group(1))
            ym = RE_YEAR.search(authors_line)
            if ym:
                year = ym.group(0)
            # Formato: "Author1, Author2 - Journal, Year - publisher"
            parts = authors_line.split(" - ")
            if parts:
                authors = parts[0].strip()
            if len(parts) >= 2:
                journal_part = parts[1]
                # Remove ano se estiver no journal
                if year:
                    journal_part = journal_part.replace(year, "").strip(" ,")
                journal = journal_part or None

        abstract = None
        sm = RE_SNIPPET.search(body)
        if sm:
            abstract = _strip_tags(sm.group(1))

        doi = None
        dm = RE_DOI.search(url + " " + (abstract or ""))
        if dm:
            doi = dm.group(0)

        article = Article(
            source=SOURCE_NAME, external_id=cid,
            title=title, authors=authors, year=year, journal=journal,
            abstract=abstract, doi=doi, external_url=url,
        )
        raw = {"cid": cid, "url": url, "title": title, "authors": authors,
               "year": year, "journal": journal, "abstract": abstract, "doi": doi}
        out.append((article, raw))
    return out
