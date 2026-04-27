"""SciELO search via search.scielo.org (HTML scraping leve).

Endpoint público, sem login. Retorna até 50 resultados por página.
"""

from __future__ import annotations

import asyncio
import re
import urllib.parse
from typing import Callable

import httpx

from ..storage import Article

SOURCE_NAME = "scielo"
SEARCH_URL = "https://search.scielo.org/"

# User-Agent de browser real (a SciELO bloqueia bots óbvios)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
}


async def _fetch_with_retry(
    client: httpx.AsyncClient, url: str, *, max_attempts: int = 3,
) -> httpx.Response | None:
    """GET com backoff em 429/5xx/timeout. Retorna None em falha definitiva."""
    for attempt in range(1, max_attempts + 1):
        try:
            r = await client.get(url)
            if r.status_code == 200:
                return r
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < max_attempts:
                    await asyncio.sleep(min(2 ** attempt, 12))
                    continue
                return None
            return None
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError):
            if attempt < max_attempts:
                await asyncio.sleep(min(2 ** attempt, 12))
                continue
            return None
    return None


async def search(
    query: str,
    *,
    max_results: int = 100,
    progress: Callable[[int, str], None] | None = None,
) -> list[tuple[Article, dict]]:
    results: list[tuple[Article, dict]] = []

    async with httpx.AsyncClient(timeout=30.0, headers=HEADERS, follow_redirects=True) as client:
        page_size = 50
        page = 1
        seen_ids: set[str] = set()
        empty_pages_in_a_row = 0
        if progress: progress(5, "SciELO: iniciando…")

        while len(results) < max_results:
            params = {
                "q": query, "lang": "pt", "count": str(page_size),
                "from": str((page - 1) * page_size + 1),
            }
            url = SEARCH_URL + "?" + urllib.parse.urlencode(params)
            r = await _fetch_with_retry(client, url)
            if r is None:
                if progress: progress(100, f"SciELO falhou na página {page} após retries; mantendo {len(results)} já coletados")
                break

            page_results = _parse_search_html(r.text)
            new = [pr for pr in page_results if pr[0].external_id not in seen_ids]
            for art, _ in new:
                seen_ids.add(art.external_id)
            results.extend(new)
            if progress:
                pct = min(95, int(95 * len(results) / max_results))
                progress(pct, f"SciELO: {len(results)} artigos (página {page})")

            # Defesa contra paginação infinita: para se 2 páginas seguidas vazias
            # (a SciELO às vezes retorna HTML de busca sem itens em vez de 404)
            if not new:
                empty_pages_in_a_row += 1
                if empty_pages_in_a_row >= 2:
                    break
            else:
                empty_pages_in_a_row = 0

            if len(page_results) < page_size:
                break
            page += 1
            # Cap defensivo de páginas
            if page > 20:
                break
            await asyncio.sleep(1.0)

    if progress: progress(100, f"SciELO: {len(results)} artigos prontos")
    return results[:max_results]


# Cada resultado: <div id="..." class="item">...</div>  até o próximo <div ... class="item"> ou final
RE_ITEM = re.compile(
    r'<div id="([^"]+)" class="item">(.*?)(?=<div id="[^"]+" class="item">|<div class="container resultBlock">|<div class="paginate)',
    re.DOTALL,
)
RE_TITLE = re.compile(r'<a href="([^"]+)"[^>]*>\s*<strong class="title"[^>]*>(.*?)</strong>', re.DOTALL)
RE_AUTHORS = re.compile(r'<div class="line authors[^"]*"[^>]*>(.*?)</div>', re.DOTALL)
RE_SOURCE = re.compile(r'<div class="line source[^"]*"[^>]*>(.*?)</div>', re.DOTALL)
RE_ABSTRACT = re.compile(r'<div class="abstract">(.*?)</div>', re.DOTALL)
RE_DOI = re.compile(r'(?:doi[\.\s:/]+|10\.)(\d{4,9}/[^\s"<>,]+)', re.IGNORECASE)
RE_YEAR = re.compile(r'\b(19|20)\d{2}\b')


def _strip_tags(html: str) -> str:
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html or '')).strip()


def _parse_search_html(html: str) -> list[tuple[Article, dict]]:
    results: list[tuple[Article, dict]] = []
    for m in RE_ITEM.finditer(html):
        external_id, body = m.group(1), m.group(2)
        tm = RE_TITLE.search(body)
        if not tm:
            continue
        url, title_html = tm.group(1), _strip_tags(tm.group(2))
        if not title_html:
            continue
        # Resolve URL absoluta
        if url.startswith("/"):
            url = "https://search.scielo.org" + url

        authors = None
        am = RE_AUTHORS.search(body)
        if am:
            authors = _strip_tags(am.group(1))

        journal = year = None
        sm = RE_SOURCE.search(body)
        if sm:
            src = _strip_tags(sm.group(1))
            ym = RE_YEAR.search(src)
            if ym:
                year = ym.group(0)
                journal = src.replace(year, "").strip(" ,;.-")
            else:
                journal = src or None

        abstract = None
        abm = RE_ABSTRACT.search(body)
        if abm:
            abstract = _strip_tags(abm.group(1))

        doi = None
        dm = RE_DOI.search(body)
        if dm:
            doi = "10." + dm.group(1).rstrip(".,;)")

        article = Article(
            source=SOURCE_NAME, external_id=external_id,
            title=title_html, authors=authors, year=year, journal=journal,
            abstract=abstract, doi=doi, external_url=url,
        )
        raw = {"id": external_id, "url": url, "title": title_html, "authors": authors,
               "journal": journal, "year": year, "abstract": abstract, "doi": doi}
        results.append((article, raw))
    return results
