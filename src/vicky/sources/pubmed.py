"""PubMed search via NCBI E-utilities (oficial, gratuito, sem login).

Docs: https://www.ncbi.nlm.nih.gov/books/NBK25497/
Rate limit: 3 req/s sem chave, 10 req/s com NCBI_API_KEY.
"""

from __future__ import annotations

import asyncio
import os
import time
import xml.etree.ElementTree as ET
from typing import Callable

import httpx

from ..storage import Article

BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
SOURCE_NAME = "pubmed"
USER_AGENT = "Vicky/1.0 (research; vickyangel@gmail.com)"


async def _http_get_with_retry(
    client: httpx.AsyncClient, url: str, params: dict,
    *, max_attempts: int = 4,
) -> httpx.Response:
    """GET com backoff exponencial para 429/5xx/timeout. Não retry em 4xx (exceto 429)."""
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                return r
            # Retry em 429 (rate limit) e 5xx (servidor)
            if r.status_code == 429 or r.status_code >= 500:
                last_exc = httpx.HTTPStatusError(
                    f"PubMed {r.status_code}", request=r.request, response=r,
                )
                if attempt < max_attempts:
                    await asyncio.sleep(min(2 ** attempt, 16))
                    continue
                r.raise_for_status()
            # 4xx (não 429) — request mal-formada, sem retry
            r.raise_for_status()
            return r
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as e:
            last_exc = e
            if attempt < max_attempts:
                await asyncio.sleep(min(2 ** attempt, 16))
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("PubMed: retries esgotados sem resposta")


async def count_results(query: str, *, timeout: float = 15.0) -> int:
    """Roda só o esearch e devolve `count` — barato, 1 HTTP request, sem efetch.

    Usado para pré-validar search strings antes de comprometer tempo/$ com a
    coleta completa: se o LLM gerou uma string que retorna 0, descobrimos
    em 1 segundo e descartamos sem rodar efetch nem chamar SciELO/Scholar.

    Retorna -1 se a chamada falhar (caller decide o fallback).
    """
    api_key = os.getenv("NCBI_API_KEY")
    headers = {"User-Agent": USER_AGENT}
    params = {"db": "pubmed", "term": query, "retmax": "0", "retmode": "json"}
    if api_key:
        params["api_key"] = api_key
    try:
        async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
            r = await _http_get_with_retry(client, f"{BASE}/esearch.fcgi", params,
                                            max_attempts=2)
            data = r.json()
        return int(data.get("esearchresult", {}).get("count", 0))
    except Exception:
        return -1


async def search(
    query: str,
    *,
    max_results: int = 200,
    progress: Callable[[int, str], None] | None = None,
) -> list[tuple[Article, dict]]:
    """Roda esearch + efetch e devolve lista de (Article, raw_dict)."""
    api_key = os.getenv("NCBI_API_KEY")  # opcional
    headers = {"User-Agent": USER_AGENT}

    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        # 1. esearch — pega lista de PMIDs
        params = {
            "db": "pubmed", "term": query, "retmax": str(max_results),
            "retmode": "json", "sort": "relevance",
        }
        if api_key:
            params["api_key"] = api_key
        if progress: progress(5, "PubMed: buscando…")
        r = await _http_get_with_retry(client, f"{BASE}/esearch.fcgi", params)
        try:
            data = r.json()
        except ValueError:
            if progress: progress(100, "PubMed: resposta não-JSON do esearch")
            return []
        pmids: list[str] = data.get("esearchresult", {}).get("idlist", [])
        if not pmids:
            if progress: progress(100, "PubMed: 0 resultados")
            return []
        total = int(data["esearchresult"].get("count", len(pmids)))
        if progress: progress(15, f"PubMed: {len(pmids)} PMIDs (de {total} totais)")

        # 2. efetch — busca em batches de 100
        results: list[tuple[Article, dict]] = []
        batch_size = 100
        for i in range(0, len(pmids), batch_size):
            batch = pmids[i : i + batch_size]
            params = {
                "db": "pubmed", "id": ",".join(batch),
                "retmode": "xml", "rettype": "abstract",
            }
            if api_key:
                params["api_key"] = api_key
            try:
                r = await _http_get_with_retry(client, f"{BASE}/efetch.fcgi", params)
                results.extend(_parse_efetch_xml(r.text))
            except Exception as e:
                # Falha de um batch não derruba a source toda — continua com o que conseguiu
                if progress:
                    progress(15 + int(85 * (i + len(batch)) / len(pmids)),
                             f"PubMed: batch {i}-{i+len(batch)} falhou ({type(e).__name__}); seguindo")
                continue
            if progress:
                pct = 15 + int(85 * (i + len(batch)) / len(pmids))
                progress(pct, f"PubMed: {len(results)} artigos coletados")
            # Rate limit
            await asyncio.sleep(0.34 if not api_key else 0.11)

    if progress: progress(100, f"PubMed: {len(results)} artigos prontos")
    return results


def _parse_efetch_xml(xml_text: str) -> list[tuple[Article, dict]]:
    """Parse do XML do efetch para extrair Article."""
    out: list[tuple[Article, dict]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out

    for art_node in root.findall(".//PubmedArticle"):
        pmid_node = art_node.find(".//PMID")
        pmid = pmid_node.text if pmid_node is not None else None
        if not pmid:
            continue

        article = art_node.find(".//Article")
        if article is None:
            continue

        title_node = article.find("ArticleTitle")
        title = _clean_title(_text(title_node))
        if title is None:
            vernacular_node = article.find("VernacularTitle")
            title = _clean_title(_text(vernacular_node))
        if title is None:
            book_title_node = art_node.find(".//BookTitle")
            title = _clean_title(_text(book_title_node))
        if title is None:
            title = "(sem título)"

        # Abstract — pode ter vários <AbstractText> com Label
        abstract_parts: list[str] = []
        for at in article.findall(".//Abstract/AbstractText"):
            label = at.get("Label")
            text = _text(at)
            if text:
                abstract_parts.append(f"{label}: {text}" if label else text)
        abstract = "\n".join(abstract_parts) or None

        # Authors
        authors: list[str] = []
        for au in article.findall(".//AuthorList/Author"):
            last = _text(au.find("LastName")) or ""
            init = _text(au.find("Initials")) or ""
            collective = _text(au.find("CollectiveName"))
            if collective:
                authors.append(collective)
            elif last:
                authors.append(f"{last} {init}".strip())
        authors_str = ", ".join(authors[:20]) or None

        # Journal
        jnode = article.find(".//Journal/Title")
        journal = _text(jnode)

        # Year — vários lugares possíveis
        year = (
            _text(article.find(".//Journal/JournalIssue/PubDate/Year"))
            or _text(article.find(".//Journal/JournalIssue/PubDate/MedlineDate"))
            or _text(article.find(".//ArticleDate/Year"))
        )
        if year and len(year) > 4:
            year = year[:4]

        # DOI
        doi = None
        for idn in art_node.findall(".//ArticleId"):
            if idn.get("IdType") == "doi" and idn.text:
                doi = idn.text.strip()
                break

        external_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        a = Article(
            source=SOURCE_NAME, external_id=pmid,
            title=title, authors=authors_str, year=year,
            journal=journal, abstract=abstract, doi=doi,
            external_url=external_url,
        )
        raw = {
            "pmid": pmid, "title": title, "authors": authors,
            "year": year, "journal": journal, "abstract": abstract,
            "doi": doi, "url": external_url,
        }
        out.append((a, raw))
    return out


def _text(node: ET.Element | None) -> str | None:
    if node is None:
        return None
    # Junta texto de todos os filhos (lida com tags inline tipo <i>)
    return "".join(node.itertext()).strip() or None


_BAD_TITLE_MARKERS = {"[not available]", "not available", "[no title available]", "no title available", "[sem título]"}


def _clean_title(raw: str | None) -> str | None:
    """Filtra placeholders do PubMed/SciELO que não são títulos reais.

    PubMed marca artigos sem título traduzido como '[Not Available].' — nesses casos
    o título real costuma estar em VernacularTitle.
    """
    if not raw:
        return None
    stripped = raw.strip().rstrip(".").strip()
    if not stripped:
        return None
    if stripped.lower() in _BAD_TITLE_MARKERS:
        return None
    return stripped
