"""Scraper Playwright para o Rayyan.

Estratégia: o Rayyan é um SPA Angular. Os artigos são carregados via XHR/fetch
de uma API JSON interna. Em vez de raspar o DOM (frágil), interceptamos as
respostas HTTP de rotas que retornam artigos e extraímos dali.

Como não conhecemos a priori os endpoints exatos, o scraper:
  1. Faz login pela tela de auth
  2. Navega para /reviews/{review_id}/fulltext
  3. Escuta TODAS as respostas JSON do domínio rayyan.ai
  4. Heurística: se o JSON contém uma lista cujos itens tem campos típicos de
     artigo (title/abstract/authors/doi), persiste no SQLite.
  5. Faz scroll incremental até o número de artigos parar de crescer.

Use `vicky scrape --debug` para dumpar todas as respostas observadas em ./debug/
para inspecionar e ajustar heurísticas se necessário.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Response,
    async_playwright,
)

from .config import Config
from .storage import Article, connect, upsert_article


@dataclass
class RayyanCreds:
    email: str
    password: str
    review_id: str

LOGIN_URL = "https://rayyan.ai/users/sign_in"
DEBUG_DIR = Path("./debug")


@dataclass
class ScrapeStats:
    seen: int = 0
    persisted: int = 0


# ─── Heurísticas ─────────────────────────────────────────────────────────────

def looks_like_article_list(payload: Any) -> list[dict] | None:
    """Procura a chave 'data' com lista de artigos (formato do endpoint /results)."""
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            first = data[0]
            if "title" in first and ("abstracts" in first or "id" in first):
                return data
        # busca recursiva como fallback
        for v in payload.values():
            found = looks_like_article_list(v)
            if found:
                return found
    return None


def extract_article(raw: dict, review_id: str) -> Article | None:
    """Extrai campos de um item da API do Rayyan para nosso dataclass Article."""
    rid = str(raw.get("id") or "").strip()
    title = (raw.get("title") or "").strip()
    if not rid or not title:
        return None

    # authors é lista de strings ou de dicts
    authors_raw = raw.get("authors") or []
    if isinstance(authors_raw, list):
        authors = ", ".join(
            (a.get("name") if isinstance(a, dict) else str(a)) for a in authors_raw if a
        ) or None
    else:
        authors = None

    # abstracts é lista de {content, label_display, english?} — pega o primeiro com conteúdo
    abstract = None
    abstracts = raw.get("abstracts")
    if isinstance(abstracts, list):
        for a in abstracts:
            if isinstance(a, dict) and a.get("content"):
                abstract = a["content"]
                break

    # citation é string tipo "Breast cancer - Volume 32, Issue 5..."
    journal = raw.get("citation") or None

    # link externo (PubMed/DOI) — pode vir como lista, string, ou None
    url_raw = raw.get("url")
    if isinstance(url_raw, list):
        url_raw = next((u for u in url_raw if u), None)
    doi_raw = raw.get("doi")
    if isinstance(doi_raw, list):
        doi_raw = next((d for d in doi_raw if d), None)
    external_url = url_raw or (f"https://doi.org/{doi_raw}" if doi_raw else None)

    return Article(
        rayyan_id=rid,
        title=str(title),
        authors=authors,
        year=str(raw.get("year") or "") or None,
        journal=str(journal) if journal else None,
        abstract=str(abstract) if abstract else None,
        doi=str(doi_raw) if doi_raw else None,
        rayyan_url=str(external_url) if external_url
            else f"https://new.rayyan.ai/reviews/{review_id}/fulltext?article={rid}",
    )


# ─── Login ───────────────────────────────────────────────────────────────────

async def login(page: Page, email: str, password: str) -> None:
    """Login no Rayyan legado (Rails). Form: user[email]/user[password]."""
    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    await page.wait_for_selector("#user_email", timeout=30_000)
    await page.fill("#user_email", email)
    await page.fill("#user_password", password)
    # Submeter o form de email/password (não os de OAuth Google/MS/Apple).
    # Pressionar Enter no campo de senha submete o form que o contém.
    async with page.expect_navigation(timeout=30_000):
        await page.press("#user_password", "Enter")
    # Após login bem-sucedido, sai de /users/sign_in
    if "/sign_in" in page.url:
        # Pode ter mensagem de erro
        err = await page.locator(".alert, .flash, .error").all_inner_texts()
        raise RuntimeError(
            f"Falha no login do Rayyan. URL final: {page.url}. "
            f"Mensagens: {err if err else '(nenhuma)'}"
        )


# ─── Scrape loop ─────────────────────────────────────────────────────────────

async def scrape(creds: RayyanCreds, workspace_id: int, *,
                 headless: bool = True, limit: int | None = None,
                 debug: bool = False) -> ScrapeStats:
    """Raspa artigos do Rayyan usando creds do workspace e persiste no workspace_id dado."""
    stats = ScrapeStats()
    if debug:
        DEBUG_DIR.mkdir(exist_ok=True)

    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(headless=headless)
        context: BrowserContext = await browser.new_context()
        page: Page = await context.new_page()

        captured: list[dict] = []

        async def on_response(resp: Response) -> None:
            if "rayyan.ai" not in resp.url:
                return
            ctype = resp.headers.get("content-type", "")
            if "json" not in ctype:
                return
            try:
                payload = await resp.json()
            except Exception:
                return
            if debug:
                fname = DEBUG_DIR / (re.sub(r"[^a-z0-9]+", "_", resp.url[-80:]) + ".json")
                fname.write_text(json.dumps(payload, ensure_ascii=False, indent=2)[:200_000])
            articles = looks_like_article_list(payload)
            if articles:
                captured.extend(articles)

        page.on("response", on_response)

        await login(page, creds.email, creds.password)

        target_url = f"https://new.rayyan.ai/reviews/{creds.review_id}/fulltext"
        await page.goto(target_url, wait_until="networkidle")
        await asyncio.sleep(2)  # garante 1ª batch carregada

        # Estratégia de scroll: busca TODOS os containers scrolláveis na página e
        # rola cada um até o fim. SPAs costumam usar containers virtualizados
        # internos, e scroll na window não dispara o IntersectionObserver deles.
        SCROLL_JS = """
        () => {
            const all = document.querySelectorAll('*');
            let scrolled = 0;
            for (const el of all) {
                if (el.scrollHeight > el.clientHeight + 50) {
                    el.scrollTop = el.scrollHeight;
                    scrolled++;
                }
            }
            window.scrollTo(0, document.body.scrollHeight);
            return scrolled;
        }
        """

        last_count = -1
        stable_iters = 0
        for i in range(80):  # cap ~13 min
            await page.evaluate(SCROLL_JS)
            await page.keyboard.press("End")
            await asyncio.sleep(2)
            current = len(captured)
            if current == last_count:
                stable_iters += 1
                if stable_iters >= 5:
                    break
            else:
                stable_iters = 0
                last_count = current
            if limit and current >= limit:
                break

        await browser.close()

    # Persiste no SQLite escopado pelo workspace (dedup por rayyan_id)
    seen_ids: set[str] = set()
    with connect() as conn:
        for raw in captured:
            art = extract_article(raw, creds.review_id)
            if not art or art.rayyan_id in seen_ids:
                continue
            seen_ids.add(art.rayyan_id)
            upsert_article(conn, workspace_id, art, raw)
            stats.persisted += 1
            if limit and stats.persisted >= limit:
                break

    stats.seen = len(captured)
    return stats
