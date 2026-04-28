"""Cliente OpenRouter (compatível com SDK OpenAI). Faz analyze + double-check."""

from __future__ import annotations

import json
from dataclasses import dataclass

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Config
from .prompts import (
    analyzer_system_prompt,
    analyzer_user_prompt,
    double_check_system_prompt,
    double_check_user_prompt,
)
from .storage import Analysis, Article, DoubleCheck

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


def make_client(cfg: Config, api_key_override: str | None = None) -> OpenAI:
    """Cria cliente OpenRouter. Se `api_key_override` for dado, usa ele
    em vez da chave global (multi-tenant: cada workspace usa sua própria
    quota OpenRouter, não compete com outros usuários por rate-limit)."""
    api_key = api_key_override or cfg.openrouter_api_key
    return OpenAI(
        base_url=OPENROUTER_BASE_URL,
        api_key=api_key,
        default_headers={
            "HTTP-Referer": cfg.openrouter_referer,
            "X-Title": cfg.openrouter_app_title,
        },
        timeout=30.0,
        max_retries=0,
    )


@dataclass
class LLMResult:
    parsed: dict
    raw: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    duration_ms: int = 0
    generation_id: str = ""


def _safe_parse_json(raw: str) -> dict:
    """Tenta parsear JSON do LLM tolerando lixo comum (texto antes/depois, ```json fences)."""
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Tenta extrair primeiro objeto JSON balanceado (cobre fences ```json ... ``` e ruído)
    start = raw.find("{")
    if start == -1:
        return {}
    depth = 0
    for i in range(start, len(raw)):
        ch = raw[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(raw[start:i + 1])
                except json.JSONDecodeError:
                    return {}
    return {}


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    reraise=True,
)
def _call(client: OpenAI, model: str, system: str, user: str) -> LLMResult:
    import time
    t0 = time.monotonic()
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        temperature=0.1,
        # max_tokens=2000: a resposta inclui decision + reason + summary_pt (3-5 linhas) +
        # criteria_matched/violated (listas) + quality_score + score_breakdown (6 chaves).
        # Com 700 a JSON era truncada em ~47% dos casos, deixando summary_pt vazio.
        max_tokens=2000,
    )
    dt = int((time.monotonic() - t0) * 1000)
    raw = resp.choices[0].message.content or ""
    parsed = _safe_parse_json(raw)
    usage = getattr(resp, "usage", None)
    return LLMResult(
        parsed=parsed, raw=raw,
        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
        duration_ms=dt,
        generation_id=getattr(resp, "id", "") or "",
    )


def _normalize_decision(value) -> str:
    """Normaliza a decision retornada pelo LLM para um dos 3 valores canônicos."""
    if not isinstance(value, str):
        return "uncertain"
    v = value.strip().lower()
    if v in ("include", "incluir", "incluido", "incluído", "yes", "sim", "true", "1"):
        return "include"
    if v in ("exclude", "excluir", "excluido", "excluído", "no", "não", "nao", "false", "0"):
        return "exclude"
    if v in ("uncertain", "incerto", "maybe", "talvez", "duvidoso"):
        return "uncertain"
    return "uncertain"


def _normalize_score(value) -> int | None:
    """Aceita int direto, string '85', float '85.5'. Retorna int 0-100 ou None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            n = int(value)
            return max(0, min(100, n))
        except (ValueError, OverflowError):
            return None
    if isinstance(value, str):
        s = value.strip().rstrip("%").strip()
        if not s:
            return None
        try:
            return max(0, min(100, int(float(s))))
        except (ValueError, OverflowError):
            return None
    return None


def _normalize_list(value) -> list:
    """Garante que o campo seja list[str]. LLM às vezes retorna string solta."""
    if isinstance(value, list):
        return [str(x) for x in value if x]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def analyze_article(client: OpenAI, model: str, art: Article) -> Analysis:
    result = _call(
        client,
        model,
        analyzer_system_prompt(),
        analyzer_user_prompt(
            title=art.title, authors=art.authors, year=art.year,
            journal=art.journal, abstract=art.abstract, doi=art.doi,
        ),
    )
    p = result.parsed
    return Analysis(
        source=art.source, external_id=art.external_id,
        decision=_normalize_decision(p.get("decision")),
        reason=str(p.get("reason") or ""),
        summary_pt=str(p.get("summary_pt") or ""),
        criteria_matched=_normalize_list(p.get("criteria_matched")),
        criteria_violated=_normalize_list(p.get("criteria_violated")),
        model=model,
        quality_score=_normalize_score(p.get("quality_score")),
        score_breakdown=p.get("score_breakdown") if isinstance(p.get("score_breakdown"), dict) else None,
    )


def double_check_exclusion(
    client: OpenAI, model: str, art: Article, original: Analysis
) -> DoubleCheck:
    result = _call(
        client,
        model,
        double_check_system_prompt(),
        double_check_user_prompt(
            title=art.title, authors=art.authors, year=art.year,
            abstract=art.abstract,
            original_decision=original.decision,
            original_reason=original.reason,
            criteria_violated=original.criteria_violated,
        ),
    )
    p = result.parsed
    return DoubleCheck(
        source=art.source, external_id=art.external_id,
        agrees=bool(p.get("agrees", True)),
        final_decision=_normalize_decision(p.get("final_decision")) if p.get("final_decision") else original.decision,
        explanation=str(p.get("explanation") or ""),
        model=model,
    )


# Expose raw text + usage alongside parsed result for storage
def analyze_with_raw(client: OpenAI, model: str, art: Article,
                     criteria: str | None = None,
                     topic: str | None = None,
                     review_type: str = "systematic_review",
                     rigidity_mode: str = "padrao") -> tuple[Analysis, str, LLMResult]:
    result = _call(
        client, model,
        analyzer_system_prompt(criteria, topic, review_type, rigidity_mode),
        analyzer_user_prompt(
            title=art.title, authors=art.authors, year=art.year,
            journal=art.journal, abstract=art.abstract, doi=art.doi,
        ),
    )
    p = result.parsed
    return (
        Analysis(
            source=art.source, external_id=art.external_id,
            decision=_normalize_decision(p.get("decision")),
            reason=str(p.get("reason") or ""),
            summary_pt=str(p.get("summary_pt") or ""),
            criteria_matched=_normalize_list(p.get("criteria_matched")),
            criteria_violated=_normalize_list(p.get("criteria_violated")),
            model=model,
            quality_score=_normalize_score(p.get("quality_score")),
            score_breakdown=p.get("score_breakdown") if isinstance(p.get("score_breakdown"), dict) else None,
        ),
        result.raw,
        result,
    )


def double_check_with_raw(
    client: OpenAI, model: str, art: Article, original: Analysis,
    criteria: str | None = None,
    topic: str | None = None,
    review_type: str = "systematic_review",
) -> tuple[DoubleCheck, str, LLMResult]:
    result = _call(
        client, model, double_check_system_prompt(criteria, topic, review_type),
        double_check_user_prompt(
            title=art.title, authors=art.authors, year=art.year,
            abstract=art.abstract,
            original_decision=original.decision,
            original_reason=original.reason,
            criteria_violated=original.criteria_violated,
        ),
    )
    p = result.parsed
    raw_final = p.get("final_decision")
    return (
        DoubleCheck(
            source=art.source, external_id=art.external_id,
            agrees=bool(p.get("agrees", True)),
            final_decision=_normalize_decision(raw_final) if raw_final else original.decision,
            explanation=str(p.get("explanation") or ""),
            model=model,
        ),
        result.raw,
        result,
    )
