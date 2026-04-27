"""Reconciliação de custo real via OpenRouter.

Após cada chamada ao chat completions, o `resp.id` é o `generation_id`. O endpoint
`GET /api/v1/generation?id=<id>` devolve o gasto EXATO em USD daquela chamada
específica (`total_cost`) — não estimado por tabela de preços. A row leva alguns
segundos para ficar disponível, então tentamos com retries.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request

GENERATION_URL = "https://openrouter.ai/api/v1/generation"


async def fetch_real_cost(
    generation_id: str,
    api_key: str,
    *,
    max_retries: int = 12,
    initial_delay: float = 1.5,
) -> dict | None:
    """Busca o gasto real da chamada. Retorna o `data` do response ou None se falhar.

    Campos relevantes em `data`: `total_cost` (USD), `native_tokens_prompt`,
    `native_tokens_completion`, `generation_time`, `cache_discount`, `provider_name`.
    """
    def _try() -> dict | None:
        req = urllib.request.Request(
            f"{GENERATION_URL}?id={urllib.parse.quote(generation_id)}",
            headers={"Authorization": f"Bearer {api_key}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=8) as r:
                payload = json.loads(r.read())
                return payload.get("data")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    delay = initial_delay
    for _ in range(max_retries):
        await asyncio.sleep(delay)
        try:
            data = await asyncio.to_thread(_try)
            if data:
                return data
        except Exception:
            pass
        delay = min(delay * 1.3, 5.0)
    return None
