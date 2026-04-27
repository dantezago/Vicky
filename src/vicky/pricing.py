"""Tabela de preços por modelo do OpenRouter (USD por 1M tokens).

Atualizar manualmente quando preços mudarem em https://openrouter.ai/models.
Fallback DEFAULT_PRICING usado para modelos não listados.
"""

from __future__ import annotations

# (input_per_1m_usd, output_per_1m_usd)
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "openai/gpt-4o-mini":           (0.15, 0.60),
    "openai/gpt-4o":                (2.50, 10.00),
    "openai/gpt-4-turbo":           (10.00, 30.00),
    "openai/gpt-3.5-turbo":         (0.50, 1.50),
    "google/gemini-2.0-flash-001":  (0.10, 0.40),
    "google/gemini-flash-1.5":      (0.075, 0.30),
    "google/gemini-pro-1.5":        (1.25, 5.00),
    "anthropic/claude-3.5-haiku":   (0.80, 4.00),
    "anthropic/claude-3.5-sonnet":  (3.00, 15.00),
    "anthropic/claude-3-opus":      (15.00, 75.00),
    "meta-llama/llama-3.3-70b-instruct": (0.59, 0.79),
    "deepseek/deepseek-chat":       (0.14, 0.28),
}

DEFAULT_PRICING: tuple[float, float] = (1.00, 3.00)


def calc_cost_usd(model: str, prompt_tokens: int, completion_tokens: int) -> float:
    pin, pout = MODEL_PRICING.get(model, DEFAULT_PRICING)
    return (prompt_tokens * pin + completion_tokens * pout) / 1_000_000.0


def has_known_pricing(model: str) -> bool:
    return model in MODEL_PRICING
