"""Configuração: carrega .env de ~/.config/vicky/.env."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ENV_PATH = Path.home() / ".config" / "vicky" / ".env"
DATA_DIR = Path.home() / ".local" / "share" / "vicky"
DB_PATH = DATA_DIR / "vicky.db"


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Variável {name} ausente. Edite {ENV_PATH} e preencha."
        )
    return value


@dataclass(frozen=True)
class Config:
    rayyan_email: str
    rayyan_password: str
    rayyan_review_id: str
    openrouter_api_key: str
    openrouter_model: str
    openrouter_referer: str
    openrouter_app_title: str

    @classmethod
    def load(cls) -> "Config":
        if not ENV_PATH.exists():
            raise RuntimeError(
                f"Arquivo de credenciais não encontrado: {ENV_PATH}\n"
                "Crie-o seguindo o exemplo em docs/."
            )
        load_dotenv(ENV_PATH)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        return cls(
            rayyan_email=_require("RAYYAN_EMAIL"),
            rayyan_password=_require("RAYYAN_PASSWORD"),
            rayyan_review_id=_require("RAYYAN_REVIEW_ID"),
            openrouter_api_key=_require("OPENROUTER_API_KEY"),
            openrouter_model=os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
            openrouter_referer=os.getenv(
                "OPENROUTER_REFERER", "https://github.com/vickyangel/vicky"
            ),
            openrouter_app_title=os.getenv("OPENROUTER_APP_TITLE", "Vicky Rayyan Triage"),
        )
