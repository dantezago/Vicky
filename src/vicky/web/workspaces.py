"""Modelo de Workspaces — cada usuário tem o seu próprio (1:1)."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from ..storage import connect


@dataclass
class Workspace:
    id: int
    name: str
    owner_user_id: int
    rayyan_email: str | None
    rayyan_password: str | None
    rayyan_review_id: str | None
    openrouter_model: str
    openrouter_api_key: str | None
    created_at: str

    @property
    def has_rayyan_credentials(self) -> bool:
        return bool(self.rayyan_email and self.rayyan_password and self.rayyan_review_id)

    @property
    def has_own_api_key(self) -> bool:
        return bool(self.openrouter_api_key and self.openrouter_api_key.strip())


def _row_to_ws(r: sqlite3.Row) -> Workspace:
    # Compatibilidade: se a coluna ainda não existe no row (DB antigo), usa None
    api_key = None
    try:
        api_key = r["openrouter_api_key"]
    except (IndexError, KeyError):
        pass
    return Workspace(
        id=r["id"], name=r["name"], owner_user_id=r["owner_user_id"],
        rayyan_email=r["rayyan_email"], rayyan_password=r["rayyan_password"],
        rayyan_review_id=r["rayyan_review_id"],
        openrouter_model=r["openrouter_model"] or "openai/gpt-4o-mini",
        openrouter_api_key=api_key,
        created_at=r["created_at"],
    )


def get_or_create_for_user(user_id: int, *, name: str | None = None,
                           default_model: str = "openai/gpt-4o-mini") -> Workspace:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM workspaces WHERE owner_user_id=? ORDER BY id LIMIT 1", (user_id,)
        ).fetchone()
        if row:
            return _row_to_ws(row)
        u = conn.execute("SELECT name FROM users WHERE id=?", (user_id,)).fetchone()
        ws_name = name or f"Workspace de {u['name'] if u else 'usuário'}"
        cur = conn.execute(
            "INSERT INTO workspaces (name, owner_user_id, openrouter_model) VALUES (?, ?, ?)",
            (ws_name, user_id, default_model),
        )
        new_id = cur.lastrowid
        return _row_to_ws(
            conn.execute("SELECT * FROM workspaces WHERE id=?", (new_id,)).fetchone()
        )


def get_by_id(ws_id: int) -> Workspace | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        return _row_to_ws(row) if row else None


def update_settings(ws_id: int, *, name: str | None = None,
                    rayyan_email: str | None = None, rayyan_password: str | None = None,
                    rayyan_review_id: str | None = None,
                    openrouter_model: str | None = None,
                    openrouter_api_key: str | None = None) -> Workspace:
    with connect() as conn:
        existing = conn.execute("SELECT * FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        if not existing:
            raise LookupError(f"Workspace {ws_id} não encontrado")
        fields, values = [], []
        if name is not None and name != "":
            fields.append("name=?"); values.append(name)
        if rayyan_email is not None:
            fields.append("rayyan_email=?"); values.append(rayyan_email or None)
        if rayyan_password is not None and rayyan_password != "":
            if rayyan_password == "__clear__":
                fields.append("rayyan_password=?"); values.append(None)
            else:
                fields.append("rayyan_password=?"); values.append(rayyan_password)
        if rayyan_review_id is not None:
            fields.append("rayyan_review_id=?"); values.append(rayyan_review_id or None)
        if openrouter_model is not None and openrouter_model != "":
            fields.append("openrouter_model=?"); values.append(openrouter_model)
        if openrouter_api_key is not None:
            # API key: senha em branco = não alterar; "__clear__" = apagar
            if openrouter_api_key == "__clear__":
                fields.append("openrouter_api_key=?"); values.append(None)
            elif openrouter_api_key.strip() != "":
                fields.append("openrouter_api_key=?"); values.append(openrouter_api_key.strip())
        if fields:
            values.append(ws_id)
            conn.execute(f"UPDATE workspaces SET {', '.join(fields)} WHERE id=?", values)
        return _row_to_ws(
            conn.execute("SELECT * FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        )


def list_all() -> list[Workspace]:
    with connect() as conn:
        rows = conn.execute("SELECT * FROM workspaces ORDER BY id").fetchall()
        return [_row_to_ws(r) for r in rows]
