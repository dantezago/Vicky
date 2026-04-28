"""Modelo de usuários: criação, autenticação, RBAC, gestão de créditos."""

from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from typing import Literal

from passlib.context import CryptContext

from ..storage import connect

Role = Literal["admin", "operacional", "visualizador"]
Status = Literal["active", "inactive"]

ROLE_LABELS = {
    "admin": "Administrador",
    "operacional": "Operacional",
    "visualizador": "Visualizador",
}

# Permissões: o que cada papel pode fazer
PERMISSIONS = {
    "admin": {"view_records", "view_users", "manage_users", "manage_credits",
              "edit_records", "view_api_usage"},
    "operacional": {"view_records", "edit_records"},
    "visualizador": {"view_records"},
}

# Saldo inicial pra usuários comuns: 0 (admin libera créditos sob demanda).
# Admins sempre ficam com saldo "infinito".
DEFAULT_INITIAL_CREDITS = 0
ADMIN_DEFAULT_CREDITS = 9999

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


@dataclass
class User:
    id: int
    email: str
    name: str
    role: Role
    status: Status
    credits: int
    created_at: str

    @property
    def role_label(self) -> str:
        return ROLE_LABELS.get(self.role, self.role)

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    @property
    def has_unlimited_credits(self) -> bool:
        # Admin nunca é bloqueado por crédito (mas mostramos o saldo mesmo assim).
        return self.role == "admin"

    def can(self, permission: str) -> bool:
        return permission in PERMISSIONS.get(self.role, set())


def _row_to_user(r: sqlite3.Row) -> User:
    # `credits` foi adicionado na migração v5→v6 — defesa para versões antigas.
    credits = r["credits"] if "credits" in r.keys() else 0
    return User(
        id=r["id"], email=r["email"], name=r["name"],
        role=r["role"], status=r["status"], credits=credits,
        created_at=r["created_at"],
    )


def hash_password(plain: str) -> str:
    return pwd_ctx.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_ctx.verify(plain, hashed)
    except Exception:
        return False


def create_user(*, email: str, password: str, name: str, role: Role,
                status: Status = "active",
                credits: int | None = None) -> User:
    """Cria usuário E auto-cria seu workspace (1:1).

    Se `credits` não for passado, usa default por papel: admin=9999, demais=0
    (admin libera créditos sob demanda).
    """
    if role not in PERMISSIONS:
        raise ValueError(f"Papel inválido: {role}")
    if credits is None:
        credits = ADMIN_DEFAULT_CREDITS if role == "admin" else DEFAULT_INITIAL_CREDITS
    credits = max(0, int(credits))
    with connect() as conn:
        conn.execute(
            "INSERT INTO users (email, password_hash, name, role, status, credits) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (email.lower().strip(), hash_password(password), name, role, status, credits),
        )
        u = _get_by_email(conn, email)
        # Auto-criar workspace para o novo usuário
        conn.execute(
            "INSERT INTO workspaces (name, owner_user_id, openrouter_model) VALUES (?, ?, ?)",
            (f"Workspace de {name}", u.id, "openai/gpt-4o-mini"),
        )
        return u


def update_user(user_id: int, *, name: str | None = None, role: Role | None = None,
                status: Status | None = None, password: str | None = None,
                credits: int | None = None) -> User:
    with connect() as conn:
        existing = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        if not existing:
            raise LookupError(f"Usuário {user_id} não encontrado")
        fields: list[str] = []
        values: list[object] = []
        if name is not None:
            fields.append("name=?"); values.append(name)
        if role is not None:
            if role not in PERMISSIONS:
                raise ValueError(f"Papel inválido: {role}")
            fields.append("role=?"); values.append(role)
        if status is not None:
            fields.append("status=?"); values.append(status)
        if password:
            fields.append("password_hash=?"); values.append(hash_password(password))
        if credits is not None:
            fields.append("credits=?"); values.append(max(0, int(credits)))
        if fields:
            values.append(user_id)
            conn.execute(f"UPDATE users SET {', '.join(fields)} WHERE id=?", values)
        return _row_to_user(conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())


def delete_user(user_id: int) -> None:
    """Remove usuário e todos os dados ligados (workspace, projetos, artigos, ...).

    Cascata garantida via FK ON DELETE CASCADE no schema. Admin não pode se auto-deletar
    (caller deve checar antes — `delete_user` só faz a operação).
    """
    with connect() as conn:
        existing = conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone()
        if not existing:
            raise LookupError(f"Usuário {user_id} não encontrado")
        # Limpa dados em ordem topológica reversa, defensivo (caso FK não tenha CASCADE)
        conn.execute("DELETE FROM user_decisions WHERE decided_by=?", (user_id,))
        conn.execute("DELETE FROM llm_usage WHERE user_id=?", (user_id,))
        # Workspaces do user → cascata em projects, articles, analyses, double_checks, jobs
        ws_rows = conn.execute("SELECT id FROM workspaces WHERE owner_user_id=?", (user_id,)).fetchall()
        for ws in ws_rows:
            wsid = ws["id"]
            conn.execute("DELETE FROM jobs WHERE project_id IN (SELECT id FROM projects WHERE workspace_id=?)", (wsid,))
            conn.execute("DELETE FROM double_checks WHERE project_id IN (SELECT id FROM projects WHERE workspace_id=?)", (wsid,))
            conn.execute("DELETE FROM analyses WHERE project_id IN (SELECT id FROM projects WHERE workspace_id=?)", (wsid,))
            conn.execute("DELETE FROM articles WHERE workspace_id=?", (wsid,))
            conn.execute("DELETE FROM projects WHERE workspace_id=?", (wsid,))
        conn.execute("DELETE FROM workspaces WHERE owner_user_id=?", (user_id,))
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))


def add_credits(user_id: int, delta: int) -> User:
    """Soma `delta` ao saldo (delta pode ser negativo). Saldo nunca fica < 0."""
    with connect() as conn:
        row = conn.execute("SELECT credits FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise LookupError(f"Usuário {user_id} não encontrado")
        new_balance = max(0, int(row["credits"]) + int(delta))
        conn.execute("UPDATE users SET credits=? WHERE id=?", (new_balance, user_id))
        return _row_to_user(conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())


def consume_credit(user_id: int) -> bool:
    """Tenta consumir 1 crédito atomicamente. Retorna False se sem saldo.

    Admins sempre passam (saldo cosmético, não bloqueia). Para demais, debita 1.
    """
    with connect() as conn:
        row = conn.execute("SELECT role, credits FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            return False
        if row["role"] == "admin":
            # Não debita admin — saldo é cosmético pra eles.
            return True
        if int(row["credits"]) <= 0:
            return False
        # UPDATE condicional pra evitar race em duplo-clique.
        cur = conn.execute(
            "UPDATE users SET credits = credits - 1 WHERE id=? AND credits > 0",
            (user_id,),
        )
        return cur.rowcount == 1


def authenticate(email: str, password: str) -> User | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE email=? AND status='active'", (email.lower().strip(),)
        ).fetchone()
        if not row or not verify_password(password, row["password_hash"]):
            return None
        return _row_to_user(row)


def get_by_id(user_id: int) -> User | None:
    with connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
        return _row_to_user(row) if row else None


def _get_by_email(conn: sqlite3.Connection, email: str) -> User:
    row = conn.execute("SELECT * FROM users WHERE email=?", (email.lower().strip(),)).fetchone()
    return _row_to_user(row)


def list_all() -> list[User]:
    with connect() as conn:
        rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
        return [_row_to_user(r) for r in rows]


def count() -> int:
    with connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]


def generate_password(length: int = 12) -> str:
    """Gera senha aleatória legível."""
    alphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))
