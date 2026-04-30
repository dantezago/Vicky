"""FastAPI app: rotas, sessões, RBAC, MULTITENANCY (workspace + projects)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import Cookie, Depends, FastAPI, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, URLSafeSerializer

import asyncio
from ..config import Config
from ..pipeline import (
    is_pipeline_zombie,
    reset_zombie_pipeline,
    resume_interrupted_pipelines,
    schedule_pipeline,
    set_main_loop,
)
from ..storage import (
    add_favorite,
    add_project_favorite,
    clear_user_decision,
    connect,
    project_stats,
    remove_favorite,
    remove_project_favorite,
    upsert_user_decision,
)
from . import projects as projects_module
from . import queries, users, workspaces
from .users import User
from .workspaces import Workspace

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"

SESSION_COOKIE = "vicky_session"


def create_app() -> FastAPI:
    cfg = Config.load()
    app = FastAPI(title="Vicky", docs_url=None, redoc_url=None)
    app.state.cfg = cfg
    app.state.serializer = URLSafeSerializer(cfg.openrouter_api_key, salt="vicky-session-v1")
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    templates = Jinja2Templates(directory=TEMPLATES_DIR)
    templates.env.globals["app_name"] = "Vicky"

    # Filtro brt: converte timestamps UTC do SQLite (datetime('now')) para o
    # fuso de Brasília (America/Sao_Paulo, UTC-3). SQLite grava sempre em UTC;
    # a UI precisa exibir no horário local do usuário brasileiro.
    from datetime import datetime, timezone, timedelta
    BRT = timezone(timedelta(hours=-3))

    def _to_brt(value, fmt: str = "%d/%m/%Y %H:%M:%S"):
        if not value:
            return ""
        s = str(value).strip()
        # SQLite costuma gravar 'YYYY-MM-DD HH:MM:SS' (UTC). Pode vir com 'T' do ISO.
        s_norm = s.replace("T", " ").split(".")[0][:19]
        try:
            dt = datetime.strptime(s_norm, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(s_norm[:10], "%Y-%m-%d")
            except ValueError:
                return s
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return dt_utc.astimezone(BRT).strftime(fmt)

    templates.env.filters["brt"] = _to_brt

    def _br_date(value, *, with_time: bool = False):
        """Converte timestamp do banco (UTC, formato 'YYYY-MM-DD[ HH:MM:SS]')
        em string DD/MM/AAAA — ou DD/MM/AAAA HH:MM se with_time=True. Tolera
        valores nulos, datetimes e strings já formatadas."""
        if value is None or value == "":
            return ""
        if isinstance(value, datetime):
            return value.strftime("%d/%m/%Y %H:%M" if with_time else "%d/%m/%Y")
        s = str(value).strip()
        s_norm = s.replace("T", " ").split(".")[0][:19]
        # Tenta parsear data+hora; cai pra só data se necessário
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s_norm[:len("0000-00-00 00:00:00")] if fmt == "%Y-%m-%d %H:%M:%S" else (s_norm[:16] if fmt == "%Y-%m-%d %H:%M" else s_norm[:10]), fmt)
                break
            except ValueError:
                continue
        else:
            return s
        return dt.strftime("%d/%m/%Y %H:%M" if with_time else "%d/%m/%Y")

    templates.env.filters["brdate"] = _br_date
    templates.env.filters["brdatetime"] = lambda v: _br_date(v, with_time=True)

    # ─── Filtro ABNT: monta referência no formato ABNT NBR 6023:2018 ────────
    # Inclui autores formatados (SOBRENOME, N.), título, periódico em itálico,
    # ano, DOI quando disponível e URL com data de acesso.
    _MESES_ABNT = {1: "jan.", 2: "fev.", 3: "mar.", 4: "abr.", 5: "maio",
                   6: "jun.", 7: "jul.", 8: "ago.", 9: "set.", 10: "out.",
                   11: "nov.", 12: "dez."}

    def _format_authors_abnt(authors_raw: str | None) -> str:
        if not authors_raw:
            return "[s. n.]"
        # Aceita "Sobrenome A, Sobrenome B" ou "Nome Sobrenome; Nome Sobrenome"
        sep = ";" if ";" in authors_raw else ","
        names = [n.strip() for n in authors_raw.split(sep) if n.strip()]
        formatted: list[str] = []
        for n in names[:6]:
            parts = n.split()
            if len(parts) == 1:
                formatted.append(parts[0].upper() + ".")
                continue
            # Heurística: estilo PubMed "Sobrenome IN" → primeiro token é sobrenome,
            # último é iniciais. Estilo "Nome Sobrenome" → último token é sobrenome.
            last_token = parts[-1]
            first_token = parts[0]
            if len(last_token) <= 3 and last_token.isupper():
                # PubMed style: "Sobrenome IN" → first_token = sobrenome
                surname = first_token
                initials = "".join(c + "." for c in last_token)
            else:
                surname = last_token
                initials = "".join(p[0].upper() + ". " for p in parts[:-1]).strip()
            formatted.append(f"{surname.upper()}, {initials}")
        result = "; ".join(formatted)
        if len(names) > 6:
            result += " et al."
        return result

    def _abnt_reference(record: dict) -> str:
        title = (record.get("title") or "[Sem título]").strip().rstrip(".")
        authors = _format_authors_abnt(record.get("authors"))
        journal = (record.get("journal") or "").strip()
        year = record.get("year") or "[s. d.]"
        doi = record.get("doi") or ""
        url = record.get("external_url") or ""

        parts = [f"{authors}. {title}."]
        if journal:
            parts.append(f"<strong><em>{journal}</em></strong>, {year}.")
        else:
            parts.append(f"{year}.")
        if doi:
            parts.append(f"DOI: <span class=\"vk-mono\">{doi}</span>.")
        if url:
            today = datetime.now()
            access = f"{today.day:02d} {_MESES_ABNT[today.month]} {today.year}"
            parts.append(f"Disponível em: &lt;{url}&gt;. Acesso em: {access}.")
        return " ".join(parts)

    templates.env.filters["abnt"] = _abnt_reference
    app.state.templates = templates
    @app.on_event("startup")
    async def _capture_loop():
        set_main_loop(asyncio.get_running_loop())
        # Retoma pipelines que foram interrompidos por crash do servidor.
        # O pipeline é idempotente: análises já feitas são preservadas, só
        # continua de onde parou (artigos pendentes).
        try:
            resumed = resume_interrupted_pipelines()
            if resumed:
                print(f"  ▶ {len(resumed)} pipeline(s) retomado(s) após reinício: {resumed}")
        except Exception as e:
            print(f"  ✗ Falha ao retomar pipelines: {e}")

    register_routes(app)
    return app


# ─── Auth + Workspace dependencies ──────────────────────────────────────────


def _set_session(response: RedirectResponse, app: FastAPI, user: User) -> None:
    token = app.state.serializer.dumps({"uid": user.id})
    response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax",
                        secure=False, max_age=60 * 60 * 24 * 14)


def _clear_session(response: RedirectResponse) -> None:
    response.delete_cookie(SESSION_COOKIE)


def _user_from_request(request: Request) -> User | None:
    session = request.cookies.get(SESSION_COOKIE)
    if not session:
        return None
    try:
        data = request.app.state.serializer.loads(session)
        u = users.get_by_id(data["uid"])
        if u and u.is_active:
            return u
    except (BadSignature, KeyError, TypeError):
        pass
    return None


def get_current_user(
    request: Request,
    session: Annotated[str | None, Cookie(alias=SESSION_COOKIE)] = None,
) -> User | None:
    return _user_from_request(request)


def require_user(user: Annotated[User | None, Depends(get_current_user)]) -> User:
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="login required")
    return user


VIEW_AS_COOKIE = "vk_view_as"


def _resolve_view_as_uid(request: Request, user: User | None) -> int | None:
    """Se admin tem cookie de view-as ativo num GET, retorna o uid alvo. Senão None.

    Mutações (POST/PUT/DELETE) NUNCA são afetadas — ações sempre recaem no
    workspace do admin (que terá 404 ao mexer em projeto alheio)."""
    if user is None or user.role != "admin":
        return None
    if request.method != "GET":
        return None
    raw = request.cookies.get(VIEW_AS_COOKIE)
    if not raw:
        return None
    try:
        target = int(raw)
    except ValueError:
        return None
    if target == user.id:
        return None
    if not users.get_by_id(target):
        return None
    return target


def get_current_workspace(request: Request,
                          user: Annotated[User, Depends(require_user)]) -> Workspace:
    target_uid = _resolve_view_as_uid(request, user)
    if target_uid is not None:
        ws = workspaces.get_or_create_for_user(target_uid)
        return ws
    ws = workspaces.get_or_create_for_user(user.id)
    if ws.owner_user_id != user.id:
        raise HTTPException(403, "Workspace não pertence ao usuário.")
    return ws


def get_effective_user_id(request: Request,
                          user: Annotated[User, Depends(require_user)]) -> int:
    """User-id efetivo respeitando view-as (admin viewing other user's workspace)."""
    target_uid = _resolve_view_as_uid(request, user)
    return target_uid if target_uid is not None else user.id


def require_perm(perm: str):
    def checker(user: Annotated[User, Depends(require_user)]) -> User:
        if not user.can(perm):
            raise HTTPException(403, "forbidden")
        return user
    return checker


def get_project_for_workspace(project_id: int, ws: Workspace):
    """Carrega projeto E confere isolamento (pertence ao workspace do user logado)."""
    p = projects_module.get(project_id)
    if not p or p.workspace_id != ws.id:
        raise HTTPException(404, "Projeto não encontrado neste workspace.")
    return p


def render(request: Request, template: str, ctx: dict, *, status_code: int = 200) -> HTMLResponse:
    templates: Jinja2Templates = request.app.state.templates
    user = _user_from_request(request)
    target_uid = _resolve_view_as_uid(request, user) if user else None
    if target_uid is not None:
        ws = workspaces.get_or_create_for_user(target_uid)
        view_as_user = users.get_by_id(target_uid)
    else:
        ws = workspaces.get_or_create_for_user(user.id) if user else None
        view_as_user = None
    base_ctx = {
        "user": user,
        "workspace": ws,
        "active_path": request.url.path,
        "view_as_user": view_as_user,
        "view_as_active": view_as_user is not None,
    }
    base_ctx.update(ctx)
    return templates.TemplateResponse(request, template, base_ctx, status_code=status_code)


# ─── Routes ──────────────────────────────────────────────────────────────────


def register_routes(app: FastAPI) -> None:

    @app.exception_handler(HTTPException)
    async def auth_redirect(request: Request, exc: HTTPException):
        if exc.status_code == 401:
            return RedirectResponse(url=f"/login?next={request.url.path}", status_code=303)
        if exc.status_code == 403:
            return render(request, "errors/403.html", {"error": str(exc.detail)}, status_code=403)
        if exc.status_code == 404:
            return render(request, "errors/404.html", {}, status_code=404)
        raise exc

    @app.get("/", response_class=HTMLResponse)
    def root(request: Request, user: Annotated[User | None, Depends(get_current_user)]):
        if user:
            return RedirectResponse("/dashboard", status_code=303)
        return render(request, "landing.html", {})

    @app.get("/termos", response_class=HTMLResponse)
    def terms_of_use(request: Request):
        return render(request, "terms.html", {})

    # ── Login / Logout / Signup ────────────────────────────────────────────

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request, next: str = "/dashboard", error: str | None = None):
        return render(request, "login.html", {"next": next, "error": error})

    @app.post("/login")
    def login_submit(request: Request, email: Annotated[str, Form()],
                     password: Annotated[str, Form()],
                     next: Annotated[str, Form()] = "/dashboard"):
        u = users.authenticate(email, password)
        if not u:
            return render(request, "login.html", {"next": next,
                "error": "E-mail ou senha incorretos.", "email": email})
        target = next if next.startswith("/") and not next.startswith("//") else "/dashboard"
        resp = RedirectResponse(target, status_code=303)
        _set_session(resp, request.app, u)
        return resp

    @app.post("/logout")
    def logout():
        resp = RedirectResponse("/login", status_code=303)
        _clear_session(resp)
        return resp

    @app.get("/signup", response_class=HTMLResponse)
    def signup_page(request: Request,
                    user: Annotated[User | None, Depends(get_current_user)],
                    error: str | None = None):
        if user:
            return RedirectResponse("/dashboard", status_code=303)
        return render(request, "signup.html", {"error": error})

    @app.post("/signup")
    def signup_submit(request: Request,
                      nome: Annotated[str, Form()],
                      email: Annotated[str, Form()],
                      senha: Annotated[str, Form()],
                      confirmar_senha: Annotated[str, Form()]):
        from .. import db as _db
        nome = nome.strip()
        email_norm = email.strip().lower()
        ctx = {"nome": nome, "email": email_norm}
        if not nome:
            return render(request, "signup.html", {**ctx, "error": "Informe seu nome."})
        if "@" not in email_norm or "." not in email_norm.split("@")[-1]:
            return render(request, "signup.html", {**ctx, "error": "E-mail inválido."})
        if len(senha) < 8:
            return render(request, "signup.html", {**ctx, "error": "A senha deve ter ao menos 8 caracteres."})
        if senha != confirmar_senha:
            return render(request, "signup.html", {**ctx, "error": "As senhas não coincidem."})
        try:
            u = users.create_user(email=email_norm, password=senha, name=nome, role="operacional")
        except Exception as exc:
            if _db.is_unique_violation(exc):
                return render(request, "signup.html", {**ctx, "error": "Este e-mail já está cadastrado."})
            raise
        resp = RedirectResponse("/dashboard", status_code=303)
        _set_session(resp, request.app, u)
        return resp

    # ── Dashboard (workspace-level) ─────────────────────────────────────────

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(request: Request,
                  user: Annotated[User, Depends(require_perm("view_records"))],
                  ws: Annotated[Workspace, Depends(get_current_workspace)]):
        m = queries.get_workspace_dashboard(ws.id)
        return render(request, "dashboard.html", {"m": m})

    # ── Projects ────────────────────────────────────────────────────────────

    @app.get("/projetos", response_class=HTMLResponse)
    def projects_list(request: Request,
                      user: Annotated[User, Depends(require_perm("view_records"))],
                      ws: Annotated[Workspace, Depends(get_current_workspace)]):
        ps = projects_module.list_for_workspace(ws.id)
        eff_uid = _resolve_view_as_uid(request, user) or user.id
        fav_ids = queries.get_favorite_project_ids(eff_uid, ws.id)
        for p in ps:
            p.is_favorite = p.id in fav_ids
        return render(request, "projects/list.html", {"projects": ps})

    @app.post("/projetos/{project_id}/favoritar")
    def toggle_project_favorite(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        action: Annotated[str, Form()] = "toggle",
        next: Annotated[str, Form()] = "",
    ):
        """Marca/desmarca projeto como favorito do usuário logado."""
        p = get_project_for_workspace(project_id, ws)
        with connect() as conn:
            already = conn.execute(
                "SELECT 1 FROM user_project_favorites WHERE user_id=? AND project_id=?",
                (user.id, p.id),
            ).fetchone()
            if action == "remove" or (action == "toggle" and already):
                remove_project_favorite(conn, user_id=user.id, project_id=p.id)
                state = "off"
            else:
                add_project_favorite(conn, user_id=user.id, project_id=p.id)
                state = "on"
        # Se for chamada AJAX, devolve JSON; senão, redireciona.
        if request.headers.get("x-requested-with") == "fetch":
            return JSONResponse({"state": state})
        target = next or f"/projetos/{p.id}"
        return RedirectResponse(target, status_code=303)

    @app.get("/projetos/novo", response_class=HTMLResponse)
    def projects_new_form(request: Request,
                          user: Annotated[User, Depends(require_perm("view_records"))],
                          ws: Annotated[Workspace, Depends(get_current_workspace)]):
        return render(request, "projects/new.html", {})

    @app.post("/projetos")
    def projects_create(
        request: Request,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        topic: Annotated[str, Form()],
        objective: Annotated[str, Form()] = "",
        years_window: Annotated[int, Form()] = 10,
        target_articles: Annotated[int, Form()] = 40,
        review_type: Annotated[str, Form()] = "systematic_review",
        rigidity_mode: Annotated[str, Form()] = "padrao",
        sources: Annotated[list[str], Form()] = ["pubmed", "scielo", "scholar"],
        auto_run: Annotated[str, Form()] = "",
    ):
        p = projects_module.create(
            workspace_id=ws.id, topic=topic, objective=objective or None,
            years_window=years_window, target_articles=target_articles,
            review_type=review_type, rigidity_mode=rigidity_mode,
            sources=sources or ["pubmed"], created_by=user.id,
        )
        if auto_run == "preview":
            # Pré-visualização de critérios é GRÁTIS — não consome crédito.
            # Crédito só é debitado quando o usuário roda o pipeline completo
            # (rota /iniciar).
            from ..pipeline import schedule_discovery_only
            schedule_discovery_only(p.id)
            return RedirectResponse(
                f"/projetos/{p.id}?msg=Gerando+crit%C3%A9rios+PICO...+aguarde+~30s",
                status_code=303,
            )
        if auto_run:
            # Cobra 1 crédito antes de iniciar. Sem saldo? Cria o projeto mas não dispara.
            if not users.consume_credit(user.id):
                return RedirectResponse(
                    f"/projetos/{p.id}?error=Sem+cr%C3%A9ditos",
                    status_code=303,
                )
            # Marca status='searching' ANTES de redirecionar pra que o detalhe do
            # projeto já mostre "Pipeline rodando" no lugar de "Iniciar pipeline".
            projects_module.update(p.id, status="searching", error=None)
            schedule_pipeline(p.id)
            return RedirectResponse(
                f"/projetos/{p.id}?msg=Pipeline+iniciado%21+Acompanhe+o+progresso+abaixo",
                status_code=303,
            )
        return RedirectResponse(f"/projetos/{p.id}", status_code=303)

    @app.get("/projetos/{project_id}/export")
    def project_export(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        format: str = "csv",     # csv | json | md | pdf
        scope: str = "top_n",    # top_n | included | all
    ):
        """Exporta resultados do projeto. Streaming response."""
        from fastapi.responses import Response
        import csv as _csv, io, json as _json

        p = get_project_for_workspace(project_id, ws)
        topic_safe = "".join(c if c.isalnum() else "-" for c in p.topic)[:60].strip("-")

        # ── Formato PDF: relatório completo (incluídos por score + excluídos) ──
        if format == "pdf":
            from ..pdf_report import generate_project_pdf
            metrics = queries.get_project_dashboard(p.id)
            included = queries.get_included_for_report(p.id, only_top=True)
            below_cutoff = queries.get_below_cutoff_for_report(p.id)
            excluded = queries.get_excluded_for_report(p.id)
            pdf_bytes = generate_project_pdf(
                project=p, metrics=metrics,
                included=included, excluded=excluded,
                below_cutoff=below_cutoff, criteria_md=p.criteria_md,
            )
            return Response(
                content=pdf_bytes,
                media_type="application/pdf",
                headers={
                    "Content-Disposition": f'attachment; filename="vicky-{topic_safe}-relatorio.pdf"',
                },
            )

        # ── Formato XLSX: planilha com abas Resumo/Top/Excluídos ──────────────
        if format == "xlsx":
            from ..xlsx_report import generate_project_xlsx
            metrics = queries.get_project_dashboard(p.id)
            top_n = queries.get_included_for_report(p.id, only_top=True)
            below_cutoff = queries.get_below_cutoff_for_report(p.id)
            excluded = queries.get_excluded_for_report(p.id)
            xlsx_bytes = generate_project_xlsx(
                project=p, metrics=metrics,
                top_n=top_n, below_cutoff=below_cutoff, excluded=excluded,
            )
            return Response(
                content=xlsx_bytes,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f'attachment; filename="vicky-{topic_safe}-relatorio.xlsx"',
                },
            )

        # Decide qual conjunto de artigos vai exportar
        if scope == "top_n":
            rows = queries.get_top_n(p.id, n=p.target_articles)
            label = f"top-{p.target_articles}"
        elif scope == "included":
            res = queries.search_records(p.id, decision="include", per_page=10000, page=1)
            rows = res.items
            label = "incluidos"
        else:  # all
            res = queries.search_records(p.id, per_page=10000, page=1)
            rows = res.items
            label = "todos"

        filename_base = f"vicky-{topic_safe}-{label}"

        if format == "json":
            return Response(
                content=_json.dumps(rows, ensure_ascii=False, indent=2),
                media_type="application/json",
                headers={"Content-Disposition": f'attachment; filename="{filename_base}.json"'},
            )

        if format == "md":
            # Markdown report
            lines = [f"# {p.topic}", "", f"_Exportado em {p.updated_at} · {len(rows)} artigos_", ""]
            if p.criteria_md and scope == "top_n":
                lines.extend(["## Critérios usados", "", p.criteria_md, "", "---", ""])
            lines.append(f"## {label.title()} ({len(rows)})")
            lines.append("")
            for i, r in enumerate(rows, 1):
                score = r.get("quality_score") or "—"
                lines.append(f"### {i}. {r.get('title') or '(sem título)'}")
                meta = []
                if r.get("authors"): meta.append(r["authors"][:200])
                if r.get("year"): meta.append(str(r["year"]))
                if r.get("journal"): meta.append(r["journal"])
                if meta: lines.append(f"_{' · '.join(meta)}_")
                lines.append("")
                lines.append(f"- **Score:** {score} · **Fonte:** {r.get('source','')} · **Decisão:** {r.get('decision','')}")
                if r.get("doi"):
                    lines.append(f"- **DOI:** [{r['doi']}](https://doi.org/{r['doi']})")
                if r.get("external_url"):
                    lines.append(f"- **Link:** {r['external_url']}")
                if r.get("summary_pt"):
                    lines.extend(["", r["summary_pt"]])
                lines.append("")
                lines.append("---")
                lines.append("")
            return Response(
                content="\n".join(lines),
                media_type="text/markdown; charset=utf-8",
                headers={"Content-Disposition": f'attachment; filename="{filename_base}.md"'},
            )

        # CSV (default)
        buf = io.StringIO()
        w = _csv.writer(buf)
        cols = ["rank", "score", "decision", "ai_decision", "user_decision",
                "source", "year", "title", "authors", "journal", "doi",
                "external_url", "summary_pt"]
        w.writerow(cols)
        for i, r in enumerate(rows, 1):
            w.writerow([
                i,
                r.get("quality_score") or "",
                r.get("decision") or "",
                r.get("ai_decision") or "",
                r.get("user_decision") or "",
                r.get("source") or "",
                r.get("year") or "",
                r.get("title") or "",
                r.get("authors") or "",
                r.get("journal") or "",
                r.get("doi") or "",
                r.get("external_url") or "",
                r.get("summary_pt") or "",
            ])
        return Response(
            content="﻿" + buf.getvalue(),  # BOM pra Excel reconhecer UTF-8
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename_base}.csv"'},
        )

    @app.post("/projetos/{project_id}/excluir")
    def project_delete(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        """Exclui projeto + todos seus artigos/análises (cascade)."""
        p = get_project_for_workspace(project_id, ws)
        with connect() as conn:
            conn.execute("DELETE FROM projects WHERE id=? AND workspace_id=?", (p.id, ws.id))
        return RedirectResponse("/projetos?msg=Projeto+exclu%C3%ADdo", status_code=303)

    @app.post("/projetos/{project_id}/atualizar")
    def project_update_meta(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        """Rota legada: target_articles é congelado depois da criação do
        projeto. A UI não expõe mais formulário de edição; esta rota fica
        como guarda de defesa em profundidade — qualquer POST direto é
        rejeitado, independentemente do status."""
        p = get_project_for_workspace(project_id, ws)
        return RedirectResponse(
            f"/projetos/{p.id}?error=Meta+do+Top+%C3%A9+definida+na+cria%C3%A7%C3%A3o+do+projeto+e+n%C3%A3o+pode+ser+alterada+depois",
            status_code=303,
        )

    @app.get("/projetos/{project_id}", response_class=HTMLResponse)
    def project_detail(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        p = get_project_for_workspace(project_id, ws)
        m = queries.get_project_dashboard(p.id)
        jobs = queries.get_jobs(p.id)
        top_n = queries.get_top_n(p.id, n=p.target_articles) if p.status == "done" else []
        eff_uid = _resolve_view_as_uid(request, user) or user.id
        p.is_favorite = queries.is_project_favorite(eff_uid, p.id)
        return render(request, "projects/detail.html", {
            "p": p, "m": m, "jobs": jobs, "top_n": top_n,
        })

    @app.get("/projetos/{project_id}/status")
    def project_status_json(
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        """Endpoint para polling do front (atualiza progress sem recarregar a página)."""
        p = get_project_for_workspace(project_id, ws)
        m = queries.get_project_dashboard(p.id)
        jobs = queries.get_jobs(p.id)
        return JSONResponse({
            "status": p.status, "error": p.error,
            "metrics": m, "jobs": jobs,
            "is_done": p.status == "done",
        })

    @app.get("/projetos/{project_id}/search-strings.json")
    def project_search_strings_json(
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        """Performance de cada substring de busca (multi-substring strategy).
        Renderizada pelo painel 'Performance das strings' com refresh 10s."""
        p = get_project_for_workspace(project_id, ws)
        return JSONResponse({"items": queries.list_search_string_stats(p.id)})

    @app.post("/projetos/{project_id}/iniciar")
    def project_run(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        p = get_project_for_workspace(project_id, ws)
        # ── Caso 1: discovery do PREVIEW ainda rodando ─────────────────────
        # Usuário clicou "Pré-visualizar critérios" e, sem esperar a geração,
        # agora clica "Iniciar pipeline". NÃO regeramos critérios: setamos a
        # flag `pending_full_pipeline` para que `run_discovery_only` dispare
        # `run_full_pipeline` automaticamente quando a discovery atual terminar.
        if p.status == "discovering":
            zombie, _reason = is_pipeline_zombie(p.id)
            if zombie:
                # Discovery está zumbi (server caiu durante o preview) — destrava
                # o status e segue como se fosse um run normal.
                reset_zombie_pipeline(p.id)
                p = get_project_for_workspace(project_id, ws)
            else:
                # Idempotente: se a flag já estava setada (clique duplo), só
                # avisa que o agendamento já existe — não cobra crédito de novo.
                if p.pending_full_pipeline:
                    return RedirectResponse(
                        f"/projetos/{p.id}?msg=Pipeline+completo+j%C3%A1+est%C3%A1+agendado.+Vamos+iniciar+a+busca+assim+que+os+crit%C3%A9rios+ficarem+prontos",
                        status_code=303,
                    )
                if not users.consume_credit(user.id):
                    return RedirectResponse(
                        f"/projetos/{p.id}?error=Sem+cr%C3%A9ditos",
                        status_code=303,
                    )
                projects_module.update(p.id, pending_full_pipeline=1)
                return RedirectResponse(
                    f"/projetos/{p.id}?msg=Pipeline+completo+agendado%21+Vamos+iniciar+a+busca+assim+que+os+crit%C3%A9rios+ficarem+prontos+(sem+regerar)",
                    status_code=303,
                )
        # ── Caso 2: pipeline completo já rodando ───────────────────────────
        if p.status in ("searching", "analyzing"):
            zombie, _reason = is_pipeline_zombie(p.id)
            if zombie:
                reset_zombie_pipeline(p.id)
                p = get_project_for_workspace(project_id, ws)
            else:
                return RedirectResponse(
                    f"/projetos/{p.id}?msg=Pipeline+j%C3%A1+est%C3%A1+rodando",
                    status_code=303,
                )
        # ── Caso 3: estado normal (draft / criteria_ready / failed / done) ─
        # SEMPRE cobra 1 crédito ao iniciar pipeline completo.
        # Pré-visualizar critérios (rota /projetos com auto_run='preview') é grátis.
        if not users.consume_credit(user.id):
            return RedirectResponse(
                f"/projetos/{p.id}?error=Sem+cr%C3%A9ditos",
                status_code=303,
            )
        # Marca status ANTES de agendar — assim o redirect já mostra banner ativo
        # e o botão "Iniciar" desaparece imediatamente, evitando re-cliques.
        projects_module.update(p.id, status="searching", error=None)
        schedule_pipeline(p.id)
        return RedirectResponse(
            f"/projetos/{p.id}?msg=Pipeline+iniciado%21+Acompanhe+o+progresso+abaixo",
            status_code=303,
        )

    @app.post("/projetos/{project_id}/parar")
    def project_stop(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        """Cancela o pipeline em andamento. Marca jobs running como failed e
        retorna o projeto pra `criteria_ready` (permite re-edição de critérios)."""
        p = get_project_for_workspace(project_id, ws)
        if p.status not in ("discovering", "searching", "analyzing"):
            return RedirectResponse(
                f"/projetos/{p.id}?msg=Pipeline+n%C3%A3o+est%C3%A1+rodando",
                status_code=303,
            )
        try:
            # Cancela de verdade o coroutine em background. Marca a flag de
            # cancelamento (workers checam antes de cada item) e tenta cancelar
            # o future. Sem isso, jobs continuariam rodando após o "Pare".
            from ..pipeline import request_cancel
            request_cancel(p.id)
            from ..storage import connect as _connect
            with _connect() as conn:
                conn.execute(
                    "UPDATE jobs SET status='failed', error='cancelado pelo usuário', "
                    "finished_at=datetime('now') "
                    "WHERE project_id=? AND status='running'",
                    (p.id,),
                )
            # Limpa pending_full_pipeline também — usuário cancelou, então
            # não queremos auto-disparar mesmo se a flag estava setada.
            projects_module.update(p.id, status="criteria_ready", error=None,
                                    pending_full_pipeline=0)
            return RedirectResponse(
                f"/projetos/{p.id}?msg=Pipeline+cancelado.+Voc%C3%AA+pode+editar+os+crit%C3%A9rios+e+reiniciar",
                status_code=303,
            )
        except Exception as e:
            return RedirectResponse(f"/projetos/{p.id}?error={e}", status_code=303)

    @app.post("/projetos/{project_id}/criterios")
    def project_save_criteria(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("edit_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        criteria_md: Annotated[str, Form()] = "",
        pubmed_query: Annotated[str, Form()] = "",
        scielo_query: Annotated[str, Form()] = "",
        scholar_query: Annotated[str, Form()] = "",
    ):
        p = get_project_for_workspace(project_id, ws)
        projects_module.update(
            p.id, criteria_md=criteria_md or None,
            search_strings={"pubmed": pubmed_query, "scielo": scielo_query, "scholar": scholar_query},
        )
        return RedirectResponse(f"/projetos/{p.id}", status_code=303)

    # ── Registros (escopados por projeto via querystring) ──────────────────

    @app.get("/projetos/{project_id}/registros", response_class=HTMLResponse)
    def records_list(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        q: str = "", decision: str = "", source: str = "", year: str = "",
        min_score: str = "", sort: str = "score_desc",
        page: int = Query(1, ge=1),
    ):
        p = get_project_for_workspace(project_id, ws)
        # min_score vem como string (form vazio = ""); converte de forma segura
        min_score_int: int | None = None
        if min_score and min_score.strip():
            try:
                min_score_int = int(min_score.strip())
            except ValueError:
                min_score_int = None
        result = queries.search_records(
            p.id, q=q, decision=decision, source=source, year=year,
            min_score=min_score_int, sort=sort, page=page, per_page=20,
        )
        eff_uid = _resolve_view_as_uid(request, user) or user.id
        fav_keys = queries.get_favorite_keys(eff_uid, p.id)
        for item in result.items:
            item["is_favorite"] = (item["source"], item["external_id"]) in fav_keys
        # Contagens para os atalhos de filtro
        m = queries.get_project_dashboard(p.id)
        return render(request, "records/list.html", {
            "p": p, "result": result, "m": m,
            "filters": {"q": q, "decision": decision, "source": source,
                       "year": year, "min_score": min_score, "sort": sort},
            "year_options": queries.get_year_options(p.id),
            "source_options": queries.get_source_options(p.id),
        })

    @app.get("/projetos/{project_id}/registros/{source}/{external_id}", response_class=HTMLResponse)
    def record_detail(
        request: Request,
        project_id: int, source: str, external_id: str,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        p = get_project_for_workspace(project_id, ws)
        record = queries.get_record_detail(p.id, source, external_id)
        if not record:
            raise HTTPException(404, "Registro não encontrado")
        eff_uid = _resolve_view_as_uid(request, user) or user.id
        record["is_favorite"] = queries.is_favorite(eff_uid, p.id, source, external_id)
        return render(request, "records/detail.html", {"p": p, "r": record})

    @app.post("/projetos/{project_id}/registros/{source}/{external_id}/favoritar")
    def toggle_favorite(
        request: Request,
        project_id: int, source: str, external_id: str,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        action: Annotated[str, Form()] = "toggle",
        next: Annotated[str, Form()] = "",
    ):
        """Marca/desmarca artigo como favorito do usuário logado."""
        p = get_project_for_workspace(project_id, ws)
        with connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM articles WHERE project_id=? AND source=? AND external_id=?",
                (p.id, source, external_id),
            ).fetchone()
            if not exists:
                raise HTTPException(404, "Registro não existe neste projeto.")
            already = conn.execute(
                "SELECT 1 FROM user_article_favorites WHERE user_id=? AND project_id=? AND source=? AND external_id=?",
                (user.id, p.id, source, external_id),
            ).fetchone()
            if action == "remove" or (action == "toggle" and already):
                remove_favorite(conn, user_id=user.id, project_id=p.id,
                                source=source, external_id=external_id)
                state = "off"
            else:
                add_favorite(conn, user_id=user.id, project_id=p.id,
                             source=source, external_id=external_id)
                state = "on"
        if request.headers.get("x-requested-with") == "fetch":
            return JSONResponse({"state": state})
        target = next or f"/projetos/{p.id}/registros/{source}/{external_id}"
        return RedirectResponse(target, status_code=303)

    # ── Favoritos (escopados por usuário, dentro do workspace) ─────────────

    @app.get("/favoritos", response_class=HTMLResponse)
    def favorites_index(
        request: Request,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        """Lista todos os projetos do workspace que tenham pelo menos 1 favorito do user."""
        eff_uid = _resolve_view_as_uid(request, user) or user.id
        projects = queries.list_favorite_projects(eff_uid, ws.id)
        return render(request, "favorites/index.html", {"projects": projects})

    @app.get("/favoritos/{project_id}", response_class=HTMLResponse)
    def favorites_in_project(
        request: Request,
        project_id: int,
        user: Annotated[User, Depends(require_perm("view_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
    ):
        p = get_project_for_workspace(project_id, ws)
        eff_uid = _resolve_view_as_uid(request, user) or user.id
        favorites = queries.list_favorites_in_project(eff_uid, p.id)
        return render(request, "favorites/project.html",
                      {"p": p, "favorites": favorites})

    @app.post("/projetos/{project_id}/registros/{source}/{external_id}/decisao")
    def set_decision(
        request: Request,
        project_id: int, source: str, external_id: str,
        user: Annotated[User, Depends(require_perm("edit_records"))],
        ws: Annotated[Workspace, Depends(get_current_workspace)],
        decision: Annotated[str, Form()],
        note: Annotated[str, Form()] = "",
        next: Annotated[str, Form()] = "",
    ):
        p = get_project_for_workspace(project_id, ws)
        with connect() as conn:
            exists = conn.execute(
                "SELECT 1 FROM articles WHERE project_id=? AND source=? AND external_id=?",
                (p.id, source, external_id),
            ).fetchone()
            if not exists:
                raise HTTPException(404, "Registro não existe neste projeto.")
            if decision == "clear":
                clear_user_decision(conn, p.id, source, external_id)
            else:
                upsert_user_decision(conn, p.id, source, external_id, decision,
                                     note=note or None, user_id=user.id)
        target = next or f"/projetos/{p.id}/registros/{source}/{external_id}"
        return RedirectResponse(target, status_code=303)

    # ── Usuários (Admin only) ───────────────────────────────────────────────

    @app.get("/usuarios", response_class=HTMLResponse)
    def users_list(request: Request,
                   user: Annotated[User, Depends(require_perm("manage_users"))]):
        return render(request, "users/list.html", {
            "users_list": users.list_all(),
            "role_options": [("admin", "Administrador"), ("operacional", "Operacional"),
                            ("visualizador", "Visualizador")],
        })

    @app.post("/usuarios")
    def users_create(request: Request,
                     user: Annotated[User, Depends(require_perm("manage_users"))],
                     email: Annotated[str, Form()], name: Annotated[str, Form()],
                     role: Annotated[str, Form()],
                     password: Annotated[str, Form()] = ""):
        try:
            pwd = password or users.generate_password()
            users.create_user(email=email, password=pwd, name=name, role=role)  # type: ignore
            msg = f"Usuário criado. Senha: {pwd}" if not password else "Usuário criado."
            return RedirectResponse(f"/usuarios?msg={msg}", status_code=303)
        except Exception as e:
            return RedirectResponse(f"/usuarios?error={e}", status_code=303)

    @app.post("/usuarios/{uid}/atualizar")
    def users_update(request: Request, uid: int,
                     user: Annotated[User, Depends(require_perm("manage_users"))],
                     name: Annotated[str, Form()] = "", role: Annotated[str, Form()] = "",
                     status: Annotated[str, Form()] = "", password: Annotated[str, Form()] = "",
                     credits: Annotated[str, Form()] = ""):
        try:
            credits_int: int | None = None
            if credits.strip():
                try:
                    credits_int = max(0, int(credits))
                except ValueError:
                    raise ValueError(f"Crédito inválido: {credits!r}")
            users.update_user(uid, name=name or None, role=role or None,  # type: ignore
                              status=status or None, password=password or None,
                              credits=credits_int)
            return RedirectResponse("/usuarios?msg=Atualizado", status_code=303)
        except Exception as e:
            return RedirectResponse(f"/usuarios?error={e}", status_code=303)

    @app.post("/usuarios/{uid}/creditos")
    def users_add_credits(request: Request, uid: int,
                          user: Annotated[User, Depends(require_perm("manage_credits"))],
                          delta: Annotated[str, Form()]):
        """Soma `delta` (positivo ou negativo) ao saldo de créditos do usuário."""
        try:
            d = int(delta)
            new_user = users.add_credits(uid, d)
            sign = "+" if d >= 0 else ""
            return RedirectResponse(
                f"/usuarios?msg=Cr%C3%A9ditos+atualizados+({sign}{d}).+Saldo+atual:+{new_user.credits}",
                status_code=303,
            )
        except Exception as e:
            return RedirectResponse(f"/usuarios?error={e}", status_code=303)

    @app.post("/usuarios/{uid}/excluir")
    def users_delete(request: Request, uid: int,
                     user: Annotated[User, Depends(require_perm("manage_users"))]):
        """Apaga usuário e todos os dados associados. Admin não pode se auto-deletar."""
        if uid == user.id:
            return RedirectResponse(
                "/usuarios?error=Voc%C3%AA+n%C3%A3o+pode+excluir+a+pr%C3%B3pria+conta",
                status_code=303,
            )
        try:
            users.delete_user(uid)
            return RedirectResponse("/usuarios?msg=Usu%C3%A1rio+exclu%C3%ADdo", status_code=303)
        except Exception as e:
            return RedirectResponse(f"/usuarios?error={e}", status_code=303)

    # ── Workspace settings ──────────────────────────────────────────────────

    @app.get("/workspace", response_class=HTMLResponse)
    def workspace_page(request: Request,
                       user: Annotated[User, Depends(require_perm("view_records"))],
                       ws: Annotated[Workspace, Depends(get_current_workspace)]):
        # Cada user tem workspace 1:1 (via get_current_workspace), então qualquer
        # logado pode ver as configurações DO PRÓPRIO workspace. Save é
        # restrito a roles que editam (edit_records: admin/operacional).
        return render(request, "workspace_settings.html", {})

    @app.post("/workspace")
    def workspace_update(request: Request,
                         user: Annotated[User, Depends(require_perm("edit_records"))],
                         ws: Annotated[Workspace, Depends(get_current_workspace)],
                         name: Annotated[str, Form()] = "",
                         rayyan_email: Annotated[str, Form()] = "",
                         rayyan_password: Annotated[str, Form()] = "",
                         rayyan_review_id: Annotated[str, Form()] = "",
                         openrouter_model: Annotated[str, Form()] = "",
                         openrouter_api_key: Annotated[str, Form()] = ""):
        try:
            workspaces.update_settings(
                ws.id, name=name, rayyan_email=rayyan_email,
                rayyan_password=rayyan_password, rayyan_review_id=rayyan_review_id,
                openrouter_model=openrouter_model,
                openrouter_api_key=openrouter_api_key,
            )
            return RedirectResponse("/workspace?msg=Configurações salvas", status_code=303)
        except Exception as e:
            return RedirectResponse(f"/workspace?error={e}", status_code=303)

    # ── Configurações globais ───────────────────────────────────────────────

    @app.get("/configuracoes", response_class=HTMLResponse)
    def settings(request: Request,
                 user: Annotated[User, Depends(require_user)],
                 ws: Annotated[Workspace, Depends(get_current_workspace)]):
        cfg: Config = request.app.state.cfg
        usage_summary = None
        if user.can("view_api_usage"):
            from datetime import datetime, timedelta
            since = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
            usage_summary = queries.llm_usage_totals(since=since)
        return render(request, "settings.html", {
            "global_cfg": {"openrouter_model_default": cfg.openrouter_model},
            "usage_summary": usage_summary,
        })

    # ── Uso da API (Admin only) ─────────────────────────────────────────────

    @app.get("/configuracoes/uso-api", response_class=HTMLResponse)
    def api_usage_list(
        request: Request,
        user: Annotated[User, Depends(require_perm("view_api_usage"))],
        step: str = "", model: str = "",
        user_id: str = "", project_id: str = "",
        since: str = "",
        page: int = Query(1, ge=1),
    ):
        per_page = 50
        offset = (page - 1) * per_page

        def _to_int(s: str) -> int | None:
            s = (s or "").strip()
            if not s:
                return None
            try:
                return int(s)
            except ValueError:
                return None

        filt = {
            "step": step or None,
            "model": model or None,
            "user_id": _to_int(user_id),
            "project_id": _to_int(project_id),
            "since": since or None,
        }
        items = queries.list_llm_usage(**filt, limit=per_page, offset=offset)
        totals = queries.llm_usage_totals(**filt)
        options = queries.llm_usage_filter_options()
        avg_by_review_type = queries.llm_usage_avg_per_project_by_review_type()
        duration_by_review_type = queries.pipeline_duration_avg_by_review_type()
        # Total acumulado por projeto inteiro (discovery + analyze + DC + rotate),
        # respeitando os mesmos filtros do user_id/project_id/since.
        by_project = queries.llm_usage_by_project(
            user_id=_to_int(user_id),
            since=since or None,
        )
        return render(request, "api_usage/list.html", {
            "items": items,
            "totals": totals,
            "options": options,
            "avg_by_review_type": avg_by_review_type,
            "duration_by_review_type": duration_by_review_type,
            "by_project": by_project,
            "filters": {"step": step, "model": model, "user_id": user_id,
                        "project_id": project_id, "since": since},
            "page": page,
            "has_more": len(items) == per_page,
        })

    @app.get("/configuracoes/uso-api/{usage_id}", response_class=HTMLResponse)
    def api_usage_detail(
        request: Request, usage_id: int,
        user: Annotated[User, Depends(require_perm("view_api_usage"))],
    ):
        item = queries.get_llm_usage(usage_id)
        if not item:
            raise HTTPException(404, "Chamada não encontrada")
        return render(request, "api_usage/detail.html", {"r": item})

    # ── Admin: view-as (entrar/sair) + Relatórios ───────────────────────────

    @app.post("/admin/view-as/sair")
    def admin_exit_view_as(
        request: Request,
        user: Annotated[User, Depends(require_user)],
        next: Annotated[str, Form()] = "/usuarios",
    ):
        resp = RedirectResponse(next or "/usuarios", status_code=303)
        resp.delete_cookie(VIEW_AS_COOKIE)
        return resp

    @app.post("/admin/view-as/{target_uid}")
    def admin_enter_view_as(
        request: Request, target_uid: int,
        user: Annotated[User, Depends(require_perm("manage_users"))],
        next: Annotated[str, Form()] = "/dashboard",
    ):
        """Admin entra no modo de visualização do workspace de outro usuário."""
        target = users.get_by_id(target_uid)
        if not target:
            return RedirectResponse("/usuarios?error=Usu%C3%A1rio+n%C3%A3o+encontrado",
                                    status_code=303)
        if target_uid == user.id:
            return RedirectResponse("/dashboard", status_code=303)
        resp = RedirectResponse(next or "/dashboard", status_code=303)
        resp.set_cookie(VIEW_AS_COOKIE, str(target_uid),
                        httponly=True, samesite="lax", max_age=60 * 60 * 8)
        return resp

    @app.get("/admin/relatorios", response_class=HTMLResponse)
    def admin_reports(
        request: Request,
        user: Annotated[User, Depends(require_perm("manage_users"))],
    ):
        report = queries.get_admin_reports()
        return render(request, "admin/reports.html", {"r": report})
