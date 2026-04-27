"""CLI principal: vicky scrape | analyze | double-check | report | run-all | stats."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.table import Table

from .config import Config, ENV_PATH
from .llm import analyze_with_raw, double_check_with_raw, make_client
from .report import generate as generate_report
from .scraper import RayyanCreds, scrape as scrape_async
from .storage import (
    articles_without_analysis,
    connect,
    excluded_without_double_check,
    insert_analysis,
    insert_double_check,
    stats as db_stats,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


def _load_config() -> Config:
    try:
        return Config.load()
    except RuntimeError as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)


def _resolve_workspace(user_email: str | None = None, workspace_id: int | None = None):
    """Resolve workspace a partir de --user EMAIL ou --workspace ID. Default = .env user."""
    from .web import users as web_users, workspaces as web_workspaces
    cfg = _load_config()

    if workspace_id is not None:
        ws = web_workspaces.get_by_id(workspace_id)
        if not ws:
            console.print(f"[red]✗[/red] Workspace #{workspace_id} não encontrado.")
            raise typer.Exit(1)
        return ws

    email = (user_email or cfg.rayyan_email).lower().strip()
    with connect() as conn:
        row = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
    if not row:
        console.print(
            f"[red]✗[/red] Usuário {email} não cadastrado.\n"
            f"  Crie com: [cyan]vicky create-user --email {email}[/cyan]"
        )
        raise typer.Exit(1)
    return web_workspaces.get_or_create_for_user(row["id"])


@app.command()
def scrape(
    user: str | None = typer.Option(None, help="E-mail do usuário (workspace destino)."),
    workspace: int | None = typer.Option(None, help="ID do workspace destino."),
    headless: bool = typer.Option(True, help="Rodar Chromium sem janela (use --no-headless para ver)."),
    limit: int | None = typer.Option(None, help="Parar após N artigos (smoke test)."),
    debug: bool = typer.Option(False, help="Salvar respostas JSON em ./debug/ para inspeção."),
) -> None:
    """1. Loga no Rayyan e raspa os artigos para o workspace selecionado."""
    ws = _resolve_workspace(user, workspace)
    if not ws.has_rayyan_credentials:
        console.print(
            f"[red]✗[/red] Workspace #{ws.id} ({ws.name}) sem credenciais Rayyan.\n"
            f"  Configure no front-end em [cyan]/workspace[/cyan] ou via DB."
        )
        raise typer.Exit(1)
    creds = RayyanCreds(email=ws.rayyan_email, password=ws.rayyan_password,
                        review_id=ws.rayyan_review_id)
    console.print(f"[cyan]→[/cyan] Workspace #{ws.id} · Logando como [bold]{creds.email}[/bold]…")
    stats = asyncio.run(scrape_async(creds, ws.id, headless=headless, limit=limit, debug=debug))
    console.print(
        f"[green]✓[/green] Raspagem concluída: {stats.persisted} artigos persistidos "
        f"({stats.seen} brutos vistos) no workspace #{ws.id}."
    )


@app.command()
def analyze(
    user: str | None = typer.Option(None, help="E-mail do usuário (workspace destino)."),
    workspace: int | None = typer.Option(None, help="ID do workspace destino."),
    limit: int | None = typer.Option(None, help="Processar no máximo N artigos."),
    parallel: int = typer.Option(5, help="Chamadas simultâneas à API."),
) -> None:
    """2. Avalia cada artigo (1ª passada) aplicando critérios PICO."""
    cfg = _load_config()
    ws = _resolve_workspace(user, workspace)
    model = ws.openrouter_model or cfg.openrouter_model
    client = make_client(cfg)  # API key continua global no .env

    with connect() as conn:
        pending = articles_without_analysis(conn, ws.id)
    if limit:
        pending = pending[:limit]
    if not pending:
        console.print(f"[yellow]Nada a analisar no workspace #{ws.id}.[/yellow]")
        return

    console.print(f"[cyan]→[/cyan] Workspace #{ws.id} · Analisando {len(pending)} artigos com [bold]{model}[/bold]…")

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TextColumn("{task.completed}/{task.total}"), TimeRemainingColumn(),
        console=console,
    ) as bar:
        task = bar.add_task("Analyze", total=len(pending))
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = {pool.submit(analyze_with_raw, client, model, art): art for art in pending}
            for fut in as_completed(futures):
                art = futures[fut]
                try:
                    analysis, raw, _usage = fut.result()
                    with connect() as conn:
                        insert_analysis(conn, ws.id, analysis, raw)
                except Exception as e:
                    console.print(f"[red]✗[/red] {art.rayyan_id}: {e}")
                bar.advance(task)
    console.print("[green]✓[/green] Análise concluída.")


@app.command(name="double-check")
def double_check(
    user: str | None = typer.Option(None, help="E-mail do usuário (workspace destino)."),
    workspace: int | None = typer.Option(None, help="ID do workspace destino."),
    parallel: int = typer.Option(5, help="Chamadas simultâneas à API."),
) -> None:
    """3. Auditoria: revisa cada decisão de exclusão (2ª passada)."""
    cfg = _load_config()
    ws = _resolve_workspace(user, workspace)
    model = ws.openrouter_model or cfg.openrouter_model
    client = make_client(cfg)

    with connect() as conn:
        pending = excluded_without_double_check(conn, ws.id)
    if not pending:
        console.print(f"[yellow]Nada a auditar no workspace #{ws.id}.[/yellow]")
        return

    console.print(f"[cyan]→[/cyan] Workspace #{ws.id} · Auditando {len(pending)} exclusões…")

    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        BarColumn(), TextColumn("{task.completed}/{task.total}"), TimeRemainingColumn(),
        console=console,
    ) as bar:
        task = bar.add_task("Double-check", total=len(pending))
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = {
                pool.submit(double_check_with_raw, client, model, art, an): (art, an)
                for art, an in pending
            }
            for fut in as_completed(futures):
                art, _ = futures[fut]
                try:
                    dc, raw, _usage = fut.result()
                    with connect() as conn:
                        insert_double_check(conn, ws.id, dc, raw)
                except Exception as e:
                    console.print(f"[red]✗[/red] {art.rayyan_id}: {e}")
                bar.advance(task)
    console.print("[green]✓[/green] Double-check concluído.")


@app.command()
def report(
    user: str | None = typer.Option(None, help="E-mail do usuário (workspace)."),
    workspace: int | None = typer.Option(None, help="ID do workspace."),
    output: Path = typer.Option(Path("relatorio.md"), help="Arquivo de saída."),
) -> None:
    """4. Gera o relatório Markdown final do workspace."""
    ws = _resolve_workspace(user, workspace)
    path = generate_report(ws.id, output)
    console.print(f"[green]✓[/green] Relatório gerado em [bold]{path}[/bold] (workspace #{ws.id})")


@app.command(name="run-all")
def run_all(
    user: str | None = typer.Option(None, help="E-mail do usuário (workspace)."),
    workspace: int | None = typer.Option(None, help="ID do workspace."),
    headless: bool = typer.Option(True, help="Headless no scrape."),
    parallel: int = typer.Option(5, help="Paralelismo nas chamadas à API."),
) -> None:
    """Roda scrape → analyze → double-check → report em sequência (mesmo workspace)."""
    scrape(user=user, workspace=workspace, headless=headless, limit=None, debug=False)
    analyze(user=user, workspace=workspace, limit=None, parallel=parallel)
    double_check(user=user, workspace=workspace, parallel=parallel)
    report(user=user, workspace=workspace, output=Path("relatorio.md"))


@app.command()
def stats(
    user: str | None = typer.Option(None, help="E-mail do usuário (workspace)."),
    workspace: int | None = typer.Option(None, help="ID do workspace."),
) -> None:
    """Mostra estatísticas do workspace."""
    ws = _resolve_workspace(user, workspace)
    with connect() as conn:
        s = db_stats(conn, ws.id)
    table = Table(title=f"Vicky — Workspace #{ws.id} ({ws.name})", show_header=False)
    table.add_column("Métrica", style="cyan")
    table.add_column("Valor", justify="right", style="bold")
    for k, v in s.items():
        table.add_row(k, str(v))
    console.print(table)


@app.command(name="list-workspaces")
def list_workspaces() -> None:
    """Lista todos os workspaces e seus owners."""
    _load_config()
    from .web import workspaces as web_workspaces
    table = Table(title="Workspaces", show_header=True, header_style="bold cyan")
    table.add_column("ID")
    table.add_column("Nome")
    table.add_column("Owner")
    table.add_column("Rayyan?")
    table.add_column("Modelo")
    for ws in web_workspaces.list_all():
        with connect() as conn:
            owner = conn.execute("SELECT email FROM users WHERE id=?", (ws.owner_user_id,)).fetchone()
            n_articles = conn.execute("SELECT COUNT(*) FROM articles WHERE workspace_id=?", (ws.id,)).fetchone()[0]
        table.add_row(
            str(ws.id), ws.name, owner["email"] if owner else "?",
            "✓" if ws.has_rayyan_credentials else "—",
            f"{ws.openrouter_model} ({n_articles} arts)",
        )
    console.print(table)


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Host para escutar (use 0.0.0.0 para LAN)."),
    port: int = typer.Option(8000, help="Porta HTTP."),
    reload: bool = typer.Option(False, help="Auto-reload no desenvolvimento."),
) -> None:
    """Sobe o frontend web (FastAPI + Jinja2)."""
    import uvicorn
    from .web import users as web_users

    cfg = _load_config()
    # Garante schema atualizado
    with connect() as conn:
        pass

    if web_users.count() == 0:
        console.print(
            "\n[yellow]⚠ Nenhum usuário cadastrado.[/yellow]\n"
            "Crie o admin antes de subir o servidor:\n"
            f"  [cyan].venv/bin/vicky create-user --email {cfg.rayyan_email} --role admin[/cyan]\n"
        )
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Subindo Vicky em [bold]http://{host}:{port}[/bold]")
    console.print(f"  Modelo: {cfg.openrouter_model}  ·  Review: {cfg.rayyan_review_id}")
    uvicorn.run("vicky.web.app:create_app", host=host, port=port, reload=reload, factory=True)


@app.command(name="create-user")
def create_user_cmd(
    email: str = typer.Option(..., prompt=True, help="E-mail do usuário."),
    name: str = typer.Option(None, help="Nome (default: parte antes do @)."),
    role: str = typer.Option("admin", help="admin | operacional | visualizador"),
    password: str | None = typer.Option(
        None, help="Senha (se omitir, será gerada uma aleatória)."
    ),
) -> None:
    """Cria um usuário do front-end web."""
    from .web import users as web_users

    _load_config()
    with connect() as conn:
        pass  # garante schema

    if not name:
        name = email.split("@")[0].replace(".", " ").title()
    pwd = password or web_users.generate_password()
    try:
        u = web_users.create_user(email=email, password=pwd, name=name, role=role)  # type: ignore
    except Exception as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    console.print(f"[green]✓[/green] Usuário criado: [bold]{u.email}[/bold] ({u.role_label})")
    if not password:
        console.print(f"  [yellow]Senha gerada:[/yellow] [bold cyan]{pwd}[/bold cyan]")
        console.print("  [dim]Anote essa senha — ela não será mostrada novamente.[/dim]")


@app.command()
def doctor() -> None:
    """Diagnóstico: confere config, conexão com OpenRouter e banco."""
    console.print(f"[cyan]Config file:[/cyan] {ENV_PATH} ({'✓ existe' if ENV_PATH.exists() else '✗ ausente'})")
    cfg = _load_config()
    console.print(f"[cyan]Modelo:[/cyan] {cfg.openrouter_model}")
    console.print(f"[cyan]Review ID:[/cyan] {cfg.rayyan_review_id}")
    client = make_client(cfg)
    try:
        resp = client.chat.completions.create(
            model=cfg.openrouter_model,
            messages=[{"role": "user", "content": "Responda apenas com a palavra OK."}],
            max_tokens=5,
        )
        console.print(f"[green]✓[/green] OpenRouter respondeu: {resp.choices[0].message.content!r}")
    except Exception as e:
        console.print(f"[red]✗[/red] OpenRouter falhou: {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
