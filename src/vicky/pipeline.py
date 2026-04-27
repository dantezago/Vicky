"""Pipeline Runner — orquestra discovery → search → analyze → double-check → verify.

Cada etapa é registrada como um `job` no SQLite com status (queued/running/success/failed).
Múltiplos projetos rodam em paralelo via asyncio.create_task.
"""

from __future__ import annotations

import asyncio
import json
import traceback

from .config import Config
from .discovery import (
    rotate_search_strings,
    run_discovery_with_fallback,
    verify_discovery,
)
from .llm import analyze_with_raw, double_check_with_raw, make_client
from .sources import REGISTRY
from .openrouter_cost import fetch_real_cost
from .storage import (
    articles_without_analysis,
    connect,
    create_job,
    excluded_without_double_check,
    insert_analysis,
    insert_double_check,
    project_stats,
    record_llm_call,
    update_job,
    update_llm_usage_real_cost,
    upsert_article,
)


async def _reconcile_real_cost(usage_id: int, generation_id: str, api_key: str) -> None:
    """Busca o gasto REAL via OpenRouter `/api/v1/generation` e sobrescreve a row de
    llm_usage. Roda em background — qualquer falha é silenciosa (mantém o estimado).
    """
    if not usage_id or not generation_id or not api_key:
        return
    try:
        data = await fetch_real_cost(generation_id, api_key)
        if not data:
            return
        cost = float(data.get("total_cost") or 0.0)
        npt = data.get("native_tokens_prompt")
        nct = data.get("native_tokens_completion")
        update_llm_usage_real_cost(
            usage_id, cost_usd=cost,
            prompt_tokens=int(npt) if npt is not None else None,
            completion_tokens=int(nct) if nct is not None else None,
        )
    except Exception as e:
        print(f"  ⚠ reconcile_real_cost falhou: {e}")


def _schedule_reconcile(usage_id: int | None, generation_id: str, api_key: str) -> None:
    """Agenda reconciliação no event loop atual. Silencioso se já não há loop."""
    if not usage_id or not generation_id:
        return
    try:
        asyncio.get_event_loop().create_task(
            _reconcile_real_cost(usage_id, generation_id, api_key)
        )
    except RuntimeError:
        pass
from .web import projects as projects_module
from .web import workspaces as workspaces_module


# ─── Public API ────────────────────────────────────────────────────────────


# ─── Limites do pipeline iterativo ────────────────────────────────────────

# ── Regras IMUTÁVEIS de quantos artigos analisar ──
# MIN: piso obrigatório de análises antes de poder parar (mesmo com Top N atingido)
# MAX: teto absoluto de análises por pipeline
MIN_REQUIRED_ARTICLES = 500       # Sistemática: ≥500 ANALISADOS (regra imutável)
MIN_REQUIRED_NARRATIVE = 200      # Narrativa: ≥200 ANALISADOS (regra imutável)
MAX_TOTAL_ARTICLES = 1300         # Cap DURO: nunca analisar mais que isso

ANALYZE_CONCURRENCY = 20          # Workers paralelos do analyzer (sweet spot p/ rate limit)
DOUBLE_CHECK_CONCURRENCY = 20     # Workers paralelos do double-check
DOUBLE_CHECK_SCORE_FLOOR = 40     # Não auditar exclusões com score < 40 (claramente ruins)
DOUBLE_CHECK_SCORE_CEILING = 70   # Não auditar exclusões com score ≥ 70 (já passariam)
DOUBLE_CHECK_MAX_AUDITS = 20      # Cap absoluto: só os 20 mais limítrofes

# Stagnation = N iterações consecutivas que NÃO trouxeram nenhum artigo novo.
# Só pode encerrar o pipeline ANTES de min_required quando a fonte está
# evidentemente esgotada. Limite alto para garantir que tentamos tudo.
STAGNATION_LIMIT = 25
HARD_ITERATION_CAP = 200          # Safety: cap absoluto do loop principal (gera margem
                                  # confortável: ~13 chunks análise + 70 expansões + extras)
MAX_CONSECUTIVE_ANALYZE_FAILURES = 3  # Após N chunks 100%-falhos, aborta
ANALYZE_CHUNK_SIZE = 100          # Tamanho do lote de análise
MAX_TERM_ROTATIONS = 20           # Rotações de termos via LLM antes de expansão genérica
MAX_EXPANSION_ITERATIONS = 50     # Iterações de expansão mecânica

# ── Reroll de discovery (regra de proporção incluídos/analisados) ──
# A cada N analisados conferimos se a proporção real de incluídos está
# alinhada com a esperada (target/MAX_TOTAL). Se não estiver, os termos de
# busca não estão acertando o universo certo — voltamos pro agente de
# descoberta gerar ÂNGULOS COMPLETAMENTE NOVOS.
RATIO_CHECK_INTERVAL = 100        # Confere proporção a cada N analisados
RATIO_CHECK_MIN_BASE = 200        # Não dispara reroll antes de 200 analisados (base estatística)
RATIO_CHECK_TOLERANCE = 0.5       # Só reroll se proporção < 50% da esperada (evita false positive)
DISCOVERY_REROLL_LIMIT = 5        # Máximo de rerolls antes de aceitar esgotamento

# ── Expansão de janela temporal ──
# Última alavanca antes de aceitar "fonte esgotada": amplia anos da busca.
# A janela inicial do usuário pode ser apertada demais para o tema (ex: 5
# anos num campo onde só há literatura mais antiga). Antes de declarar
# esgotamento, escala progressivamente: 2× → 4× → sem limite (100 anos).
YEARS_WINDOW_EXPANSION_LADDER = [2, 4, 100]  # multiplicadores aplicados sobre a janela ORIGINAL
YEARS_WINDOW_EXPANSION_LIMIT = len(YEARS_WINDOW_EXPANSION_LADDER)

# ── Circuit breaker para operações inúteis ──
# Quando rotate_terms/expand_search/reroll geram strings que retornam 0 artigos
# em TODAS as fontes, queimar 20 rotações de uma só não ajuda. Após N operações
# consecutivas com 0 artigos novos, pula direto pra alavanca seguinte (janela
# temporal). Evita o loop infinito que vimos no projeto #68.
USELESS_OPERATIONS_LIMIT = 3


def _min_required_for(review_type: str | None) -> int:
    """Mínimo IMUTÁVEL de artigos ANALISADOS antes do pipeline poder parar.

    Revisão sistemática: 500. Revisão narrativa: 200.
    Mesmo que o Top N seja atingido antes do mínimo, o pipeline continua
    coletando+analisando até bater o mínimo (ou cap MAX, ou esgotamento real).
    """
    return MIN_REQUIRED_NARRATIVE if review_type == "narrative_review" else MIN_REQUIRED_ARTICLES


class LLMUnavailableError(RuntimeError):
    """Erro fatal: LLM não está respondendo (sem créditos, chave inválida, rede caiu).

    Quando levantado, o pipeline aborta IMEDIATAMENTE e o projeto é marcado como failed
    com mensagem útil pro usuário — sem ficar em loop infinito de retries.
    """


def _is_llm_fatal_error(err: Exception) -> bool:
    """Detecta erros de LLM que NÃO valem retry (auth, créditos, chave inválida)."""
    msg = str(err).lower()
    fatal_signatures = [
        "key limit exceeded", "insufficient_quota", "invalid_api_key",
        "authentication", "permission_denied", "401", "403",
        "no credits", "credit balance",
    ]
    return any(sig in msg for sig in fatal_signatures)


async def run_full_pipeline(project_id: int) -> None:
    """Pipeline iterativo com regras IMUTÁVEIS de tamanho:

    Regras (não negociáveis):
      1. analisar SEMPRE ≥ min_required (sistemática=500, narrativa=200)
         — mesmo se Top N for atingido antes
      2. analisar NUNCA > MAX_TOTAL_ARTICLES (=1300)
      3. depois do mínimo: parar SE Top N atingido OU cap MAX atingido OU
         coleta genuinamente esgotada (STAGNATION_LIMIT iters sem novidade)

    Estrutura:
      A. Discovery (critérios + search strings)
      B. Busca inicial paralela em todas as sources
      C. LOOP UNIFICADO (intercala análise + coleta) até cumprir regras acima
      D. Double-check + Finalize + Verify
    """
    cfg = Config.load()
    p = projects_module.get(project_id)
    if not p:
        return
    ws = workspaces_module.get_by_id(p.workspace_id)
    if not ws:
        _fail_project(project_id, "Workspace não encontrado")
        return
    model = ws.openrouter_model or cfg.openrouter_model

    _close_orphan_jobs(project_id, "Job órfão de execução anterior")

    try:
        # ══════ FASE A: Discovery ══════
        if not p.criteria_md or not p.search_strings.get("pubmed"):
            await _step_discovery(project_id, cfg, model)
            p = projects_module.get(project_id)

        projects_module.update(project_id, status="searching", error=None)
        workspace_id = p.workspace_id
        target = p.target_articles
        min_required = _min_required_for(p.review_type)

        # ══════ FASE B: Busca inicial paralela em todas as sources ══════
        search_tasks = [
            _step_search(project_id, workspace_id, source,
                         p.search_strings.get(source, p.topic))
            for source in p.sources if source in REGISTRY
        ]
        if search_tasks:
            await asyncio.gather(*search_tasks, return_exceptions=True)
        await _step_dedup(project_id)

        # ══════ FASE C: LOOP UNIFICADO (coleta + análise intercaladas) ══════
        # Invariantes verificados a cada iteração:
        #   STOP se: analyzed ≥ min E included ≥ target  (sucesso pleno)
        #   STOP se: analyzed ≥ MAX_TOTAL                (cap duro)
        #   STOP se: stagnation ≥ STAGNATION_LIMIT       (fonte esgotada)
        #   STOP se: iteration ≥ HARD_ITERATION_CAP      (safety)
        # Caso contrário: 1) analisa pendentes, 2) se ainda precisa mais e não
        # tem pendentes, expande coleta (rotate_terms ou expand_search).
        iteration = 0
        stagnation = 0
        prev_total = _project_counts(project_id)["total"]
        consecutive_analyze_failures = 0
        # ── Restauração de contadores via DB (sobrevive a restart do servidor) ──
        # Antes esses contadores existiam só em memória — quando o uvicorn reiniciava
        # e resume_interrupted_pipelines retomava, eles voltavam a 0 e o orçamento
        # de rotações era queimado várias vezes (ex: #67 acumulou 221 rotações).
        # Agora reconstruímos a partir dos jobs já gravados.
        rotations_done, discovery_rerolls_done, expansion_iter, years_window_expansions_done = (
            _restore_pipeline_counters(project_id)
        )
        if rotations_done or discovery_rerolls_done or expansion_iter or years_window_expansions_done:
            print(f"  ↻ Contadores restaurados do DB: "
                  f"rot={rotations_done}/{MAX_TERM_ROTATIONS}, "
                  f"reroll={discovery_rerolls_done}/{DISCOVERY_REROLL_LIMIT}, "
                  f"exp={expansion_iter}/{MAX_EXPANSION_ITERATIONS}, "
                  f"yw={years_window_expansions_done}/{YEARS_WINDOW_EXPANSION_LIMIT}")
        last_ratio_check_at = _project_counts(project_id)["analyzed"]  # não dispara reroll já analisados
        expected_inclusion_ratio = target / MAX_TOTAL_ARTICLES
        # Expansão de janela temporal: alavanca antes de aceitar esgotamento.
        # Guardamos a janela original do usuário pra calcular multiplicadores.
        original_years_window = p.years_window
        # Circuit breaker: conta operações de busca consecutivas que retornaram
        # 0 artigos novos. Após USELESS_OPERATIONS_LIMIT, força salto pra próxima
        # alavanca em vez de queimar todo orçamento de rotações.
        useless_streak = 0

        while True:
            iteration += 1
            stats = _project_counts(project_id)

            # ── Condições de parada (em ordem de precedência) ──
            # 1) Cap absoluto de análises (regra imutável)
            if stats["analyzed"] >= MAX_TOTAL_ARTICLES:
                print(f"  ⊘ cap MAX_TOTAL_ARTICLES={MAX_TOTAL_ARTICLES} atingido — para")
                break
            # 2) Sucesso pleno: mínimo + target ambos atingidos
            if stats["analyzed"] >= min_required and stats["included"] >= target:
                print(f"  ✓ sucesso: analyzed={stats['analyzed']} ≥ {min_required} "
                      f"E included={stats['included']} ≥ {target}")
                break
            # 3) Esgotamento real: muitas iters sem novidade. ANTES de aceitar
            #    esgotamento, se ainda temos orçamento de reroll de discovery
            #    e não atingimos target, força um reroll (ângulos novos) e
            #    reseta stagnation. Só aceita esgotamento depois de esgotar
            #    todos os rerolls.
            if stagnation >= STAGNATION_LIMIT:
                # Alavanca 1: ainda temos rerolls de discovery? Tenta antes de aceitar.
                # Mas: se circuit breaker acionou, pula direto pra alavanca 2.
                if (discovery_rerolls_done < DISCOVERY_REROLL_LIMIT
                        and useless_streak < USELESS_OPERATIONS_LIMIT
                        and stats["included"] < target
                        and stats["analyzed"] < MAX_TOTAL_ARTICLES):
                    discovery_rerolls_done += 1
                    pre_reroll_total = _project_counts(project_id)["total"]
                    print(f"  ↻ Stagnation no limite — Reroll #{discovery_rerolls_done}"
                          f"/{DISCOVERY_REROLL_LIMIT} antes de aceitar esgotamento")
                    try:
                        await _step_rotate_terms(
                            project_id, cfg, model,
                            1000 + discovery_rerolls_done * 100,
                        )
                        p = projects_module.get(project_id)
                        post_reroll_total = _project_counts(project_id)["total"]
                        if post_reroll_total <= pre_reroll_total:
                            useless_streak += 1
                            print(f"  ⚠ Reroll trouxe 0 artigos novos — useless_streak={useless_streak}")
                            # Mantém stagnation alto pra reentrar nesse bloco
                            # mas se atingiu limite, próxima iter pula pra years_window
                            if useless_streak >= USELESS_OPERATIONS_LIMIT:
                                discovery_rerolls_done = DISCOVERY_REROLL_LIMIT  # queima
                                continue
                        else:
                            useless_streak = 0
                        stagnation = 0
                        rotations_done = 0
                        expansion_iter = 0
                        prev_total = post_reroll_total
                        continue
                    except LLMUnavailableError:
                        raise
                    except Exception as e:
                        print(f"  ⚠ reroll de stagnation falhou: {e}")
                # Alavanca 2: ainda temos expansões de janela temporal? Última cartada
                # antes de aceitar esgotamento. Janela apertada é causa COMUM de
                # 'esgotamento' falso — amplia 2× → 4× → 100 anos progressivamente.
                if (years_window_expansions_done < YEARS_WINDOW_EXPANSION_LIMIT
                        and stats["analyzed"] < MAX_TOTAL_ARTICLES):
                    years_window_expansions_done += 1
                    print(f"  ⏰ Stagnation persistente — Expandindo janela temporal "
                          f"#{years_window_expansions_done}/{YEARS_WINDOW_EXPANSION_LIMIT} "
                          f"(original={original_years_window} anos)")
                    try:
                        await _step_expand_years_window(
                            project_id, cfg, model,
                            original_years_window, years_window_expansions_done,
                        )
                        p = projects_module.get(project_id)
                        stagnation = 0
                        rotations_done = 0
                        expansion_iter = 0
                        # Reset rerolls + circuit breaker: nova janela é alavanca
                        # diferente e merece chance completa, sem herdar useless streak
                        # acumulado pelas rotações com a janela apertada.
                        discovery_rerolls_done = 0
                        useless_streak = 0
                        last_ratio_check_at = stats["analyzed"]
                        prev_total = _project_counts(project_id)["total"]
                        continue
                    except LLMUnavailableError:
                        raise
                    except Exception as e:
                        print(f"  ⚠ expansão de janela falhou: {e}")
                print(f"  ⊘ esgotamento real: {stagnation} iters sem novos artigos "
                      f"(analyzed={stats['analyzed']}, included={stats['included']}, "
                      f"rerolls={discovery_rerolls_done}/{DISCOVERY_REROLL_LIMIT}, "
                      f"yw_expansions={years_window_expansions_done}/{YEARS_WINDOW_EXPANSION_LIMIT})")
                break
            # 4) Safety: cap de iterações
            if iteration > HARD_ITERATION_CAP:
                print(f"  ⊘ cap HARD_ITERATION_CAP={HARD_ITERATION_CAP} atingido")
                break

            # ── Passo 1: analisar pendentes (em chunks, respeitando MAX) ──
            pending_n = _pending_count(project_id)
            if pending_n > 0 and stats["analyzed"] < MAX_TOTAL_ARTICLES:
                projects_module.update(project_id, status="analyzing")
                remaining_to_cap = MAX_TOTAL_ARTICLES - stats["analyzed"]
                chunk = min(ANALYZE_CHUNK_SIZE, remaining_to_cap, pending_n)
                pre_analyzed = stats["analyzed"]
                try:
                    await _step_analyze(project_id, cfg, model, batch_limit=chunk)
                except LLMUnavailableError:
                    # Erro fatal de LLM — propaga e aborta o pipeline
                    raise
                except Exception as e:
                    # Falha de chunk inteiro (rate limit storm, timeout transiente).
                    # NÃO mata o pipeline imediatamente — tenta de novo na próxima
                    # iteração. Só desiste após N falhas consecutivas.
                    consecutive_analyze_failures += 1
                    print(f"  ⚠ chunk de análise falhou "
                          f"({consecutive_analyze_failures}/{MAX_CONSECUTIVE_ANALYZE_FAILURES}): {e}")
                    if consecutive_analyze_failures >= MAX_CONSECUTIVE_ANALYZE_FAILURES:
                        raise RuntimeError(
                            f"Análise falhou em {consecutive_analyze_failures} chunks consecutivos. "
                            f"Último erro: {e}"
                        )
                    # Aguarda um pouco antes de tentar de novo (backoff)
                    await asyncio.sleep(2)
                    continue
                # Sucesso: reseta contador de falhas
                consecutive_analyze_failures = 0
                # Safety: travamento detectado se chunk não progrediu (sem exception)
                post = _project_counts(project_id)
                if post["analyzed"] <= pre_analyzed:
                    raise RuntimeError(
                        f"Pipeline travado: análise não progrediu "
                        f"(analyzed={pre_analyzed}). Provável falha de LLM."
                    )
                stats = post

            # ── Passo 1.5: regra de proporção (incluídos/analisados) ──
            # A cada RATIO_CHECK_INTERVAL artigos, confere se a proporção
            # real está alinhada com a esperada (target/MAX). Se não,
            # volta ao agente de descoberta pra gerar ângulos novos.
            if (stats["analyzed"] >= RATIO_CHECK_MIN_BASE
                    and stats["analyzed"] - last_ratio_check_at >= RATIO_CHECK_INTERVAL):
                last_ratio_check_at = stats["analyzed"]
                actual_ratio = stats["included"] / max(1, stats["analyzed"])
                # Tolerância: só reroll se proporção for SIGNIFICATIVAMENTE baixa.
                # Evita false positives quando target/MAX é muito apertado (ex: 40/1300=3%
                # — variação natural pode levar batches a ficar abaixo sem o tema estar errado).
                threshold = expected_inclusion_ratio * RATIO_CHECK_TOLERANCE
                if (actual_ratio < threshold
                        and stats["included"] < target
                        and stats["analyzed"] < MAX_TOTAL_ARTICLES
                        and discovery_rerolls_done < DISCOVERY_REROLL_LIMIT):
                    discovery_rerolls_done += 1
                    print(f"  ↻ Reroll #{discovery_rerolls_done}/{DISCOVERY_REROLL_LIMIT}: "
                          f"proporção {actual_ratio:.3f} < esperada {expected_inclusion_ratio:.3f} "
                          f"(incluídos={stats['included']}/{stats['analyzed']}) — "
                          f"agente de descoberta gerando ângulos novos")
                    try:
                        # attempt muito alto força LLM a divergir radicalmente
                        # dos termos anteriores (sinônimos, traduções, conceitos relacionados)
                        await _step_rotate_terms(
                            project_id, cfg, model,
                            1000 + discovery_rerolls_done * 100,
                        )
                        p = projects_module.get(project_id)
                        # Reset orçamentos: novas strings merecem chance completa
                        stagnation = 0
                        rotations_done = 0
                        expansion_iter = 0
                        prev_total = _project_counts(project_id)["total"]
                    except LLMUnavailableError:
                        raise
                    except Exception as e:
                        print(f"  ⚠ reroll de discovery falhou: {e}")

            # ── Passo 2: re-checar paradas após análise ──
            if stats["analyzed"] >= MAX_TOTAL_ARTICLES:
                continue  # próxima iter vai parar
            if stats["analyzed"] >= min_required and stats["included"] >= target:
                continue  # próxima iter vai parar

            # ── Passo 3: precisa mais artigos? expandir coleta ──
            # (só faz sentido se não há pendentes — senão, próxima iter analisa primeiro)
            needs_more_for_min = stats["analyzed"] < min_required
            needs_more_for_target = (stats["included"] < target
                                     and stats["analyzed"] < MAX_TOTAL_ARTICLES)
            attempted_expansion = False
            if (needs_more_for_min or needs_more_for_target) and _pending_count(project_id) == 0:
                projects_module.update(project_id, status="searching")
                reason_msg = (f"analyzed={stats['analyzed']}/{min_required}, "
                              f"included={stats['included']}/{target}")
                if rotations_done < MAX_TERM_ROTATIONS:
                    rotations_done += 1
                    attempted_expansion = True
                    try:
                        await _step_rotate_terms(project_id, cfg, model, rotations_done)
                        p = projects_module.get(project_id)
                    except LLMUnavailableError:
                        # LLM caiu / sem créditos — não tem como continuar
                        raise
                    except Exception as e:
                        print(f"  ⚠ rotate_terms #{rotations_done} falhou: {e}")
                elif expansion_iter < MAX_EXPANSION_ITERATIONS:
                    expansion_iter += 1
                    attempted_expansion = True
                    await _step_expand_search(
                        project_id, workspace_id, p, expansion_iter, [reason_msg],
                    )
                    await _step_dedup(project_id)
                else:
                    # Esgotou todos os mecanismos de expansão — força stagnation
                    stagnation = STAGNATION_LIMIT

            # ── Passo 4: stagnation SÓ conta quando tentamos expandir e nada veio ──
            # (iterações que só analisam não devem queimar o orçamento de stagnation)
            if attempted_expansion:
                new_total = _project_counts(project_id)["total"]
                if new_total <= prev_total:
                    stagnation += 1
                    useless_streak += 1
                    # Circuit breaker: rotate_terms/expand consecutivamente inúteis.
                    # LLM gerou strings hiperespecíficas (ex: frases descritivas
                    # entre aspas) que não existem na literatura. Não vale gastar
                    # mais orçamento de rotação — pula pra próxima alavanca.
                    if useless_streak >= USELESS_OPERATIONS_LIMIT:
                        print(f"  ⊘ Circuit breaker: {useless_streak} operações de busca "
                              f"consecutivas com 0 artigos novos — queimando rotações restantes "
                              f"e forçando próxima alavanca")
                        rotations_done = MAX_TERM_ROTATIONS
                        expansion_iter = MAX_EXPANSION_ITERATIONS
                        stagnation = STAGNATION_LIMIT
                        useless_streak = 0
                else:
                    stagnation = 0
                    useless_streak = 0
                prev_total = new_total

        # ── Invariant check pré-finalize (log apenas, não falha) ──
        # Detecta quando saímos do loop violando a regra imutável de 500 análises.
        # Saída legítima: analyzed < MIN porque coletado < MIN (fonte esgotada real).
        # Saída ilegítima: analyzed < MIN E coletado > analyzed (sobraram pendentes).
        final_stats = _project_counts(project_id)
        if final_stats["analyzed"] < min_required and final_stats["analyzed"] < final_stats["total"]:
            print(f"  ⚠⚠⚠ INVARIANT VIOLATION: analyzed={final_stats['analyzed']} < "
                  f"min_required={min_required} mas total coletado={final_stats['total']} "
                  f"(sobraram pendentes!) — investigar por que loop saiu antes")
        if final_stats["analyzed"] < min_required:
            print(f"  ⚠ Saída abaixo do mínimo: analyzed={final_stats['analyzed']} < {min_required} "
                  f"(coletado={final_stats['total']}, stagnation={stagnation}, iter={iteration}) "
                  f"— provável esgotamento real da fonte")

        # ══════ FASE D: Double-check + Finalize + Verify ══════
        await _step_double_check(project_id, cfg, model)
        await _step_finalize(project_id, target)
        await _step_verify(project_id)

        _close_orphan_jobs(project_id, "Pipeline concluído")
        projects_module.update(project_id, status="done", error=None)
    except LLMUnavailableError:
        traceback.print_exc()
        _fail_project(project_id, "Sem créditos")
    except Exception as e:
        traceback.print_exc()
        _fail_project(project_id, str(e))


def _project_counts(project_id: int) -> dict:
    """Conta artigos não-duplicados, analisados e incluídos."""
    with connect() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM articles WHERE project_id=? AND is_duplicate=0",
            (project_id,)).fetchone()[0]
        analyzed = conn.execute(
            "SELECT COUNT(*) FROM analyses WHERE project_id=?", (project_id,)).fetchone()[0]
        included = conn.execute(
            "SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='include'",
            (project_id,)).fetchone()[0]
        return {"total": total, "analyzed": analyzed, "included": included}


def _restore_pipeline_counters(project_id: int) -> tuple[int, int, int, int]:
    """Reconstrói contadores do pipeline a partir dos jobs já gravados.

    Quando o servidor reinicia e resume_interrupted_pipelines retoma um pipeline,
    contadores em memória (rotations_done, etc.) voltam a 0 — isso queimava o
    orçamento de rotações múltiplas vezes (caso #67 acumulou 221 rotações).
    Esta função lê os jobs persistidos e reconstrói os contadores.

    Convenção dos nomes de jobs:
      - rotate_terms_attempt{N}: N < 1000 = rotação normal; N >= 1000 = reroll discovery
      - expand_iter{N}: expansão mecânica (N começa em 1)
      - expand_years_window_lvl{N}: expansão de janela temporal

    Retorna: (rotations_done, discovery_rerolls_done, expansion_iter, yw_expansions)
    """
    rotations_done = discovery_rerolls_done = 0
    expansion_iter = years_window_expansions_done = 0
    with connect() as conn:
        rows = conn.execute(
            "SELECT step FROM jobs WHERE project_id=? AND step LIKE 'rotate_terms_attempt%'",
            (project_id,),
        ).fetchall()
        for r in rows:
            try:
                attempt = int(r["step"].replace("rotate_terms_attempt", ""))
            except ValueError:
                continue
            if attempt >= 1000:
                discovery_rerolls_done += 1
            else:
                rotations_done += 1
        expansion_iter = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE project_id=? AND step LIKE 'expand_iter%'",
            (project_id,),
        ).fetchone()[0]
        years_window_expansions_done = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE project_id=? AND step LIKE 'expand_years_window%'",
            (project_id,),
        ).fetchone()[0]
    return rotations_done, discovery_rerolls_done, expansion_iter, years_window_expansions_done


def _pending_count(project_id: int) -> int:
    """Conta artigos não-duplicados ainda sem análise."""
    with connect() as conn:
        return conn.execute(
            """SELECT COUNT(*) FROM articles a
               WHERE a.project_id=? AND a.is_duplicate=0
                 AND NOT EXISTS (
                   SELECT 1 FROM analyses an
                   WHERE an.project_id=a.project_id
                     AND an.source=a.source
                     AND an.external_id=a.external_id
                 )""",
            (project_id,),
        ).fetchone()[0]


# ─── Steps ─────────────────────────────────────────────────────────────────


def _api_key_for_project(project_id: int, cfg: Config) -> str | None:
    """Resolve qual API key usar pra esse projeto: a do workspace (se houver)
    ou a global do servidor. Retorna None se nem um nem outro tiverem chave —
    nesse caso o cliente OpenRouter falhará com auth, registrado como fatal."""
    p = projects_module.get(project_id)
    if not p:
        return cfg.openrouter_api_key
    ws = workspaces_module.get_by_id(p.workspace_id)
    if ws and ws.openrouter_api_key and ws.openrouter_api_key.strip():
        return ws.openrouter_api_key.strip()
    return cfg.openrouter_api_key


async def _step_discovery(project_id: int, cfg: Config, model: str) -> None:
    job_id = _create_job(project_id, "discovery")
    try:
        client = make_client(cfg, api_key_override=_api_key_for_project(project_id, cfg))
        p = projects_module.get(project_id)
        result, used_fallback = await asyncio.to_thread(
            run_discovery_with_fallback, client, model,
            topic=p.topic, objective=p.objective, years_window=p.years_window,
            review_type=p.review_type,
        )
        # Só registra uso de LLM se não foi fallback (fallback não chama o modelo)
        if not used_fallback and result.generation_id:
            usage_id = record_llm_call(
                project_id=project_id, pipeline_step="discovery", model=model,
                prompt_tokens=result.prompt_tokens,
                completion_tokens=result.completion_tokens,
                duration_ms=result.duration_ms,
                extra_metadata={"rationale": (result.rationale or "")[:200]},
                generation_id=result.generation_id,
            )
            _schedule_reconcile(usage_id, result.generation_id, cfg.openrouter_api_key)
        issues = verify_discovery(result)
        warning_parts = []
        if used_fallback:
            warning_parts.append("⚠ Fallback ativado (LLM indisponível ou retornou lixo)")
        if issues:
            warning_parts.append(f"avisos: {'; '.join(issues)}")
        if warning_parts:
            _job_update(job_id, message=" · ".join(warning_parts))
        projects_module.update(
            project_id,
            criteria_md=result.criteria_md,
            search_strings=result.search_strings,
            status="criteria_ready",
        )
        suffix = " (fallback)" if used_fallback else ""
        _job_done(job_id, message=f"Critérios + search strings gerados{suffix} ({result.rationale[:120]})")
    except Exception as e:
        _job_fail(job_id, e)
        raise


async def _step_search(project_id: int, workspace_id: int, source: str, query: str) -> None:
    """Busca multi-estratégia: roda as 3 estratégias EM PARALELO e dedupa por
    external_id. Antes era serial (estrita → relaxed → tema cru) mas a maioria
    das vezes precisamos das 3 pra atingir volume — paralelizar economiza
    1-2 min de wait sequencial.

    Estratégias (todas rodam ao mesmo tempo):
      1. Query do discovery agent (estrita, com MeSH/filtros)
      2. Query "ampliada" — remove filtros de tipo, mantém termos
      3. Query "tema cru" — só palavras-chave do tema
    Resultados são unificados via dict por external_id (dedup automático).
    """
    job_id = _create_job(project_id, f"search_{source}")
    if not query or len(query.strip()) < 5:
        p_init = projects_module.get(project_id)
        query = p_init.topic
    try:
        scraper = REGISTRY[source]
        max_per_source = MAX_RESULTS_PER_SOURCE[source]

        p = projects_module.get(project_id)
        strategies = _build_strategies(source, query, p.topic, p.years_window)

        all_results: dict[str, tuple] = {}
        strategy_log = []
        errors: list[str] = []

        async def run_strategy(idx: int, label: str, qs: str):
            try:
                # Cada estratégia tenta puxar até max_per_source/2 — somando vão
                # cobrir o cap; dedup por external_id elimina overlap.
                results = await scraper.search(qs, max_results=max_per_source,
                                               progress=lambda *_: None)
                return (idx, label, results, None)
            except Exception as e:
                return (idx, label, [], e)

        # Roda TODAS as estratégias em paralelo (era sequencial)
        tasks = [run_strategy(i, label, qs) for i, (label, qs) in enumerate(strategies, 1)]
        _job_update(job_id, progress=10,
                    message=f"Buscando em {len(strategies)} estratégias paralelas")
        outcomes = await asyncio.gather(*tasks, return_exceptions=False)

        for idx, label, results, err in outcomes:
            if err:
                errors.append(f"E{idx}({label}): {type(err).__name__}")
                strategy_log.append(f"E{idx} ({label}): erro")
                continue
            new_count = 0
            for art, raw in results:
                # Sem cap interno: deixa as 3 estratégias paralelas agregarem tudo
                # que acharam de único. O cap de quantidade é por-strategy (max_per_source)
                # passado pro scraper. O DB final ainda é controlado por MAX_TOTAL_ARTICLES.
                if art.external_id not in all_results:
                    all_results[art.external_id] = (art, raw)
                    new_count += 1
            strategy_log.append(f"E{idx} ({label}): +{new_count}")

        _job_update(job_id, progress=85,
                    message=f"Acumulado: {len(all_results)} ({' · '.join(strategy_log)})")

        # Persiste tudo num batch
        try:
            with connect() as conn:
                for art, raw in all_results.values():
                    upsert_article(conn, workspace_id=workspace_id,
                                   project_id=project_id, art=art, raw=raw)
        except Exception as e:
            # Persistência falhou após coleta bem-sucedida — registra mas não fatal:
            # próxima rodada de busca vai re-coletar o que perdemos.
            errors.append(f"persist: {type(e).__name__}: {e}")

        # Diagnostic: 0 artigos coletados em TODAS as estratégias é sinal grave
        # mas não derruba o pipeline (outras sources podem cobrir). Marca como
        # success com aviso pra o verify final ver no log.
        msg_parts = [f"{len(all_results)} artigos de {source}"]
        if strategy_log:
            msg_parts.append(f"({' · '.join(strategy_log)})")
        if errors:
            msg_parts.append(f"⚠ {len(errors)} erro(s): {'; '.join(errors[:3])}")
        _job_done(job_id, message=" ".join(msg_parts))
    except Exception as e:
        _job_fail(job_id, e)


# Limites por source — cada estratégia paralela puxa até esse teto.
# Como rodam 3 estratégias paralelas e há rotações posteriores, o universo
# real de artigos coletados pode ser muito maior antes de bater MAX_TOTAL_ARTICLES.
MAX_RESULTS_PER_SOURCE = {"pubmed": 1000, "scielo": 300, "scholar": 100}
TARGET_MIN_PER_SOURCE = {"pubmed": 200, "scielo": 50, "scholar": 30}


def _build_strategies(source: str, llm_query: str, topic: str,
                      years_window: int) -> list[tuple[str, str]]:
    """Retorna lista [(label, query)] em ordem de uso."""
    strategies = [("LLM", llm_query)]

    # Estratégia 2: remove filtros restritivos, mantém termos principais
    relaxed = _relax_query(llm_query, source)
    if relaxed and relaxed != llm_query:
        strategies.append(("ampliada", relaxed))

    # Estratégia 3: tema cru com janela temporal
    raw = _raw_topic_query(topic, source, years_window)
    if raw and raw != llm_query and raw != relaxed:
        strategies.append(("tema_cru", raw))

    return strategies


def _relax_query(query: str, source: str) -> str:
    """Remove os filtros mais restritivos (Publication Type, etc) mas mantém termos."""
    import re
    if source == "pubmed":
        # Remove filtros de Publication Type
        q = re.sub(r'\s*AND\s*\([^)]*Publication Type[^)]*\)', '', query, flags=re.IGNORECASE)
        # Remove filtros de Validation Study
        q = re.sub(r'\s*AND\s*\([^)]*Validation Study[^)]*\)', '', q, flags=re.IGNORECASE)
        # Mantém data se existir
        return q.strip()
    elif source == "scielo":
        # SciELO: simplifica AND/OR aninhados
        # Remove parênteses internos extras, mantém termos primários
        q = re.sub(r'\bAND\b\s*\("[^"]+"\s*OR\s*"[^"]+"\)', "", query)
        return q.strip().strip("()") or query
    else:  # scholar
        # Mantém só os 2-3 primeiros termos entre aspas
        terms = re.findall(r'"([^"]+)"', query)
        if len(terms) >= 2:
            return f'"{terms[0]}" AND "{terms[1]}"'
        return query


def _raw_topic_query(topic: str, source: str, years_window: int) -> str:
    """Tema bruto — palavras-chave principais sem filtros."""
    import re
    from datetime import date
    current_year = date.today().year
    start_year = current_year - years_window + 1

    words = re.findall(r'\b\w{4,}\b', topic.lower())
    stop = {"para", "como", "com", "uma", "este", "essa", "esse", "esta",
            "aplicada", "aplicado", "aplicação", "based", "the", "and", "for",
            "of", "in", "on", "uso", "use", "using", "with", "without"}
    keep = [w for w in words if w not in stop][:5]
    if not keep:
        return topic

    if source == "pubmed":
        terms = " AND ".join(f'"{w}"[Title/Abstract]' for w in keep)
        return f'({terms}) AND ("{start_year}"[Date - Publication] : "{current_year}"[Date - Publication])'
    elif source == "scielo":
        return " AND ".join(keep)
    else:
        return " ".join(f'"{w}"' for w in keep[:3])


async def _step_expand_search(project_id: int, workspace_id: int, project,
                              iteration: int, reasons: list[str]) -> None:
    """Expansão progressiva de busca quando target/mínimo não foi atingido.

    Cada iteração tenta uma estratégia mais ampla:
      it=1 → keywords do tema com janela 7 anos (vs 5)
      it=2 → keywords mais soltas + sem filtros de tipo
      it=3 → janela 10 anos + tema cru
      it=4 → último recurso: top-level keyword apenas
    """
    job_id = _create_job(project_id, f"expand_iter{iteration}")
    _job_update(job_id, message=f"Expansão iter {iteration} — motivo: {'; '.join(reasons)}")

    topic = project.topic
    yw = project.years_window
    expansions = _build_expansion_queries(topic, iteration, yw)

    total_added = 0
    for source in project.sources:
        if source not in REGISTRY:
            continue
        if source not in expansions:
            continue

        scraper = REGISTRY[source]
        max_per_source = MAX_RESULTS_PER_SOURCE.get(source, 200)

        try:
            new_query = expansions[source]
            results = await scraper.search(new_query, max_results=max_per_source,
                                           progress=lambda *_: None)
            with connect() as conn:
                added = 0
                for art, raw in results:
                    # upsert é idempotente; só conta novos por dedup posterior
                    upsert_article(conn, workspace_id=workspace_id,
                                   project_id=project_id, art=art, raw=raw)
                    added += 1
                total_added += added
            _job_update(job_id, message=f"iter{iteration} {source}: +{added} brutos")
        except Exception as e:
            _job_update(job_id, message=f"iter{iteration} {source}: erro {e}")

    _job_done(job_id, message=f"Expansão iter {iteration}: +{total_added} artigos brutos")


def _build_expansion_queries(topic: str, iteration: int, yw: int) -> dict[str, str]:
    """Gera queries cada vez mais amplas para cada source."""
    import re
    from datetime import date
    cy = date.today().year

    words = re.findall(r"\b\w{4,}\b", topic.lower())
    stop = {"para", "como", "com", "uma", "esse", "essa", "este", "esta",
            "based", "uso", "use", "using", "with", "and", "the", "for",
            "of", "in", "on", "by", "to"}
    keep = [w for w in words if w not in stop]
    if not keep:
        keep = words[:5]

    out = {}
    if iteration == 1:
        # Iter 1: keywords + janela 7 anos
        sy = cy - 7
        if len(keep) >= 2:
            out["pubmed"] = (f"({' AND '.join(f'\"{w}\"[Title/Abstract]' for w in keep[:5])}) "
                             f"AND (\"{sy}\"[Date - Publication] : \"{cy}\"[Date - Publication])")
            out["scielo"] = " AND ".join(keep[:4])
            out["scholar"] = " ".join(f'"{w}"' for w in keep[:3])
    elif iteration == 2:
        # Iter 2: menos keywords, OR'd
        if len(keep) >= 2:
            sy = cy - 7
            top2 = keep[:2]
            out["pubmed"] = (f"({top2[0]}[Title/Abstract] OR {top2[1]}[Title/Abstract]) "
                             f"AND (\"{sy}\"[Date - Publication] : \"{cy}\"[Date - Publication])")
            out["scielo"] = " OR ".join(keep[:3])
            out["scholar"] = " OR ".join(f'"{w}"' for w in keep[:2])
    elif iteration == 3:
        # Iter 3: janela 10 anos + tema cru
        sy = cy - 10
        if keep:
            out["pubmed"] = (f"({keep[0]}[Title/Abstract]) "
                             f"AND (\"{sy}\"[Date - Publication] : \"{cy}\"[Date - Publication])")
            out["scielo"] = keep[0]
            out["scholar"] = f'"{keep[0]}"'
    elif iteration == 4:
        # Iter 4: keyword principal, janela 15 anos
        sy = cy - 15
        if keep:
            out["pubmed"] = (f"{keep[0]}[Title/Abstract] "
                             f"AND (\"{sy}\"[Date - Publication] : \"{cy}\"[Date - Publication])")
            out["scielo"] = keep[0]
            out["scholar"] = keep[0]
    elif iteration == 5:
        # Iter 5: kw1 OR kw2, sem filtros
        if len(keep) >= 2:
            out["pubmed"] = f"{keep[0]}[Title/Abstract] OR {keep[1]}[Title/Abstract]"
            out["scielo"] = f"{keep[0]} OR {keep[1]}"
            out["scholar"] = f"{keep[0]} OR {keep[1]}"
        elif keep:
            out["pubmed"] = keep[0]
            out["scielo"] = keep[0]
            out["scholar"] = keep[0]
    elif iteration == 6:
        # Iter 6: tema completo no campo livre
        out["pubmed"] = topic
        out["scielo"] = topic
        out["scholar"] = topic
    else:  # iter 7+: keyword qualquer
        kw = keep[0] if keep else (words[0] if words else topic.split()[0])
        out["pubmed"] = kw
        out["scielo"] = kw
        out["scholar"] = kw
    return out


def _normalize_title_for_dedup(title: str | None) -> str:
    """Normaliza título pra detectar duplicatas: lowercase, sem acentos,
    sem pontuação, espaços colapsados. Retorna '' pra títulos curtos demais
    (evita falso-positivo em títulos genéricos do tipo 'Editorial')."""
    if not title:
        return ""
    import re
    import unicodedata
    # Remove diacríticos
    nfkd = unicodedata.normalize("NFKD", title)
    cleaned = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    # Lowercase, só alfanumérico + espaço, espaços colapsados
    cleaned = re.sub(r"[^a-z0-9\s]+", " ", cleaned.lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # Títulos < 40 chars têm risco alto de colisão acidental — não dedupa
    if len(cleaned) < 40:
        return ""
    return cleaned


async def _step_dedup(project_id: int) -> None:
    """Marca duplicados por DOI (primário) e por título normalizado (secundário,
    para artigos sem DOI que aparecem em múltiplas sources). Mantém o primeiro
    de cada grupo (ordem de scraped_at)."""
    job_id = _create_job(project_id, "dedup")
    try:
        n_dup_doi = 0
        n_dup_title = 0
        with connect() as conn:
            # zera estado de dedup
            conn.execute("UPDATE articles SET is_duplicate=0 WHERE project_id=?", (project_id,))

            # ── 1ª passada: DOI (case-insensitive) ──
            dups = conn.execute(
                """SELECT LOWER(doi) AS doi_lc, COUNT(*) AS n
                   FROM articles
                   WHERE project_id=? AND doi IS NOT NULL AND doi != ''
                   GROUP BY LOWER(doi) HAVING n > 1""",
                (project_id,),
            ).fetchall()
            for d in dups:
                rows = conn.execute(
                    """SELECT source, external_id FROM articles
                       WHERE project_id=? AND LOWER(doi)=? ORDER BY scraped_at ASC""",
                    (project_id, d["doi_lc"]),
                ).fetchall()
                for to_dedup in rows[1:]:
                    conn.execute(
                        """UPDATE articles SET is_duplicate=1
                           WHERE project_id=? AND source=? AND external_id=?""",
                        (project_id, to_dedup["source"], to_dedup["external_id"]),
                    )
                    n_dup_doi += 1

            # ── 2ª passada: título normalizado (só artigos ainda não-dups) ──
            # Estratégia: agrupa por título normalizado em Python (SQL não tem
            # unaccent built-in confiável). Só dedupa se ≥2 artigos compartilham
            # exatamente o mesmo título normalizado de comprimento ≥40 chars.
            non_dup_articles = conn.execute(
                """SELECT source, external_id, title, scraped_at
                   FROM articles
                   WHERE project_id=? AND is_duplicate=0
                   ORDER BY scraped_at ASC""",
                (project_id,),
            ).fetchall()

            title_groups: dict[str, list[tuple[str, str]]] = {}
            for r in non_dup_articles:
                norm = _normalize_title_for_dedup(r["title"])
                if not norm:
                    continue
                title_groups.setdefault(norm, []).append((r["source"], r["external_id"]))

            for _norm, members in title_groups.items():
                if len(members) < 2:
                    continue
                # Mantém o primeiro (ordem cronológica de scraped_at), marca os demais
                for source, ext_id in members[1:]:
                    conn.execute(
                        """UPDATE articles SET is_duplicate=1
                           WHERE project_id=? AND source=? AND external_id=?""",
                        (project_id, source, ext_id),
                    )
                    n_dup_title += 1

        total = n_dup_doi + n_dup_title
        msg = f"{total} duplicados marcados ({n_dup_doi} por DOI, {n_dup_title} por título)"
        _job_done(job_id, message=msg)
    except Exception as e:
        _job_fail(job_id, e)


async def _step_analyze(project_id: int, cfg: Config, model: str,
                        batch_limit: int | None = None) -> None:
    """Analisa artigos pendentes. Se `batch_limit` for dado, processa no máximo esse nº.

    Sempre respeita o cap DURO de 1300 análises totais por pipeline.
    """
    job_id = _create_job(project_id, "analyze")
    try:
        # Critérios DO PROJETO (gerados pelo discovery agent)
        p = projects_module.get(project_id)
        criteria = p.criteria_md or None
        topic = p.topic or None
        review_type = p.review_type
        with connect() as conn:
            pending = articles_without_analysis(conn, project_id)
        if not pending:
            _job_done(job_id, message="Nada a analisar")
            return

        # ── Cap DURO de 1300 análises por pipeline ──
        already = _project_counts(project_id)["analyzed"]
        budget = MAX_TOTAL_ARTICLES - already
        capped_msg = ""
        if budget <= 0:
            _job_done(job_id, message=f"Cap de {MAX_TOTAL_ARTICLES} análises atingido — pulando")
            return
        # Aplica batch_limit (chunked analysis) se passado
        effective_limit = budget if batch_limit is None else min(budget, batch_limit)
        if len(pending) > effective_limit:
            pending = pending[:effective_limit]
            if batch_limit is not None:
                capped_msg = f" (lote de {effective_limit})"
            else:
                capped_msg = f" (cap {MAX_TOTAL_ARTICLES} aplicado)"

        client = make_client(cfg, api_key_override=_api_key_for_project(project_id, cfg))
        total = len(pending)
        progress = {"done": 0, "successes": 0, "failures": 0,
                    "last_error": None}

        # Queue + worker pool: padrão determinístico que evita o bug do
        # asyncio.wait com reassignment de set em loop. 8 workers consomem
        # da fila; cada worker chama analyze_with_raw via asyncio.to_thread.
        queue: asyncio.Queue = asyncio.Queue()
        for art in pending:
            queue.put_nowait(art)

        async def worker() -> None:
            while True:
                try:
                    art = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    analysis, raw, usage = await asyncio.to_thread(
                        analyze_with_raw, client, model, art, criteria, topic, review_type)
                    with connect() as conn:
                        insert_analysis(conn, project_id, analysis, raw)
                    usage_id = record_llm_call(
                        project_id=project_id, pipeline_step="analyze", model=model,
                        prompt_tokens=usage.prompt_tokens,
                        completion_tokens=usage.completion_tokens,
                        duration_ms=usage.duration_ms,
                        article=art,
                        extra_metadata={"decision": analysis.decision,
                                        "quality_score": analysis.quality_score},
                        generation_id=usage.generation_id,
                    )
                    _schedule_reconcile(usage_id, usage.generation_id, cfg.openrouter_api_key)
                    progress["successes"] += 1
                except Exception as e:
                    progress["last_error"] = e
                    progress["failures"] += 1
                    print(f"  ✗ analyze: {e}")
                finally:
                    progress["done"] += 1
                    d = progress["done"]
                    if d % 5 == 0 or d == total:
                        _job_update(job_id, progress=int(100 * d / total),
                                    message=f"{progress['successes']}✓ {progress['failures']}✗ ({d}/{total})")

        await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(ANALYZE_CONCURRENCY)))

        successes = progress["successes"]
        failures = progress["failures"]
        last_error = progress["last_error"]

        # ── Falha total ou erro fatal de LLM → aborta o pipeline ──
        if successes == 0 and failures > 0:
            err = last_error if last_error is not None else RuntimeError(
                f"Análise abortada: {failures}/{total} chamadas falharam, 0 sucessos")
            assert isinstance(err, Exception)
            _job_fail(job_id, err)
            if _is_llm_fatal_error(err):
                raise LLMUnavailableError("Sem créditos")
            raise RuntimeError(
                f"Análise falhou em {failures}/{total} artigos. Último erro: {err}"
            )

        suffix = capped_msg
        if failures > 0:
            suffix += f" ({failures} falharam)"
        _job_done(job_id, message=f"{successes} artigos analisados{suffix}")
    except Exception as e:
        _job_fail(job_id, e)
        raise


async def _step_double_check(project_id: int, cfg: Config, model: str) -> None:
    job_id = _create_job(project_id, "double_check")
    try:
        p = projects_module.get(project_id)
        criteria = p.criteria_md or None
        topic = p.topic or None
        review_type = p.review_type
        with connect() as conn:
            # Double-check inteligente: só audita casos limítrofes (score 30-75
            # ou sem score). Exclusões com score < 30 são claramente ruins;
            # ≥ 75 são casos onde o LLM excluiu por violação rígida (não vai
            # mudar de ideia). Reduz custo e tempo do double-check em ~40%
            # sem comprometer auditoria de erros prováveis.
            pending = excluded_without_double_check(
                conn, project_id,
                score_floor=DOUBLE_CHECK_SCORE_FLOOR,
                score_ceiling=DOUBLE_CHECK_SCORE_CEILING,
                max_audits=DOUBLE_CHECK_MAX_AUDITS,
            )
        if not pending:
            _job_done(job_id, message="Nenhuma exclusão limítrofe para auditar")
            return
        client = make_client(cfg, api_key_override=_api_key_for_project(project_id, cfg))
        total = len(pending)
        progress = {"done": 0, "successes": 0, "failures": 0, "last_error": None}

        queue: asyncio.Queue = asyncio.Queue()
        for art, an in pending:
            queue.put_nowait((art, an))

        async def worker() -> None:
            while True:
                try:
                    art, an = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    dc, raw, usage = await asyncio.to_thread(
                        double_check_with_raw, client, model, art, an, criteria, topic, review_type)
                    with connect() as conn:
                        insert_double_check(conn, project_id, dc, raw)
                    usage_id = record_llm_call(
                        project_id=project_id, pipeline_step="double_check", model=model,
                        prompt_tokens=usage.prompt_tokens,
                        completion_tokens=usage.completion_tokens,
                        duration_ms=usage.duration_ms,
                        article=art,
                        extra_metadata={"agrees": dc.agrees,
                                        "final_decision": dc.final_decision},
                        generation_id=usage.generation_id,
                    )
                    _schedule_reconcile(usage_id, usage.generation_id, cfg.openrouter_api_key)
                    progress["successes"] += 1
                except Exception as e:
                    progress["failures"] += 1
                    progress["last_error"] = e
                    print(f"  ✗ double_check: {e}")
                finally:
                    progress["done"] += 1
                    d = progress["done"]
                    if d % 5 == 0 or d == total:
                        _job_update(job_id, progress=int(100 * d / total),
                                    message=f"{progress['successes']}✓ {progress['failures']}✗ ({d}/{total})")

        await asyncio.gather(*(asyncio.create_task(worker()) for _ in range(DOUBLE_CHECK_CONCURRENCY)))

        successes = progress["successes"]
        failures = progress["failures"]
        last_error = progress["last_error"]

        # Falha total → aborta o pipeline (mesma defesa do _step_analyze).
        # Se 100% dos double-checks falharem, é provável que o LLM esteja fora
        # ou sem créditos — não dá pra confiar nas exclusões sem auditoria.
        if successes == 0 and failures > 0:
            err = last_error if last_error is not None else RuntimeError(
                f"Double-check abortado: {failures}/{total} chamadas falharam, 0 sucessos")
            assert isinstance(err, Exception)
            _job_fail(job_id, err)
            if _is_llm_fatal_error(err):
                raise LLMUnavailableError("Sem créditos") from err
            raise RuntimeError(
                f"Double-check falhou em {failures}/{total} exclusões. Último erro: {err}"
            )

        suffix = ""
        if failures > 0:
            suffix = f" ({failures} falharam — exclusões sem auditoria 2ª passada)"
        _job_done(job_id, message=f"{successes} exclusões auditadas{suffix}")
    except Exception as e:
        _job_fail(job_id, e)
        raise


async def _step_expand_years_window(project_id: int, cfg: Config, model: str,
                                    original_years_window: int, level: int) -> None:
    """Amplia a janela temporal e re-executa busca + rotate_terms.

    Última alavanca antes de aceitar 'fonte esgotada'. Quando termos
    rotacionados + expansões mecânicas + rerolls de discovery não trouxeram
    artigos suficientes dentro da janela atual, escala a janela usando
    YEARS_WINDOW_EXPANSION_LADDER (2× → 4× → 100 anos = "qualquer ano").

    Atualiza `project.years_window` no DB (visível no PDF e na UI), regera
    search strings com a nova janela e re-executa search em cada source.
    """
    multiplier = YEARS_WINDOW_EXPANSION_LADDER[level - 1]
    new_window = max(original_years_window * multiplier, 1)
    job_id = _create_job(project_id, f"expand_years_window_lvl{level}")
    try:
        projects_module.update(project_id, years_window=new_window)
        p = projects_module.get(project_id)
        # Regera search strings com a nova janela (LLM produz queries
        # apropriadas pra novo intervalo). attempt alto força divergência
        # dos termos anteriores também.
        client = make_client(cfg, api_key_override=_api_key_for_project(project_id, cfg))
        rot = await asyncio.to_thread(
            rotate_search_strings, client, model,
            topic=p.topic, previous_strings=p.search_strings or {},
            attempt=2000 + level * 100, years_window=new_window,
        )
        usage_id = record_llm_call(
            project_id=project_id, pipeline_step="expand_years_window", model=model,
            prompt_tokens=rot.prompt_tokens,
            completion_tokens=rot.completion_tokens,
            duration_ms=rot.duration_ms,
            extra_metadata={"new_window": new_window, "level": level},
            generation_id=rot.generation_id,
        )
        _schedule_reconcile(usage_id, rot.generation_id, cfg.openrouter_api_key)
        projects_module.update(project_id, search_strings=rot.strings)
        # Re-busca em todas as sources com a janela ampliada
        for source in p.sources:
            if source not in REGISTRY:
                continue
            await _step_search(project_id, p.workspace_id, source,
                               rot.strings.get(source, p.topic))
        await _step_dedup(project_id)
        _job_done(job_id, message=f"Janela ampliada: {original_years_window}→{new_window} anos · busca refeita")
    except Exception as e:
        _job_fail(job_id, e)
        if _is_llm_fatal_error(e):
            raise LLMUnavailableError("Sem créditos") from e
        # Não fatal — pipeline tenta próxima alavanca


async def _step_rotate_terms(project_id: int, cfg: Config, model: str,
                             attempt: int) -> None:
    """Troca os termos de pesquisa quando os atuais estão produzindo 0 inclusões.

    Pede ao LLM search strings ALTERNATIVAS (sinônimos, traduções, ângulos diferentes),
    grava em `project.search_strings`, e dispara nova rodada de search.
    """
    job_id = _create_job(project_id, f"rotate_terms_attempt{attempt}")
    try:
        p = projects_module.get(project_id)
        client = make_client(cfg, api_key_override=_api_key_for_project(project_id, cfg))
        rot = await asyncio.to_thread(
            rotate_search_strings, client, model,
            topic=p.topic, previous_strings=p.search_strings or {},
            attempt=attempt, years_window=p.years_window,
        )
        usage_id = record_llm_call(
            project_id=project_id, pipeline_step="rotate_terms", model=model,
            prompt_tokens=rot.prompt_tokens,
            completion_tokens=rot.completion_tokens,
            duration_ms=rot.duration_ms,
            extra_metadata={"attempt": attempt},
            generation_id=rot.generation_id,
        )
        _schedule_reconcile(usage_id, rot.generation_id, cfg.openrouter_api_key)
        new_strings = rot.strings
        # Mescla: novos termos sobrescrevem, mas mantém antigos como base de comparação
        projects_module.update(project_id, search_strings=new_strings)
        # Re-executa search com os novos termos
        for source in p.sources:
            if source not in REGISTRY:
                continue
            await _step_search(project_id, p.workspace_id, source,
                               new_strings.get(source, p.topic))
        await _step_dedup(project_id)
        _job_done(job_id, message=f"Rotação #{attempt}: termos trocados e nova busca executada")
    except Exception as e:
        _job_fail(job_id, e)
        # Erros fatais de LLM (sem créditos, chave inválida) abortam o pipeline
        if _is_llm_fatal_error(e):
            raise LLMUnavailableError("Sem créditos") from e
        # Erros não-fatais (ex: source temporariamente fora) são logados mas não param tudo


async def _step_finalize(project_id: int, target: int) -> None:
    """Aplica o cutoff por quality_score.

    Marca como `in_top_n=1` os top N (ordenados por quality_score DESC) entre os incluídos.
    Marca como `in_top_n=0` o resto. Excluídos não são tocados.

    GARANTIA: nunca promove artigo que a IA marcou como exclude.
    GARANTIA: nunca marca mais que `target` como in_top_n=1.
    """
    job_id = _create_job(project_id, "finalize")
    try:
        with connect() as conn:
            # Reset
            conn.execute(
                "UPDATE analyses SET in_top_n=0 WHERE project_id=?", (project_id,))
            # Pega ids dos incluídos ordenados por score DESC
            top_rows = conn.execute(
                """SELECT source, external_id FROM analyses
                   WHERE project_id=? AND decision='include'
                   ORDER BY quality_score DESC NULLS LAST,
                            analyzed_at ASC LIMIT ?""",
                (project_id, target),
            ).fetchall()
            # Marca os top como in_top_n=1
            for r in top_rows:
                conn.execute(
                    """UPDATE analyses SET in_top_n=1
                       WHERE project_id=? AND source=? AND external_id=?""",
                    (project_id, r["source"], r["external_id"]),
                )
            # Conta resultado
            n_top = conn.execute(
                "SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='include' AND in_top_n=1",
                (project_id,)).fetchone()[0]
            n_below = conn.execute(
                "SELECT COUNT(*) FROM analyses WHERE project_id=? AND decision='include' AND in_top_n=0",
                (project_id,)).fetchone()[0]
        _job_done(job_id, message=f"Top {n_top} marcado · {n_below} incluídos abaixo do corte")
    except Exception as e:
        _job_fail(job_id, e)
        raise


async def _step_verify(project_id: int) -> None:
    """Checklist interno final — agora verifica também:
       - Mínimo de artigos analisados conforme tipo de revisão
         (systematic_review=500, narrative_review=200), ou explicação se cap atingido
       - Top N final respeita o target (nunca > target, nunca promove excluído)

    AUTO-CURA: se detectar invariante quebrado (top N tem excluído ou top N > target),
    re-roda finalize uma vez antes de gerar o checklist final.
    """
    job_id = _create_job(project_id, "verify")
    checklist: list[tuple[str, bool, str]] = []
    try:
        p = projects_module.get(project_id)
        # Auto-cura: detecta invariantes quebrados antes do checklist final
        with connect() as conn:
            n_top_pre = conn.execute(
                "SELECT COUNT(*) FROM analyses WHERE project_id=? AND in_top_n=1",
                (project_id,)).fetchone()[0]
            n_top_with_exclude_pre = conn.execute(
                "SELECT COUNT(*) FROM analyses WHERE project_id=? AND in_top_n=1 AND decision != 'include'",
                (project_id,)).fetchone()[0]
        if n_top_with_exclude_pre > 0 or n_top_pre > p.target_articles:
            _job_update(job_id, message=(
                f"Auto-cura: detectado invariante quebrado "
                f"(top={n_top_pre}, target={p.target_articles}, "
                f"excluídos no top={n_top_with_exclude_pre}); re-rodando finalize"
            ))
            await _step_finalize(project_id, p.target_articles)

        with connect() as conn:
            stats = project_stats(conn, project_id)
            jobs = conn.execute(
                "SELECT step, status FROM jobs WHERE project_id=? AND step!='verify'",
                (project_id,),
            ).fetchall()
            n_top = conn.execute(
                "SELECT COUNT(*) FROM analyses WHERE project_id=? AND in_top_n=1",
                (project_id,)).fetchone()[0]
            n_top_with_exclude = conn.execute(
                "SELECT COUNT(*) FROM analyses WHERE project_id=? AND in_top_n=1 AND decision != 'include'",
                (project_id,)).fetchone()[0]

        # Discovery
        checklist.append((
            "Discovery rodou e gerou critérios",
            bool(p.criteria_md and len(p.criteria_md) > 200),
            f"{len(p.criteria_md or '')} chars",
        ))
        # Search por source
        for source in p.sources:
            ran = any(j["step"] == f"search_{source}" and j["status"] == "success" for j in jobs)
            n = 0
            with connect() as conn:
                n = conn.execute(
                    "SELECT COUNT(*) FROM articles WHERE project_id=? AND source=?",
                    (project_id, source),
                ).fetchone()[0]
            checklist.append((f"Search {source}", ran or n > 0, f"{n} artigos"))
        # Dedup
        checklist.append(("Dedup executado", True, f"{stats['duplicates']} duplicados"))
        # Analyze — regra IMUTÁVEL: ≥ min_required (sistemática=500, narrativa=200)
        # Exceção legítima: fonte esgotada (analyzed = total coletado < min_required).
        # NUNCA aceitar analyzed < min_required quando ainda há pendentes.
        min_req = _min_required_for(p.review_type)
        analyzed_enough = (
            stats["analyzed"] >= min_req
            or stats["analyzed"] >= stats["articles"]  # esgotamento real
        )
        checklist.append((
            f"Analisou ≥{min_req} (regra imutável) ou fonte esgotada",
            analyzed_enough,
            f"{stats['analyzed']} analisados / {stats['articles']} coletados / mínimo {min_req}",
        ))
        # Cap MAX nunca pode ser ultrapassado
        checklist.append((
            f"Análises ≤ {MAX_TOTAL_ARTICLES} (cap absoluto)",
            stats["analyzed"] <= MAX_TOTAL_ARTICLES,
            f"{stats['analyzed']} analisados",
        ))
        # Invariante imutável: ou atingiu target, ou chegou em MAX, ou esgotou
        # genuinamente (fonte sem mais artigos a oferecer). Saída fora dessas
        # 3 categorias com analyzed entre [min, MAX-1] e included < target
        # significa que o reroll de discovery deveria ter sido acionado.
        target_or_max_or_exhausted = (
            stats["included"] >= p.target_articles
            or stats["analyzed"] >= MAX_TOTAL_ARTICLES
            or stats["analyzed"] >= stats["articles"]  # esgotou o que coletou
        )
        checklist.append((
            "Saída válida (target atingido OU cap MAX OU esgotamento real)",
            target_or_max_or_exhausted,
            f"included={stats['included']}/{p.target_articles}, "
            f"analyzed={stats['analyzed']}/{MAX_TOTAL_ARTICLES}, "
            f"coletado={stats['articles']}",
        ))
        # Double-check inteligente: só audita exclusões limítrofes na faixa
        # [DC_SCORE_FLOOR, DC_SCORE_CEILING) ou sem score, com cap de
        # DOUBLE_CHECK_MAX_AUDITS (priorizando os mais próximos do corte).
        with connect() as conn:
            n_borderline = conn.execute(
                """SELECT COUNT(*) FROM analyses
                   WHERE project_id=? AND decision='exclude'
                     AND (quality_score IS NULL
                          OR (quality_score >= ? AND quality_score < ?))""",
                (project_id, DOUBLE_CHECK_SCORE_FLOOR, DOUBLE_CHECK_SCORE_CEILING),
            ).fetchone()[0]
        target_dc = min(n_borderline, DOUBLE_CHECK_MAX_AUDITS)
        checklist.append((
            "Double-check cobre exclusões limítrofes (cap inteligente)",
            stats["double_checked"] >= target_dc,
            f"{stats['double_checked']}/{target_dc} (cap {DOUBLE_CHECK_MAX_AUDITS} sobre {n_borderline} limítrofes; {stats['excluded']} exclusões totais)",
        ))
        # Pelo menos 1 incluído
        checklist.append((
            "Pelo menos 1 incluído com score",
            stats["included"] >= 1,
            f"{stats['included']} incluídos",
        ))
        # Coleta — só checagem informativa (a regra imutável é sobre análises)
        type_label = "narrativa" if p.review_type == "narrative_review" else "sistemática"
        checklist.append((
            f"Coletou ≥{min_req} artigos (revisão {type_label})",
            stats["articles"] >= min_req,
            f"{stats['articles']} artigos coletados",
        ))
        # Top N respeita target
        checklist.append((
            f"Top N final ≤ target ({n_top} ≤ {p.target_articles})",
            n_top <= p.target_articles,
            f"{n_top} marcados como top",
        ))
        # NENHUM artigo no top N pode ter decision != include
        checklist.append((
            "Top N não promove artigos excluídos pela IA",
            n_top_with_exclude == 0,
            f"{n_top_with_exclude} artigos no top sem decision='include' (deve ser 0)",
        ))

        all_ok = all(ok for _, ok, _ in checklist)
        msg = json.dumps([{"check": c, "ok": ok, "detail": d} for c, ok, d in checklist], ensure_ascii=False)
        if all_ok:
            _job_done(job_id, message=msg)
        else:
            failed = [c for c, ok, _ in checklist if not ok]
            _job_update(job_id, status="success", message=msg,
                        error=f"Avisos: {', '.join(failed)}", finish=True)
    except Exception as e:
        _job_fail(job_id, e)


# ─── Helpers ───────────────────────────────────────────────────────────────


def _create_job(project_id: int, step: str) -> int:
    with connect() as conn:
        return create_job(conn, project_id, step)


def _job_update(job_id: int, **kw) -> None:
    with connect() as conn:
        update_job(conn, job_id, **kw)


def _job_done(job_id: int, *, message: str = "") -> None:
    with connect() as conn:
        update_job(conn, job_id, status="success", progress=100,
                   message=message, finish=True)


def _job_fail(job_id: int, err: Exception) -> None:
    with connect() as conn:
        update_job(conn, job_id, status="failed",
                   error=f"{type(err).__name__}: {err}", finish=True)


def _fail_project(project_id: int, msg: str) -> None:
    _close_orphan_jobs(project_id, "Pipeline encerrado")
    projects_module.update(project_id, status="failed", error=msg)


def _close_orphan_jobs(project_id: int, reason: str) -> None:
    """Marca como failed quaisquer jobs deste projeto presos em status='running'.
    Evita que o banner do front fique em loop por causa de jobs órfãos."""
    with connect() as conn:
        conn.execute(
            "UPDATE jobs SET status='failed', error=?, finished_at=datetime('now') "
            "WHERE project_id=? AND status='running'",
            (reason, project_id),
        )


# Tempo máximo sem atualização de job pra considerar um pipeline como zumbi.
# Em pipelines reais, o intervalo entre updates raramente passa de 2-3 minutos
# (cada análise demora 1-3s, double-check idem). 10 minutos é margem confortável.
PIPELINE_ZOMBIE_THRESHOLD_MINUTES = 10


def is_pipeline_zombie(project_id: int) -> tuple[bool, str]:
    """Detecta pipeline zumbi (status=running mas sem progresso há muito tempo).

    Cenário: uvicorn reiniciou no meio do pipeline ou houve crash não-tratado;
    project.status ficou em 'searching'/'analyzing'/'discovering' pra sempre,
    bloqueando o botão Iniciar.

    Retorna (is_zombie, reason). Considera zumbi se:
      - status é de execução (searching/analyzing/discovering)
      - E não há job 'running' OU o último job_update foi há > threshold
    """
    p = projects_module.get(project_id)
    if not p or p.status not in ("searching", "analyzing", "discovering"):
        return (False, "")
    with connect() as conn:
        row = conn.execute(
            """SELECT
                  (SELECT COUNT(*) FROM jobs WHERE project_id=? AND status='running') AS running_jobs,
                  (SELECT MAX(COALESCE(finished_at, started_at))
                     FROM jobs WHERE project_id=?) AS last_activity,
                  (CAST((julianday('now') -
                         julianday(COALESCE(
                            (SELECT MAX(COALESCE(finished_at, started_at))
                               FROM jobs WHERE project_id=?),
                            datetime('now')))) * 24 * 60 AS INTEGER)) AS minutes_since
            """,
            (project_id, project_id, project_id),
        ).fetchone()
    minutes_since = row["minutes_since"] if row and row["minutes_since"] is not None else 0
    running_jobs = row["running_jobs"] if row else 0

    # Sem jobs running E status diz que está rodando = zumbi imediato
    if running_jobs == 0:
        return (True, f"status={p.status} mas nenhum job 'running'")
    # Tem jobs running mas sem update há muito tempo = zumbi por inatividade
    if minutes_since > PIPELINE_ZOMBIE_THRESHOLD_MINUTES:
        return (True, f"sem progresso há {minutes_since} minutos")
    return (False, "")


def reset_zombie_pipeline(project_id: int) -> None:
    """Reseta um projeto zumbi pra permitir reinício do pipeline.

    Marca jobs órfãos como failed, volta status pra 'criteria_ready' (se já tem
    critérios) ou 'draft' (se não tem). Não apaga dados — só destrava o estado.
    """
    p = projects_module.get(project_id)
    if not p:
        return
    _close_orphan_jobs(project_id, "Pipeline zumbi resetado")
    # Volta pro estado anterior viável: se discovery já rodou, vai pra criteria_ready
    new_status = "criteria_ready" if (p.criteria_md and p.search_strings.get("pubmed")) else "draft"
    projects_module.update(project_id, status=new_status, error=None)


def resume_interrupted_pipelines() -> list[int]:
    """Retoma pipelines interrompidos por crash do servidor.

    Chamado no startup do FastAPI. Procura projetos cujo status está em estado de
    execução (searching/analyzing/discovering) — sintoma de que o uvicorn caiu
    antes do pipeline terminar. O pipeline é idempotente:
      - Discovery: skip se criteria_md já existe
      - Search: upsert (não duplica)
      - Analyze: só processa articles_without_analysis (já analisados ficam)
      - Double-check: só processa excluded_without_double_check
      - Finalize/verify: idempotentes

    Retorna lista de project_ids retomados.
    """
    resumed: list[int] = []
    with connect() as conn:
        rows = conn.execute(
            "SELECT id FROM projects WHERE status IN ('searching', 'analyzing', 'discovering')"
        ).fetchall()
    for r in rows:
        pid = r["id"]
        # Limpa jobs órfãos do crash anterior antes de re-agendar
        _close_orphan_jobs(pid, "Pipeline retomado após reinício do servidor")
        try:
            schedule_pipeline(pid)
            resumed.append(pid)
            print(f"  ▶ Pipeline retomado: project {pid}")
        except Exception as e:
            print(f"  ✗ Falha ao retomar project {pid}: {e}")
    return resumed


# ─── Background scheduler (chamado pela rota POST /projetos/.../iniciar) ────


# Event loop principal do uvicorn — capturado no startup
_MAIN_LOOP: asyncio.AbstractEventLoop | None = None


def set_main_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _MAIN_LOOP
    _MAIN_LOOP = loop


async def run_discovery_only(project_id: int) -> None:
    """Roda apenas o passo de discovery (gera critérios PICO + search strings).

    Usado pelo fluxo de pré-visualização: o usuário cria o projeto, vê os critérios
    que a IA gerou e só então decide se quer iniciar o pipeline completo.
    """
    cfg = Config.load()
    p = projects_module.get(project_id)
    if not p:
        return
    if p.criteria_md and p.search_strings.get("pubmed"):
        # Já tem critérios — não regera
        return
    ws = workspaces_module.get_by_id(p.workspace_id)
    if not ws:
        _fail_project(project_id, "Workspace não encontrado")
        return
    model = ws.openrouter_model or cfg.openrouter_model
    projects_module.update(project_id, status="discovering", error=None)
    try:
        await _step_discovery(project_id, cfg, model)
    except Exception as e:
        if _is_llm_fatal_error(e):
            _fail_project(project_id, "Sem créditos")
        else:
            _fail_project(project_id, f"Discovery falhou: {type(e).__name__}")


def schedule_discovery_only(project_id: int) -> None:
    """Agenda apenas o discovery em background (mesma mecânica do schedule_pipeline)."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(run_discovery_only(project_id))
    except RuntimeError:
        if _MAIN_LOOP is not None:
            asyncio.run_coroutine_threadsafe(run_discovery_only(project_id), _MAIN_LOOP)
        else:
            import threading
            threading.Thread(
                target=lambda: asyncio.run(run_discovery_only(project_id)),
                daemon=True,
            ).start()


def schedule_pipeline(project_id: int) -> None:
    """Dispara o pipeline em background. Funciona dentro ou fora do event loop."""
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(run_full_pipeline(project_id))
    except RuntimeError:
        # Estamos numa thread sem event loop (ex: rota síncrona do FastAPI)
        if _MAIN_LOOP is not None:
            asyncio.run_coroutine_threadsafe(run_full_pipeline(project_id), _MAIN_LOOP)
        else:
            # Último recurso: roda sincronamente em nova thread
            import threading
            threading.Thread(
                target=lambda: asyncio.run(run_full_pipeline(project_id)),
                daemon=True,
            ).start()
