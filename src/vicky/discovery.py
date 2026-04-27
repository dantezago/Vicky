"""Discovery Agent — gera critérios PICO + search strings a partir de um tema."""

from __future__ import annotations

import json
from dataclasses import dataclass

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class DiscoveryResult:
    criteria_md: str
    search_strings: dict[str, str]   # {"pubmed": "...", "scielo": "...", "scholar": "..."}
    rationale: str
    raw_response: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    duration_ms: int = 0
    generation_id: str = ""


SYSTEM_PROMPT_SYSTEMATIC = """Você é um especialista sênior em metodologia científica e revisões sistemáticas, capaz de adaptar critérios PICO para QUALQUER campo do conhecimento (saúde, educação, ciências sociais, engenharia, gestão, direito, etc.).

Sua tarefa: dado um tema de Iniciação Científica, gerar critérios de inclusão/exclusão de elite seguindo a "Metodologia dos 4 Pilares" e produzir search strings prontas para PubMed, SciELO e Google Scholar.

# IMPORTANTE — adapte ao domínio do tema

PRIMEIRO identifique o domínio do tema:
- **Saúde/Medicina/Clínica**: trials clínicos, sobrevida, sensibilidade, especificidade, validação externa em outro hospital
- **Educação/Ensino**: comparação com método tradicional, retenção, satisfação, aproveitamento, pré/pós-teste
- **Ciências Sociais/Comportamento**: amostragem populacional, controle de viés, validação cruzada, intervenção vs controle
- **Engenharia/Computação**: benchmark vs estado-da-arte, replicabilidade, dataset público, métricas técnicas validadas
- **Gestão/Negócios**: estudo de caso múltiplo, validação por triangulação, generalização
- **Direito/Políticas**: análise comparada, jurisprudência, contexto regulatório múltiplo

DEPOIS adapte os 4 Pilares ao domínio:
1. **PICO** (P-População, I-Intervenção, C-Comparação, O-Outcome): use a terminologia do campo (estudantes, organizações, sistemas, etc.)
2. **Qualidade Metodológica**: Validação Externa/Multicêntrica/Multi-institucional (o equivalente do campo), N mínimo apropriado ao desenho
3. **Atualidade**: janela temporal de 3-5 anos típica (mais agressiva para tecnologia, mais ampla para teoria)
4. **Relevância Real**: desfechos PRIMÁRIOS do campo (cura/aprendizado/adoção/eficácia), excluir teoria pura sem aplicação

Quality Score 0-100 com rubrica genérica que se aplica a TODOS os campos.

# REGRAS PARA SEARCH STRINGS

## PubMed (usa MeSH terms quando aplicável)
- Sintaxe: `("Termo MeSH"[Mesh]) AND ("Outro"[Mesh]) AND (filtros)`
- Filtros: `("Clinical Trial"[Publication Type] OR "Multicenter Study"[Publication Type] OR "Validation Study"[Publication Type])`
- Limitar por data: `AND ("YYYY"[Date - Publication] : "YYYY"[Date - Publication])`

## SciELO
- Sintaxe simples: `(termo1 OR sinonimo) AND (termo2)` no campo título/resumo
- Idiomas comum: português, espanhol, inglês

## Google Scholar
- Use frases entre aspas: `"Artificial Intelligence" AND "mammography"`
- Foco em frases exatas para filtrar ruído
- Sem MeSH, mas pode usar `intitle:` para forçar termo no título

# FORMATO DA SAÍDA

Responda APENAS com JSON válido neste formato:

```json
{
  "criteria_md": "<Markdown completo dos critérios — 4 Pilares + Quality Score>",
  "search_strings": {
    "pubmed": "<string pronta para colar no PubMed Advanced Search>",
    "scielo": "<string para SciELO>",
    "scholar": "<string para Google Scholar>"
  },
  "rationale": "<2-3 frases explicando as escolhas estratégicas>"
}
```

O `criteria_md` deve seguir EXATAMENTE esta estrutura:

```markdown
# Protocolo de Seleção — {tema}

## Objetivo
{1-2 frases}

## 1. Critérios de Inclusão (DEVE atender TODOS) ✅

### 1.1 Padrão-Ouro de Validação
- **Validação Externa Independente** ou estudo multicêntrico
- {detalhes}

### 1.2 Recência
- Publicações entre {ano_inicio} e {ano_fim}

### 1.3 Robustez Amostral
- N mínimo: {N}

### 1.4 Métricas / Desfechos Reportados
- {desfechos primários do campo — adapte: AUC/sensibilidade para saúde, ganho de aprendizado para educação, ROI para gestão, etc.}

### 1.5 Relevância Aplicada / Comparação com Baseline
- {o estudo precisa comparar com algo — padrão de cuidado, método tradicional, baseline da literatura, controle, etc.}

## 2. Critérios de Exclusão ❌

### 2.1 {categoria 1}
### 2.2 {categoria 2}
### 2.3 {categoria 3}
### 2.4 Tipo de publicação
- Editoriais, cartas, revisões narrativas, abstracts de congresso

### 2.5 Idioma
- Excluir idiomas que não sejam Inglês, Português ou Espanhol

## 3. Quality Score (0-100)

| Atributo | Peso |
|---|---|
| Validação externa / multi-institucional / replicação independente | 25 |
| N amostral robusto para o desenho do estudo (acima de {limite alto}) | 20 |
| Reporta {2 desfechos primários do campo} | 20 |
| Desenho rigoroso (RCT, prospectivo, longitudinal — adapte ao campo) | 15 |
| Comparação com baseline / controle / padrão de cuidado / método tradicional | 10 |
| Publicação nos últimos 2 anos | 10 |
```
"""


SYSTEM_PROMPT_NARRATIVE = """Você é um especialista sênior em metodologia científica, medicina baseada em evidências e escrita acadêmica, capaz de selecionar referências para artigos de RESUMO / REVISÃO NARRATIVA em QUALQUER campo do conhecimento (saúde, educação, ciências sociais, engenharia, etc.).

Sua tarefa: dado um tema, gerar critérios de inclusão/exclusão **flexíveis mas qualificados** — diferente de revisão sistemática, o objetivo não é selecionar apenas a elite metodológica, e sim reunir as **melhores referências para construir uma visão clara, atualizada, didática e bem fundamentada do tema**. Também deve produzir search strings prontas para PubMed, SciELO e Google Scholar.

# DIFERENÇA FUNDAMENTAL — REVISÃO NARRATIVA vs SISTEMÁTICA

Em revisão narrativa NÃO se exige obrigatoriamente:
- validação externa independente;
- estudo multicêntrico;
- grupo controle;
- baseline formal;
- N mínimo rígido em todos os artigos;
- apenas estudos primários;
- homogeneidade metodológica.

A seleção é guiada por **relevância temática, atualidade, confiabilidade e utilidade explicativa**. Revisões sistemáticas, metanálises, guidelines, consensos, revisões narrativas recentes e artigos de atualização SÃO BEM-VINDOS (ao contrário do protocolo sistemático, que costuma excluí-los).

# IMPORTANTE — adapte ao domínio do tema

PRIMEIRO identifique o domínio do tema (saúde/educação/engenharia/etc.) e adapte os exemplos, terminologia e desfechos.

# PILARES (substituem o PICO rígido)

1. **Pilar Temático** — pergunta norteadora ampla (não PICO rígido). Inclui população, tema central, subtemas, desfechos de interesse, contexto.
2. **Pilar de Atualidade** — janela temporal (default 5 anos). Artigos clássicos/fundamentais podem entrar mesmo se mais antigos.
3. **Pilar de Confiabilidade** — revistas indexadas, revisão por pares, instrumentos validados, metodologia clara.
4. **Pilar de Utilidade para o Texto** — cada artigo precisa servir para introduzir o tema, explicar epidemiologia, apresentar fatores de risco, manifestações, consequências, intervenções, lacunas, ou sustentar conclusão.

# REGRAS PARA SEARCH STRINGS

## PubMed
- Sintaxe: `(termo1 OR sinonimo) AND (termo2)`
- **NÃO** restringir por Publication Type (queremos revisões, guidelines, observacionais, etc.).
- Limitar por data: `AND ("YYYY"[Date - Publication] : "YYYY"[Date - Publication])`

## SciELO
- Sintaxe simples: `(termo1 OR sinonimo) AND (termo2)` no campo título/resumo
- Idiomas: português, espanhol, inglês

## Google Scholar
- Use frases entre aspas: `"saúde mental" AND "estudantes de medicina"`
- Foco em frases exatas para filtrar ruído

# FORMATO DA SAÍDA

Responda APENAS com JSON válido neste formato:

```json
{
  "criteria_md": "<Markdown completo dos critérios — 4 Pilares narrativos + Quality Score narrativo>",
  "search_strings": {
    "pubmed": "<string pronta para colar no PubMed Advanced Search>",
    "scielo": "<string para SciELO>",
    "scholar": "<string para Google Scholar>"
  },
  "rationale": "<2-3 frases explicando as escolhas estratégicas>"
}
```

O `criteria_md` deve seguir EXATAMENTE esta estrutura (mantenha os títulos exatos das seções 1, 2 e 3 — o sistema valida por eles):

```markdown
# Protocolo de Seleção (Artigo de Resumo / Revisão Narrativa) — {tema}

## Objetivo
Selecionar referências atuais, confiáveis, relevantes e didáticas para fundamentar um artigo de resumo / revisão narrativa sobre {tema}. Não é o objetivo realizar uma revisão sistemática exaustiva, e sim reunir as melhores referências para apresentar uma visão geral clara e bem fundamentada.

## 1. Critérios de Inclusão (atender um conjunto SUFICIENTE de critérios) ✅

> Diferente da revisão sistemática, o artigo NÃO precisa atender 100% dos critérios. Deve preencher um conjunto suficiente que justifique sua relevância para o texto final.

### 1.1 Alta relevância temática
- O artigo aborda diretamente {tema} ou subtemas fortemente relacionados ({listar 5-8 subtemas concretos do domínio: ex. ansiedade, depressão, burnout, sono, ideação suicida, fatores acadêmicos, intervenções de prevenção, etc.}).

### 1.2 Tipo de publicação prioritário
- Priorizar nesta ordem: (1) diretrizes/consensos/recomendações institucionais; (2) revisões sistemáticas e metanálises; (3) revisões narrativas/atualização recentes; (4) estudos observacionais relevantes (transversais, coortes, multicêntricos); (5) ensaios clínicos / estudos de intervenção; (6) estudos qualitativos bem conduzidos; (7) clássicos muito citados.

### 1.3 Recência
- Priorizar publicações dos últimos {janela} anos ({ano_inicio}–{ano_fim}). Artigos mais antigos só se forem clássicos, fundamentais, muito citados, indispensáveis para conceitos centrais ou instrumentos de avaliação.

### 1.4 Abrangência do conteúdo
- O artigo ajuda a responder perguntas como: prevalência, fatores associados, fases mais críticas, impactos, estratégias de prevenção/intervenção, lacunas, instrumentos de mensuração — adapte ao tema.

### 1.5 Qualidade metodológica suficiente
- Metodologia clara, amostra adequada (sem N mínimo rígido), descrição objetiva dos métodos. Para empíricos: tamanho da amostra coerente, instrumento descrito, desenho do estudo, análise estatística, coerência entre resultados e conclusão.

### 1.6 População adequada
- Estudos cuja população esteja diretamente alinhada ao tema, ou que tragam dados altamente comparáveis/úteis.

### 1.7 Desfechos relevantes
- Avalia desfechos diretamente relacionados ao tema (listar exemplos do domínio).

### 1.8 Aplicabilidade acadêmica ou clínica
- Tem utilidade prática para compreender fatores de risco, sinais de alerta, consequências, intervenções, programas, mudanças curriculares, políticas — adapte ao tema.

### 1.9 Idioma
- Priorizar inglês, português e espanhol.

### 1.10 Disponibilidade
- Priorizar artigos com abstract disponível e, quando possível, texto completo acessível.

## 2. Critérios de Exclusão ❌

### 2.1 Baixa relação com o tema
- Mencionam o tema apenas superficialmente, sem contribuir diretamente para o texto.

### 2.2 População inadequada
- Populações sem dados específicos ou aplicáveis ao tema.

### 2.3 Publicações pouco úteis para artigo de resumo
- Editoriais, cartas ao editor, comentários de opinião, resumos de congresso, textos sem dados claros, sem metodologia descrita, duplicados.
- **Exceção:** editoriais ou comentários podem entrar se forem de grande impacto, em revista de alto prestígio e úteis para contextualização.

### 2.4 Artigos antigos sem relevância clássica
- Mais de {janela} anos quando houver literatura recente equivalente.

### 2.5 Estudos com baixa qualidade metodológica evidente
- Metodologia pouco clara, amostra muito pequena sem justificativa, instrumentos não validados, conclusões exageradas, ausência de descrição de desfechos, viés evidente não discutido.

### 2.6 Estudos excessivamente específicos
- Muito locais ou limitados quando não ajudarem a construir uma visão geral.

### 2.7 Idiomas não acessíveis
- Diferentes de inglês, português ou espanhol — exceto se extremamente relevantes.

## 3. Quality Score (0-100) — voltado para utilidade no artigo de resumo

> Mede **utilidade para compor um artigo de resumo**, não pureza metodológica.

| Atributo | Peso |
|---|---|
| Relevância direta para o tema | 25 |
| Atualidade (preferencialmente últimos {janela} anos) | 15 |
| Qualidade e confiabilidade da fonte (revista indexada, revisão por pares) | 15 |
| Utilidade didática para explicar o tema | 15 |
| Abrangência do conteúdo | 10 |
| Qualidade metodológica do artigo | 10 |
| Aplicabilidade clínica, acadêmica ou institucional | 10 |

### Interpretação do score
- **85–100**: incluir com prioridade máxima.
- **70–84**: incluir com alta prioridade.
- **55–69**: incluir se houver espaço ou cobrir subtema específico.
- **40–54**: considerar apenas se houver pouca literatura sobre o subtema.
- **0–39**: excluir.
```
"""


# Mantém compatibilidade com importações antigas
SYSTEM_PROMPT = SYSTEM_PROMPT_SYSTEMATIC


REVIEW_TYPES = ("systematic_review", "narrative_review")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15), reraise=True)
def run_discovery(client: OpenAI, model: str, *, topic: str,
                  objective: str | None = None, years_window: int = 5,
                  review_type: str = "systematic_review") -> DiscoveryResult:
    from datetime import date
    current_year = date.today().year
    start_year = current_year - years_window + 1
    user = f"""**Tema:** {topic}
**Objetivo:** {objective or "Mapear o estado da arte e selecionar a elite metodológica"}

**ANO ATUAL: {current_year}**
**Janela temporal: últimos {years_window} anos = de {start_year} até {current_year}**

IMPORTANTE: as search strings DEVEM usar o intervalo {start_year}-{current_year}, NÃO use anos anteriores.
Para PubMed, use o filtro `("{start_year}"[Date - Publication] : "{current_year}"[Date - Publication])`.

Gere o protocolo completo + search strings para os 3 portais."""

    system_prompt = (
        SYSTEM_PROMPT_NARRATIVE if review_type == "narrative_review"
        else SYSTEM_PROMPT_SYSTEMATIC
    )
    import time
    t0 = time.monotonic()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        temperature=0.3,
        max_tokens=3500,
    )
    dt = int((time.monotonic() - t0) * 1000)
    raw = resp.choices[0].message.content or ""
    from .llm import _safe_parse_json
    parsed = _safe_parse_json(raw)
    usage = getattr(resp, "usage", None)
    ss = parsed.get("search_strings") if isinstance(parsed.get("search_strings"), dict) else {}

    return DiscoveryResult(
        criteria_md=str(parsed.get("criteria_md") or ""),
        search_strings={
            "pubmed": str(ss.get("pubmed") or ""),
            "scielo": str(ss.get("scielo") or ""),
            "scholar": str(ss.get("scholar") or ""),
        },
        rationale=str(parsed.get("rationale") or ""),
        raw_response=raw,
        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
        duration_ms=dt,
        generation_id=getattr(resp, "id", "") or "",
    )


@dataclass
class RotateResult:
    strings: dict[str, str]
    prompt_tokens: int = 0
    completion_tokens: int = 0
    duration_ms: int = 0
    generation_id: str = ""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15), reraise=True)
def rotate_search_strings(client: OpenAI, model: str, *, topic: str,
                          previous_strings: dict[str, str],
                          attempt: int = 1,
                          years_window: int = 5) -> RotateResult:
    """Gera search strings ALTERNATIVAS quando as anteriores não trouxeram inclusões.

    Pede ao LLM um ângulo diferente: sinônimos, termos correlatos, traduções,
    abreviações, formulações alternativas. NÃO repete os termos antigos.

    Retorna dict {pubmed, scielo, scholar} apenas — não regenera critérios.
    """
    from datetime import date
    cy = date.today().year
    sy = cy - years_window + 1

    prev_summary = json.dumps(previous_strings, ensure_ascii=False, indent=2)

    system = """Você é especialista em recuperação de informação científica.
Sua tarefa: sugerir search strings ALTERNATIVAS para um tema cujas queries anteriores
NÃO retornaram artigos relevantes (zero inclusões em 100+ artigos analisados).

Use SINÔNIMOS, termos correlatos, traduções (en/pt/es), abreviações, e ângulos
diferentes do mesmo problema. NÃO repita os termos das queries anteriores.

Responda em JSON com a chave "search_strings": {"pubmed": "...", "scielo": "...", "scholar": "..."}
"""
    user = f"""**Tema:** {topic}
**Tentativa de rotação:** #{attempt}
**Janela:** {sy}-{cy}

**Search strings ANTERIORES (não funcionaram):**
```json
{prev_summary}
```

Gere search strings ALTERNATIVAS — sinônimos, traduções, ângulos diferentes.
Para PubMed inclua o filtro de data `("{sy}"[Date - Publication] : "{cy}"[Date - Publication])`.
Responda em JSON: {{"search_strings": {{"pubmed": "...", "scielo": "...", "scholar": "..."}}}}"""

    import time
    t0 = time.monotonic()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        temperature=0.7,
        max_tokens=1500,
    )
    dt = int((time.monotonic() - t0) * 1000)
    raw = resp.choices[0].message.content or "{}"
    from .llm import _safe_parse_json
    parsed = _safe_parse_json(raw)
    ss = parsed.get("search_strings") if isinstance(parsed.get("search_strings"), dict) else {}
    usage = getattr(resp, "usage", None)
    return RotateResult(
        strings={
            "pubmed": ss.get("pubmed", "") or previous_strings.get("pubmed", ""),
            "scielo": ss.get("scielo", "") or previous_strings.get("scielo", ""),
            "scholar": ss.get("scholar", "") or previous_strings.get("scholar", ""),
        },
        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
        duration_ms=dt,
        generation_id=getattr(resp, "id", "") or "",
    )


def verify_discovery(result: DiscoveryResult) -> list[str]:
    """Dupla checagem do output do discovery agent. Retorna lista de problemas."""
    issues = []
    if len(result.criteria_md) < 500:
        issues.append("criteria_md muito curto (<500 chars)")
    expected_sections = ["## 1. Critérios de Inclusão", "## 2. Critérios de Exclusão", "## 3. Quality Score"]
    for section in expected_sections:
        if section not in result.criteria_md:
            issues.append(f"Seção ausente: {section}")
    for source, qs in result.search_strings.items():
        if not qs or len(qs) < 10:
            issues.append(f"Search string vazia ou muito curta para {source}")
    return issues


def fallback_discovery(*, topic: str, years_window: int = 5,
                       review_type: str = "systematic_review") -> DiscoveryResult:
    """Gera critérios + search strings genéricas quando o LLM falha repetidamente.

    Não substitui o discovery normal — é um plano B pra evitar matar o pipeline
    quando OpenRouter está fora ou retornando lixo. Usa o tema cru pra montar
    queries simples mas funcionais nas 3 sources.
    """
    import re
    from datetime import date
    cy = date.today().year
    sy = cy - years_window + 1

    words = re.findall(r"\b\w{4,}\b", (topic or "").lower())
    stop = {"para", "como", "com", "uma", "esse", "essa", "este", "esta",
            "based", "uso", "use", "using", "with", "and", "the", "for",
            "of", "in", "on", "by", "to", "or"}
    keep = [w for w in words if w not in stop][:5] or words[:3] or [topic or "research"]

    pubmed_q = (
        f"({' AND '.join(f'{w}[Title/Abstract]' for w in keep[:4])}) "
        f'AND ("{sy}"[Date - Publication] : "{cy}"[Date - Publication])'
    )
    scielo_q = " AND ".join(keep[:3])
    scholar_q = " ".join(f'"{w}"' for w in keep[:3])

    is_narrative = review_type == "narrative_review"
    type_label = "Artigo de Resumo / Revisão Narrativa" if is_narrative else "Revisão Sistemática"
    criteria_md = f"""# Protocolo de Seleção ({type_label}) — {topic}

## Objetivo
Critérios gerados em modo de contingência (sem LLM disponível) para o tema "{topic}".
Recomenda-se revisar e refinar manualmente quando possível.

## 1. Critérios de Inclusão ✅

### 1.1 Relevância temática
- O artigo aborda diretamente "{topic}" ou seus subtemas centrais.

### 1.2 Recência
- Publicações entre {sy} e {cy} ({years_window} anos).

### 1.3 Qualidade da fonte
- Revista indexada com revisão por pares.

### 1.4 Metodologia
- Metodologia clara e descrita (não exige desenho específico).

### 1.5 Idioma
- Inglês, português ou espanhol.

## 2. Critérios de Exclusão ❌

### 2.1 Baixa relação com o tema
- Mencionam o tema apenas superficialmente.

### 2.2 Tipo de publicação
- Editoriais, cartas, resumos de congresso, comentários sem dados.

### 2.3 Idioma
- Diferente de inglês, português ou espanhol.

### 2.4 Antiguidade
- Publicados antes de {sy} (sem relevância clássica reconhecida).

## 3. Quality Score (0-100)

| Atributo | Peso |
|---|---|
| Relevância direta para o tema | 30 |
| Atualidade ({sy}-{cy}) | 20 |
| Qualidade metodológica | 20 |
| Confiabilidade da fonte | 15 |
| Aplicabilidade prática | 15 |
"""

    return DiscoveryResult(
        criteria_md=criteria_md,
        search_strings={"pubmed": pubmed_q, "scielo": scielo_q, "scholar": scholar_q},
        rationale="Fallback automático — LLM indisponível, query baseada em keywords do tema.",
        raw_response="",
    )


def run_discovery_with_fallback(client: OpenAI, model: str, *, topic: str,
                                objective: str | None = None,
                                years_window: int = 5,
                                review_type: str = "systematic_review",
                                ) -> tuple[DiscoveryResult, bool]:
    """Tenta `run_discovery` (com retry interno × 3); se falhar, retorna fallback.

    Retorna (result, used_fallback). O caller pode checar `used_fallback` para logar
    aviso e gravar o status do projeto como "discovery em modo contingência".
    """
    try:
        result = run_discovery(client, model, topic=topic, objective=objective,
                               years_window=years_window, review_type=review_type)
        # Mesmo com sucesso, valida se tem o mínimo viável
        if (result.criteria_md and len(result.criteria_md) >= 200
                and any(qs and len(qs) >= 5 for qs in result.search_strings.values())):
            return result, False
        # Resposta lixo (curta demais ou search strings todas vazias) → fallback
    except Exception as e:
        print(f"  ⚠ run_discovery falhou após retries ({type(e).__name__}: {e}); usando fallback")
        # Re-levanta erros fatais de auth/quota (não adianta fallback)
        msg = str(e).lower()
        fatal = any(s in msg for s in [
            "key limit exceeded", "insufficient_quota", "invalid_api_key",
            "authentication", "401", "403", "no credits",
        ])
        if fatal:
            raise
    return fallback_discovery(topic=topic, years_window=years_window,
                              review_type=review_type), True
