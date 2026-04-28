"""Discovery Agent — gera critérios PICO + search strings a partir de um tema."""

from __future__ import annotations

import json
from dataclasses import dataclass

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential


@dataclass
class DiscoveryResult:
    criteria_md: str
    # Multi-substring: cada source tem uma LISTA de strings (default 6 ângulos
    # diferentes do tema). Antes era 1 string só por source.
    search_strings: dict[str, list[str]]
    rationale: str
    raw_response: str
    # Maturidade do tema declarada pelo discovery: high (RCTs abundantes),
    # moderate (ativo mas heterogêneo), emerging (predominantemente retrospectivos).
    # Adapta janela temporal e pesos do score específico no Modo Padrão.
    topic_maturity: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    duration_ms: int = 0
    generation_id: str = ""


# Quantas substrings o discovery agent gera por source
DEFAULT_SUBSTRINGS_PER_SOURCE = 6
# Mínimo aceitável (caso o LLM devolva poucas, completamos com fallbacks programáticos)
MIN_SUBSTRINGS_PER_SOURCE = 3


SYSTEM_PROMPT_SYSTEMATIC_ELITE = """Você é um especialista sênior em metodologia científica e revisões sistemáticas, capaz de adaptar critérios PICO para QUALQUER campo do conhecimento (saúde, educação, ciências sociais, engenharia, gestão, direito, etc.).

Sua tarefa: dado um tema de Iniciação Científica, gerar critérios de inclusão/exclusão de ELITE METODOLÓGICA seguindo a "Metodologia dos 4 Pilares" e produzir search strings prontas para PubMed, SciELO e Google Scholar.

# CONTEXTO — Modo "Elite Metodológica"

Este protocolo é usado quando o pesquisador escolheu o modo MAIS RIGOROSO (rigidity_mode='elite').
Aqui qualidade metodológica é PISO OBRIGATÓRIO: validação externa, N robusto, comparação com baseline,
desenho prospectivo são exigências de elegibilidade, NÃO apenas itens de score. Use este modo apenas
em temas com literatura abundante e madura.

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

Você deve gerar **6 substrings (ângulos diferentes)** por source — não apenas 1 — cobrindo
ângulos semânticos distintos para maximizar a recuperação de literatura. Cada substring deve
ser independente e poder rodar sozinha. Os 6 ângulos sugeridos:

  1. **PICO completo**: P + I + C + O todos juntos (busca rigorosa)
  2. **PICO sem comparador**: P + I + O (mais ampla, sem exigir grupo controle)
  3. **MeSH/keywords canônicos**: termos MeSH ou termos canônicos + filtros
  4. **Sinônimos PT/EN**: variações linguísticas e tradução de termos centrais
  5. **Recortes populacionais**: foco em subgrupos (idade, gênero, contexto)
  6. **Abordagem metodológica**: filtro por desenho de estudo (RCT, coorte, validação)

CADA substring deve usar OR entre sinônimos do mesmo conceito e AND entre conceitos distintos
(formato correto). NÃO use frases descritivas inventadas entre aspas — geram 0 resultados.

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

Responda APENAS com JSON válido neste formato — `search_strings.{source}` é uma LISTA de 6 strings:

```json
{
  "criteria_md": "<Markdown completo dos critérios — 4 Pilares + Quality Score>",
  "search_strings": {
    "pubmed": [
      "<string 1: PICO completo>",
      "<string 2: PICO sem comparador>",
      "<string 3: MeSH/canônicos>",
      "<string 4: sinônimos PT/EN>",
      "<string 5: recorte populacional>",
      "<string 6: abordagem metodológica>"
    ],
    "scielo": ["<6 strings com mesmas variações, em PT/ES priorizando termos populares>"],
    "scholar": ["<6 strings com aspas para frases exatas, sem MeSH>"]
  },
  "rationale": "<2-3 frases explicando as escolhas estratégicas e como os 6 ângulos se complementam>"
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


SYSTEM_PROMPT_SYSTEMATIC = """Você é um especialista sênior em metodologia científica e revisões sistemáticas (PRISMA 2020), capaz de adaptar critérios de elegibilidade para QUALQUER campo do conhecimento (saúde, educação, ciências sociais, engenharia, gestão, direito, etc.).

Sua tarefa: dado um tema, gerar um protocolo de revisão sistemática que entregue uma TRIAGEM REAL — combinando PRISMA 2020 (triagem ampla → qualidade → ranking) com RIGOR METODOLÓGICO ADAPTATIVO (validação externa, multi-centro, N robusto, baseline) cujos pesos variam conforme a maturidade do tema. Também produzir search strings prontas para PubMed, SciELO e Google Scholar.

# A REGRA DE OURO

> **Triagem ampla primeiro. Qualidade metodológica RIGOROSA no score. Ranking só no final.**

A qualidade metodológica clássica de revisão sistemática (validação externa independente, multi-centro,
N robusto, comparação com baseline, desenho prospectivo) é **PARTE CENTRAL** deste protocolo —
mas entra no **SCORE 0-100** com pesos ADAPTADOS à maturidade do tema, NÃO como guilhotina
inicial que zera a busca em temas emergentes.

# DETECÇÃO DE MATURIDADE DO TEMA (obrigatório — passo 1)

ANTES de gerar critérios, classifique a maturidade da literatura sobre o tema em UMA das três categorias:

- **high** — Literatura abundante e madura. Existem RCTs multicêntricos, meta-análises recentes,
  guidelines internacionais. Estudos de elite metodológica abundam. Exemplos: hipertensão arterial
  sistêmica, diabetes tipo 2, IA em mamografia, screening de câncer cervical, AVC isquêmico agudo.
- **moderate** — Pesquisa ativa mas heterogênea. Mistura de RCTs pequenos, coortes prospectivas,
  observacionais grandes. Validação externa existe mas não é a regra. Exemplos: telemedicina em
  cardiologia, microbiota e doenças metabólicas, biomarcadores em sepse, canabinoides em dor crônica.
- **emerging** — Tema novo ou nicho. Predominantemente retrospectivos, séries de casos, estudos
  preliminares. Validação externa rara. Exemplos: novas técnicas cirúrgicas minimamente invasivas,
  terapias gênicas raras, biomarcadores recém-descobertos.

Justifique a escolha em `rationale` e ADAPTE os critérios e pesos:

| Maturidade | Janela | Stage 2 piso | Stage 3 Específico (validação ext / multi-centro / N alto / baseline) |
|---|---|---|---|
| **high** | 3-5 anos | rigoroso (exige metodologia clara + N coerente + desfechos formais) | pesos ALTOS (cada um 12-15 pts) — total 50 |
| **moderate** | 5-10 anos | balanceado | pesos MÉDIOS (cada um 8-12 pts) — total 50 |
| **emerging** | 10-15 anos | mais permissivo (retrospectivos OK, séries de casos OK) | pesos BAIXOS (cada um 5-8 pts) + mais peso em "metodologia clara" e "relevância prática" — total 50 |

A regra essencial: em temas **maduros** (high), estudos sem validação externa, sem multi-centro,
com N pequeno e sem baseline ENTRAM (Stages 1+2 OK) mas ficam com Score Específico baixo (≤20/50)
— não chegam ao Top N. Em temas **emergentes**, esses mesmos atributos são raros e por isso
seus pesos são reduzidos no score, dando peso maior a clareza metodológica e relevância.

# FUNIL DE TRIAGEM EM 4 ESTÁGIOS

Os critérios devem ser organizados em 4 seções claras:

## Stage 1 — ELEGIBILIDADE DE ESCOPO (obrigatório, exclui se falha)
Critérios AMPLOS de "esse artigo fala do tema certo?":
- Tema central abordado (descrever o que conta)
- Idioma acessível (inglês, português, espanhol)
- Tipo de publicação primário (estudo com dados — exclui editoriais puros, cartas, abstracts de congresso)
- Texto completo OU abstract estruturado disponível
- Janela temporal proporcional à maturidade do tema (declarar)

## Stage 2 — PISO METODOLÓGICO MÍNIMO (obrigatório, mas amplo)
Garantir que o estudo é minimamente analisável — SEM exigir desenho específico:
- Metodologia descrita (qualquer desenho: retrospectivo, coorte, transversal, RCT, qualitativo)
- Amostra coerente com o desenho (sem cap fixo de N — apenas que o estudo justifique seu próprio tamanho)
- Pelo menos um desfecho/métrica relevante reportado
- Coerência interna entre objetivo, método, resultado e conclusão

## Stage 3 — QUALITY SCORE (0-100, NÃO exclui — só ranqueia)
O score tem DUAS sub-rubricas com peso 50/50:

### 3.1 Score Universal (50 pontos — aplicável a qualquer tema)
| Critério | Peso |
|---|---|
| Clareza da pergunta clínica/científica e definição da população | 10 |
| Adequação do desenho ao objetivo | 15 |
| Controle de viés / confundidores / análise estatística adequada | 15 |
| Atualidade e relevância prática | 10 |

### 3.2 Score Específico do Tema (50 pontos — pesos adaptados à maturidade)
4-6 critérios temáticos com pesos definidos por VOCÊ conforme o tema, totalizando 50 pontos.
Exemplos por domínio:
- **Saúde clínica**: validação externa (15) + comparação com baseline/padrão (15) + métricas
  primárias do desfecho (10) + tipo de estudo prospectivo/RCT (10)
- **Educação**: pré/pós-teste (15) + grupo controle (15) + retenção/aproveitamento (10) + tamanho da intervenção (10)
- **Engenharia/IA**: validação externa (15) + benchmark vs estado-da-arte (15) + dataset público/replicabilidade (10) + descrição completa do modelo (10)

**IMPORTANTE**: pesos do score específico devem REFLETIR a maturidade do tema:
- high: pesos altos em validação externa, multi-centro, RCT
- moderate: pesos equilibrados
- emerging: pesos altos em "metodologia clara" e "relevância", baixos em "elite metodológica"

## Stage 4 — RANKING TOP N (priorização, NUNCA exclui)
Após Stage 3, os artigos são ordenados por quality_score DESC. Os Top N (definidos pelo usuário)
recebem prioridade de leitura. Os demais incluídos continuam disponíveis na lista completa.

# REGRAS PARA SEARCH STRINGS

Você deve gerar **6 substrings (ângulos diferentes)** por source — não apenas 1 — cobrindo
ângulos semânticos distintos para maximizar a recuperação de literatura. Os 6 ângulos sugeridos:

  1. **PICO completo**: P + I + C + O todos juntos (busca rigorosa)
  2. **PICO sem comparador**: P + I + O (mais ampla, sem exigir grupo controle)
  3. **MeSH/keywords canônicos**: termos MeSH ou termos canônicos + filtros
  4. **Sinônimos PT/EN**: variações linguísticas e tradução de termos centrais
  5. **Recortes populacionais**: foco em subgrupos (idade, gênero, contexto)
  6. **Abordagem metodológica**: filtro por desenho de estudo (RCT, coorte, observacional)

CADA substring usa OR entre sinônimos do mesmo conceito e AND entre conceitos distintos.
NÃO use frases descritivas inventadas entre aspas — geram 0 resultados.

## PubMed (sem restringir Publication Type — não filtre estudos por tipo no Stage 1)
- Sintaxe: `("Termo MeSH"[Mesh] OR sinônimo) AND (filtros de data)`
- Limitar por data: `AND ("YYYY"[Date - Publication] : "YYYY"[Date - Publication])`

## SciELO
- Sintaxe simples: `(termo1 OR sinonimo) AND (termo2)`
- Idiomas: português, espanhol, inglês

## Google Scholar
- Frases curtas entre aspas
- Sem MeSH

# FORMATO DA SAÍDA

Responda APENAS com JSON válido neste formato:

```json
{
  "criteria_md": "<Markdown completo dos critérios — 4 ESTÁGIOS + Quality Score Universal + Específico>",
  "topic_maturity": "high" | "moderate" | "emerging",
  "search_strings": {
    "pubmed":  ["6 substrings — ângulos distintos"],
    "scielo":  ["6 substrings — ângulos distintos"],
    "scholar": ["6 substrings — ângulos distintos"]
  },
  "rationale": "<2-3 frases: maturidade detectada + ângulos das substrings + lógica do score específico>"
}
```

O `criteria_md` deve seguir EXATAMENTE esta estrutura (mantenha os títulos das 4 seções — o sistema valida por eles):

```markdown
# Protocolo de Triagem (Revisão Sistemática Padrão) — {tema}

## Maturidade do tema
{high|moderate|emerging} — {justificativa em 1-2 frases}

## Janela temporal recomendada
{ano_inicio}–{ano_fim} ({N} anos, proporcional à maturidade)

## 1. Elegibilidade de Escopo (obrigatória — exclui se falha) ✅

### 1.1 Tema central
- O artigo aborda diretamente {tema} ou subtemas claramente relacionados.

### 1.2 Tipo de publicação
- Estudo primário ou secundário com dados (RCT, coorte, caso-controle, transversal, qualitativo,
  revisão sistemática, meta-análise, guideline, consenso). Exclui editoriais puros, cartas ao editor,
  comentários sem dados, abstracts de congresso isolados.

### 1.3 Idioma
- Inglês, português ou espanhol.

### 1.4 Disponibilidade
- Texto completo OU abstract estruturado disponível.

### 1.5 Janela temporal
- Publicação entre {ano_inicio} e {ano_fim}. Estudos clássicos/fundacionais anteriores podem entrar se
  forem altamente citados ou conceitualmente indispensáveis.

## 2. Piso Metodológico Mínimo (obrigatório, mas amplo) ✅

### 2.1 Metodologia descrita
- O método é descrito (qualquer desenho aceito). NÃO exige RCT, validação externa ou multi-centro.

### 2.2 Amostra coerente com desenho
- O N é coerente com o tipo de estudo (sem cap fixo). Estudos pequenos com desenho justificado entram.

### 2.3 Métrica/desfecho relevante
- Pelo menos UM desfecho ou métrica relacionada ao tema é reportado.

### 2.4 Coerência interna
- Há coerência entre objetivo, método, resultados e conclusão.

## 3. Quality Score (0–100, NÃO exclui)

### 3.1 Universal (50 pts — aplicável a qualquer tema)
| Atributo | Peso |
|---|---|
| Clareza da pergunta científica / definição da população | 10 |
| Adequação do desenho ao objetivo | 15 |
| Controle de viés, confundidores, análise estatística adequada | 15 |
| Atualidade e relevância prática | 10 |

### 3.2 Específico do tema (50 pts — pesos adaptados à maturidade {high|moderate|emerging})
| Atributo específico | Peso |
|---|---|
| {critério 1 — peso conforme maturidade} | {peso} |
| {critério 2} | {peso} |
| {critério 3} | {peso} |
| {critério 4} | {peso} |

### Interpretação do score
- **80–100**: prioridade máxima de leitura.
- **60–79**: alta prioridade.
- **40–59**: incluído mas leitura secundária.
- **0–39**: incluído (passou Stage 1+2) mas baixa prioridade — só lido se tiver tempo.

## 4. Ranking Top N
- Os artigos incluídos são ordenados por quality_score DESC.
- Top N (definido pela meta do usuário) recebem prioridade.
- Os demais incluídos continuam acessíveis na lista completa — em revisão sistemática REAL todos os
  elegíveis devem ser considerados.
```

# CRITÉRIOS DE EXCLUSÃO (apenas Stage 1 + 2 — não excluir por qualidade no Stage 3)

Lembre-se: NÃO exclua por:
- Falta de validação externa
- N pequeno (se desenho justifica)
- Não ser RCT
- Não ser multi-centro
- Não ter comparação com baseline

Esses entram como pontos no SCORE, não como filtros.

EXCLUA apenas por:
- Tema fora do escopo
- Idioma fora dos 3 aceitos
- Editorial puro / carta / abstract sem dados
- Sem método descrito
- Sem nenhum desfecho/métrica relevante reportado
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

Você deve gerar **6 substrings (ângulos diferentes)** por source — não apenas 1 — cobrindo
ângulos semânticos distintos para maximizar a recuperação de literatura. Os 6 ângulos sugeridos
para artigo de resumo:

  1. **Tema central + subtemas principais** (busca ampla)
  2. **Conceitos correlatos**: termos próximos do tema (analogias, conceitos vizinhos)
  3. **Sinônimos PT/EN/ES**: variações linguísticas
  4. **Recorte populacional/contextual**: foco em subgrupo, contexto ou cenário
  5. **Abordagem por tipo de publicação**: revisões, guidelines, consensos
  6. **Desfechos / aplicações práticas**: termos de outcome, intervenção, implicações

CADA substring usa OR entre sinônimos do mesmo conceito e AND entre conceitos distintos.
NÃO use frases descritivas inventadas entre aspas — geram 0 resultados.

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

Responda APENAS com JSON válido neste formato — `search_strings.{source}` é uma LISTA de 6 strings:

```json
{
  "criteria_md": "<Markdown completo dos critérios — 4 Pilares narrativos + Quality Score narrativo>",
  "search_strings": {
    "pubmed": [
      "<string 1: tema central + subtemas>",
      "<string 2: conceitos correlatos>",
      "<string 3: sinônimos PT/EN/ES>",
      "<string 4: recorte populacional>",
      "<string 5: tipo de publicação>",
      "<string 6: desfechos/aplicações>"
    ],
    "scielo": ["<6 strings em PT/ES priorizando termos populares>"],
    "scholar": ["<6 strings com aspas para frases exatas>"]
  },
  "rationale": "<2-3 frases explicando como os 6 ângulos se complementam>"
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
SYSTEM_PROMPT = SYSTEM_PROMPT_SYSTEMATIC  # alias para retrocompat
SYSTEM_PROMPT_SYSTEMATIC_PADRAO = SYSTEM_PROMPT_SYSTEMATIC  # alias retrocompat (modo único agora)


REVIEW_TYPES = ("systematic_review", "narrative_review")


def _normalize_substrings(value) -> list[str]:
    """Aceita string única, lista, ou None e devolve lista limpa.

    Filtra strings vazias/curtas e remove duplicatas (mantendo ordem). Se a entrada
    foi uma string única (formato legacy), embrulha em [s] preservando retrocompat.
    """
    if value is None:
        return []
    if isinstance(value, str):
        s = value.strip()
        return [s] if len(s) >= 5 else []
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        s = item.strip()
        if len(s) < 5 or s.lower() in seen:
            continue
        seen.add(s.lower())
        out.append(s)
    return out


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15), reraise=True)
def run_discovery(client: OpenAI, model: str, *, topic: str,
                  objective: str | None = None, years_window: int = 10,
                  review_type: str = "systematic_review",
                  rigidity_mode: str = "padrao") -> DiscoveryResult:
    """Gera critérios + 6 substrings/source. `rigidity_mode` só aplica quando
    review_type='systematic_review':
      - 'padrao' (default): funil 4 estágios; qualidade vira ranking, não exclui
      - 'elite': qualidade obrigatória; comportamento histórico rigoroso
    """
    from datetime import date
    current_year = date.today().year
    start_year = current_year - years_window + 1
    objective_default = (
        "Reunir evidência para artigo de resumo / revisão narrativa"
        if review_type == "narrative_review"
        else "Revisão sistemática PRISMA com rigor metodológico adaptativo conforme maturidade do tema"
    )
    user = f"""**Tema:** {topic}
**Objetivo:** {objective or objective_default}

**ANO ATUAL: {current_year}**
**Janela temporal sugerida pelo usuário: últimos {years_window} anos = de {start_year} até {current_year}**

IMPORTANTE: as search strings DEVEM usar o intervalo {start_year}-{current_year}, NÃO use anos anteriores.
Para PubMed, use o filtro `("{start_year}"[Date - Publication] : "{current_year}"[Date - Publication])`.
Você PODE recomendar uma janela mais ampla no `criteria_md` se classificar o tema como emergente.

Gere o protocolo completo + LISTAS de 6 search strings (ângulos distintos) para os 3 portais."""

    if review_type == "narrative_review":
        system_prompt = SYSTEM_PROMPT_NARRATIVE
    else:
        # Sistemática agora é único modo (rigor adapta via topic_maturity).
        # `rigidity_mode` é mantido como coluna no DB pra retrocompat mas não roteia mais.
        system_prompt = SYSTEM_PROMPT_SYSTEMATIC
    import time
    t0 = time.monotonic()
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        temperature=0.4,
        # max_tokens=6000: precisa caber criteria_md (~2500) + 18 substrings × ~80 chars = ~1500 + rationale
        max_tokens=6000,
    )
    dt = int((time.monotonic() - t0) * 1000)
    raw = resp.choices[0].message.content or ""
    from .llm import _safe_parse_json
    parsed = _safe_parse_json(raw)
    usage = getattr(resp, "usage", None)
    ss = parsed.get("search_strings") if isinstance(parsed.get("search_strings"), dict) else {}
    raw_maturity = str(parsed.get("topic_maturity") or "").strip().lower()
    if raw_maturity not in {"high", "moderate", "emerging"}:
        raw_maturity = ""

    return DiscoveryResult(
        criteria_md=str(parsed.get("criteria_md") or ""),
        search_strings={
            "pubmed": _normalize_substrings(ss.get("pubmed")),
            "scielo": _normalize_substrings(ss.get("scielo")),
            "scholar": _normalize_substrings(ss.get("scholar")),
        },
        rationale=str(parsed.get("rationale") or ""),
        raw_response=raw,
        topic_maturity=raw_maturity,
        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
        duration_ms=dt,
        generation_id=getattr(resp, "id", "") or "",
    )


@dataclass
class RotateResult:
    # Multi-substring: substitutas vêm como LISTA por source (3-4 ângulos novos).
    strings: dict[str, list[str]]
    prompt_tokens: int = 0
    completion_tokens: int = 0
    duration_ms: int = 0
    generation_id: str = ""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15), reraise=True)
def rotate_search_strings(client: OpenAI, model: str, *, topic: str,
                          previous_strings_burned: dict | None = None,
                          previous_strings: dict | None = None,  # alias legacy
                          attempt: int = 1,
                          years_window: int = 5,
                          n_substitutes: int = 3) -> RotateResult:
    """Gera search strings ALTERNATIVAS substituindo strings que foram queimadas.

    Pede ao LLM `n_substitutes` strings novas por source com ângulo diferente:
    sinônimos, termos correlatos, traduções, abreviações. NÃO repete os termos
    queimados. Retorna `dict[source -> list[str]]`.

    `previous_strings` aceito como alias por retrocompat.
    """
    from datetime import date
    cy = date.today().year
    sy = cy - years_window + 1

    burned = previous_strings_burned if previous_strings_burned is not None else (previous_strings or {})
    # Aceita formato legacy (str única) por retrocompat
    norm_prev: dict[str, list[str]] = {}
    for k, v in burned.items():
        if isinstance(v, list):
            norm_prev[k] = v
        elif isinstance(v, str) and v:
            norm_prev[k] = [v]
        else:
            norm_prev[k] = []
    prev_summary = json.dumps(norm_prev, ensure_ascii=False, indent=2)

    system = f"""Você é especialista em recuperação de informação científica em PubMed, SciELO e Google Scholar.

Sua tarefa: sugerir {n_substitutes} search strings ALTERNATIVAS por source (substituindo strings queimadas que não trouxeram inclusões).

REGRAS CRÍTICAS (queries anteriores fracassaram POR ESTAREM HIPERESPECÍFICAS):
1. NÃO use frases descritivas entre aspas (ex: "Pediatric Echography for Dermatological Conditions").
   Essas frases NÃO EXISTEM literalmente na literatura — geram 0 resultados.
2. Use TERMOS SIMPLES e CURTOS conectados por OR. Ex: "ultrasound OR ultrasonography OR sonography".
3. Prefira palavras-chave canônicas: MeSH terms reais (ex: "Skin Diseases"[MeSH]),
   DeCS, palavras únicas em vez de frases inventadas.
4. Se a tentativa atual é alta (>5), AMPLIE drasticamente — use só 2-3 conceitos centrais.
5. NÃO repita termos das anteriores. Use sinônimos, traduções (en/pt/es), abreviações.
6. Cada uma das {n_substitutes} substrings DEVE cobrir um ângulo distinto entre si.
7. Para PubMed: combine MeSH com [tiab] (texto livre). NÃO encadeie ANDs em excesso.
8. Para SciELO: use vocabulário em PT primário, EN como fallback.
9. Para Scholar: queries CURTAS (5-10 palavras), sem operadores complexos.

EXEMPLO BOM (PubMed para tema "ultrassom dermatologia pediátrica"):
("Ultrasonography"[MeSH] OR ultrasound OR sonography) AND ("Skin Diseases"[MeSH] OR dermatology OR cutaneous) AND (child* OR pediatric OR paediatric)

Responda em JSON: {{"search_strings": {{"pubmed": ["s1",...,"s{n_substitutes}"], "scielo": [...], "scholar": [...]}}}}
"""
    aggressive_msg = ""
    if attempt >= 5:
        aggressive_msg = (
            "\n\n⚠️ TENTATIVA #{} — queries anteriores claramente estão muito específicas. "
            "Reduza drasticamente: use SÓ 2-3 conceitos centrais ligados por OR. "
            "Esqueça refinar — AMPLIE.".format(attempt)
        )
    user = f"""**Tema:** {topic}
**Tentativa de rotação:** #{attempt}
**Janela:** {sy}-{cy}
**Substitutas pedidas por source:** {n_substitutes}

**Search strings QUEIMADAS (retornaram 0 inclusões):**
```json
{prev_summary}
```
{aggressive_msg}
Gere {n_substitutes} search strings MAIS AMPLAS por source — termos simples conectados por OR.
Para PubMed inclua o filtro `("{sy}"[Date - Publication] : "{cy}"[Date - Publication])`.
Responda em JSON: {{"search_strings": {{"pubmed": [...{n_substitutes} strings...], "scielo": [...], "scholar": [...]}}}}"""

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
        max_tokens=2000,
    )
    dt = int((time.monotonic() - t0) * 1000)
    raw = resp.choices[0].message.content or "{}"
    from .llm import _safe_parse_json
    parsed = _safe_parse_json(raw)
    ss = parsed.get("search_strings") if isinstance(parsed.get("search_strings"), dict) else {}
    usage = getattr(resp, "usage", None)
    return RotateResult(
        strings={
            "pubmed": _normalize_substrings(ss.get("pubmed")),
            "scielo": _normalize_substrings(ss.get("scielo")),
            "scholar": _normalize_substrings(ss.get("scholar")),
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
    md = result.criteria_md or ""
    # Aceita 2 estruturas: clássica (Inclusão/Exclusão/Quality Score) ou Padrão (4 estágios)
    is_padrao = ("## 1. Elegibilidade" in md or "## 1. Elegibilidade de Escopo" in md)
    if is_padrao:
        expected_sections = [
            "## 1. Elegibilidade",  # de Escopo
            "## 2. Piso Metodológico",
            "## 3. Quality Score",
            "## 4. Ranking",
        ]
        for section in expected_sections:
            if section not in md:
                issues.append(f"Seção ausente (Padrão): {section}")
    else:
        expected_sections = ["## 1. Critérios de Inclusão", "## 2. Critérios de Exclusão", "## 3. Quality Score"]
        for section in expected_sections:
            if section not in md:
                issues.append(f"Seção ausente: {section}")
    for source, queries in result.search_strings.items():
        if not isinstance(queries, list) or len(queries) == 0:
            issues.append(f"Lista de search strings vazia para {source}")
            continue
        if len(queries) < MIN_SUBSTRINGS_PER_SOURCE:
            issues.append(
                f"{source}: só {len(queries)} substring(s) — esperado ≥ {MIN_SUBSTRINGS_PER_SOURCE}"
            )
        for i, qs in enumerate(queries):
            if not qs or len(qs) < 10:
                issues.append(f"{source}[{i}]: substring vazia/curta")
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

    # Geramos 3 variações programáticas em cada source (atende MIN_SUBSTRINGS_PER_SOURCE)
    pubmed_q1 = (
        f"({' AND '.join(f'{w}[Title/Abstract]' for w in keep[:4])}) "
        f'AND ("{sy}"[Date - Publication] : "{cy}"[Date - Publication])'
    )
    pubmed_q2 = (
        f"({' OR '.join(f'{w}[Title/Abstract]' for w in keep[:3])}) "
        f'AND ("{sy}"[Date - Publication] : "{cy}"[Date - Publication])'
    )
    pubmed_q3 = (
        f"{keep[0]}[Title/Abstract] "
        f'AND ("{sy}"[Date - Publication] : "{cy}"[Date - Publication])'
    )
    pubmed_list = [pubmed_q1, pubmed_q2, pubmed_q3]

    scielo_list = [
        " AND ".join(keep[:3]),
        " OR ".join(keep[:3]),
        keep[0],
    ]
    scholar_list = [
        " ".join(f'"{w}"' for w in keep[:3]),
        " OR ".join(f'"{w}"' for w in keep[:2]),
        f'"{keep[0]}"',
    ]

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
        search_strings={"pubmed": pubmed_list, "scielo": scielo_list, "scholar": scholar_list},
        rationale="Fallback automático — LLM indisponível, queries baseadas em keywords do tema.",
        raw_response="",
    )


def run_discovery_with_fallback(client: OpenAI, model: str, *, topic: str,
                                objective: str | None = None,
                                years_window: int = 5,
                                review_type: str = "systematic_review",
                                rigidity_mode: str = "padrao",
                                ) -> tuple[DiscoveryResult, bool]:
    """Tenta `run_discovery` (com retry interno × 3); se falhar, retorna fallback.

    Retorna (result, used_fallback). O caller pode checar `used_fallback` para logar
    aviso e gravar o status do projeto como "discovery em modo contingência".
    """
    try:
        result = run_discovery(client, model, topic=topic, objective=objective,
                               years_window=years_window, review_type=review_type,
                               rigidity_mode=rigidity_mode)
        # Mesmo com sucesso, valida se tem o mínimo viável (≥1 substring não-trivial em qualquer source)
        has_any_substring = any(
            isinstance(lst, list) and any(s and len(s) >= 5 for s in lst)
            for lst in result.search_strings.values()
        )
        if (result.criteria_md and len(result.criteria_md) >= 200 and has_any_substring):
            return result, False
        # Resposta lixo (curta demais ou listas todas vazias) → fallback
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
