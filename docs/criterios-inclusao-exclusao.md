# Protocolo Refinado de Seleção — Operação Top 40

## Objetivo

Selecionar **40 artigos de elite** sobre Deep Learning aplicado a Mamografia para a iniciação científica da Victoria. O afunilamento é metodológico (não "a dedo"): critérios de alta exigência derrubam a maior parte dos candidatos automaticamente.

---

## 1. Critérios de Inclusão (DEVE atender TODOS) ✅

Para incluir, o estudo precisa cumprir **TODOS** os requisitos abaixo:

### 1.1 Padrão-Ouro de Validação
- **Validação Externa Independente obrigatória.** O modelo deve ter sido testado em um banco de dados diferente daquele em que foi treinado (ex: treinado nos EUA, validado na Suécia).
- Aceitar termos como: "external validation", "independent cohort", "multi-institutional validation", "external test set", "international validation".

### 1.2 Recência Tecnológica
- Publicações entre **janeiro de 2021 e abril de 2026**.
- *Justificativa:* Modelos pré-2021 (CNNs antigas) já são considerados obsoletos perante Transformers e arquiteturas híbridas modernas.

### 1.3 Robustez Amostral (Big Data)
- N (número de exames) **superior a 1.000 mamografias**.
- Para estudos de **rastreio populacional**, dar preferência a N > 5.000.
- Se o abstract não mencionar o N explicitamente mas for um estudo multi-institucional, dar benefício da dúvida.

### 1.4 Métricas Clínicas Reais
- Reportar **AUC** (área sob a curva) **+** pelo menos **2 dos seguintes** desfechos clínicos:
  1. **CDR** (Cancer Detection Rate / Taxa de Detecção de Câncer)
  2. **Recall Rate / RNC** (Taxa de Recall / Reconvocação)
  3. **Câncer de Intervalo** (cânceres que surgem entre rastreios)
- Sensibilidade e Especificidade isoladas NÃO contam como desfechos clínicos — precisa de pelo menos uma das três métricas acima.

### 1.5 Interação Humano-IA
- Estudos que **comparem ou integrem a IA ao fluxo de trabalho médico**:
  - IA como suporte ao radiologista
  - IA como segundo leitor (second reader)
  - IA como triagem autônoma (autonomous triage)
  - IA vs leitura única ou dupla por radiologista
- Estudos puramente algorítmicos sem componente clínico-comparativo NÃO contam.

---

## 2. Critérios de Exclusão (QUALQUER um já exclui) ❌

### 2.1 Vício de Dataset Público
- Estudos que usem **EXCLUSIVAMENTE** bancos públicos antigos (DDSM, MIAS, BCDR, INbreast) **sem validação em coorte clínica real**.
- *Motivo:* Esses bancos estão "viciados" — a comunidade já saturou treinamento neles e os resultados não refletem a prática médica de 2026.

### 2.2 Arquiteturas Ultrapassadas
- CAD (Computer-Aided Detection) clássico **sem Deep Learning**.
- Métodos de feature engineering manual (LBP, HOG, GLCM puros, SVM clássico).

### 2.3 Foco Estritamente Matemático
- Artigos em revistas de engenharia/computação que foquem só em **otimização de código** (ex: "melhoramos a perda de entropia em 0.01%") **sem desfecho clínico**.

### 2.4 Baixa Qualidade Metodológica
- Relatos de caso, séries de casos com **<100 pacientes**.
- Editoriais, cartas ao editor, comentários.
- Revisões narrativas, scoping reviews, revisões sistemáticas (usamos pra achar fontes, não pra incluir).
- Resumos de anais de congressos (sem paper completo).

### 2.5 Single-View Analysis
- Estudos que analisem **apenas uma incidência isolada** (ex: só CC ou só MLO) sem considerar o exame completo (CC + MLO).

### 2.6 Outras Modalidades Sem Mamografia
- Estudos de **só ultrassom**, **só MRI**, **só BSGI**, **só tomografia** — sem mamografia associada.
- Estudos multimodais que usam mamografia + outras modalidades CONTAM se a mamografia for componente principal.

### 2.7 Foco Não-Diagnóstico
- Estudos que usam mamografia para outros desfechos (ex: idade biológica, calcificação coronariana, espessura de mama) **sem foco em câncer de mama**.

### 2.8 Idioma
- Idiomas que não sejam **Inglês, Português ou Espanhol**.

---

## 3. Quality Score (0-100)

Para os artigos que passam por todos os filtros de inclusão, atribuir um **score numérico de qualidade** baseado em:

| Atributo | Peso máximo |
|---|---|
| Validação externa multi-institucional/internacional | 25 |
| N > 10.000 exames (vs N entre 1k–10k) | 20 |
| Reporta CDR + Recall + Câncer de Intervalo (3 desfechos) | 20 |
| Estudo prospectivo / ensaio clínico randomizado | 15 |
| Comparação direta com radiologistas (não só métricas) | 10 |
| Publicação 2024-2026 (vs 2021-2023) | 10 |

O score serve para **ranquear** dentro do pool de incluídos e selecionar os Top 40 quando o filtro deixa mais que 40 candidatos.

---

## 4. Estratégia de Triagem

1. **Hard filter** (binário): aplica os critérios de inclusão/exclusão acima → produz pool de "incluídos elegíveis"
2. **Score** (0-100): ranqueia o pool
3. **Top 40** = os 40 com maior score
