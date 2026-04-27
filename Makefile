# Vicky — utilitários de Postgres + seed.
#
# Uso típico:
#   make pg-up        # sobe o container Postgres em background
#   make pg-init      # aplica schema_pg.sql num banco vazio
#   make seed         # ETL do SQLite (vicky/vicky.db) → Postgres
#   make pg-fresh     # reset total + schema + seed
#
# Override de variáveis pela linha de comando:
#   make seed SQLITE=vicky/vicky.db.backup-pre-multitenancy
#   make seed DATABASE_URL=postgresql://outro:senha@host:5432/db

PYTHON          ?= .venv/bin/python
VICKY           ?= .venv/bin/vicky
COMPOSE         ?= docker compose
DB_USER         ?= vicky
DB_PASS         ?= vicky_dev
DB_NAME         ?= vicky
DB_HOST         ?= localhost
DB_PORT         ?= 5433
DATABASE_URL    ?= postgresql://$(DB_USER):$(DB_PASS)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)
SQLITE          ?= vicky/vicky.db

VPS_USER        ?= synaptha
VPS_HOST        ?= 72.61.128.48
VPS_PATH        ?= ~/Vicky
PROD_COMPOSE    ?= docker compose -f compose.prod.yml --env-file .env.prod

export DATABASE_URL

.DEFAULT_GOAL := help

.PHONY: help pg-up pg-down pg-restart pg-logs pg-shell pg-status pg-wait \
        pg-reset pg-init pg-drop seed seed-truncate pg-fresh psql \
        deploy prod-logs prod-status prod-restart

## help: Lista os comandos disponíveis
help:
	@awk 'BEGIN{FS=":.*?## "} /^## / {sub(/^## /,""); print}' $(MAKEFILE_LIST) | column -t -s ':'

## pg-up: Sobe o container Postgres em background
pg-up:
	$(COMPOSE) up -d postgres
	@$(MAKE) -s pg-wait

## pg-down: Para o container (mantém o volume de dados)
pg-down:
	$(COMPOSE) stop postgres

## pg-restart: Reinicia o container
pg-restart: pg-down pg-up

## pg-logs: Acompanha logs do Postgres (Ctrl+C pra sair)
pg-logs:
	$(COMPOSE) logs -f postgres

## pg-status: Mostra se o container está saudável
pg-status:
	@$(COMPOSE) ps postgres

## pg-wait: Aguarda o Postgres aceitar conexões (com timeout)
pg-wait:
	@printf "Aguardando Postgres em $(DB_HOST):$(DB_PORT)..."
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T postgres pg_isready -U $(DB_USER) -d $(DB_NAME) >/dev/null 2>&1; then \
			echo " pronto."; exit 0; \
		fi; \
		printf "."; sleep 1; \
	done; \
	echo " TIMEOUT."; exit 1

## psql: Abre psql conectado ao banco
psql:
	$(COMPOSE) exec postgres psql -U $(DB_USER) -d $(DB_NAME)

## pg-shell: Abre shell sh dentro do container
pg-shell:
	$(COMPOSE) exec postgres sh

## pg-reset: APAGA todos os dados (volume) e sobe banco zerado. Pede confirmação.
pg-reset:
	@printf "⚠  Isso vai APAGAR todos os dados do Postgres local. Continuar? [y/N] "; \
	read ans; [ "$$ans" = "y" ] || { echo "abortado."; exit 1; }
	$(COMPOSE) down -v
	$(COMPOSE) up -d postgres
	@$(MAKE) -s pg-wait

## pg-drop: TRUNCATE em todas as tabelas (preserva schema). Pede confirmação.
pg-drop:
	@printf "⚠  TRUNCATE em todas as tabelas. Continuar? [y/N] "; \
	read ans; [ "$$ans" = "y" ] || { echo "abortado."; exit 1; }
	$(COMPOSE) exec -T postgres psql -U $(DB_USER) -d $(DB_NAME) -c \
		"TRUNCATE users, workspaces, projects, articles, analyses, double_checks, user_decisions, jobs, llm_usage RESTART IDENTITY CASCADE;"

## pg-init: Aplica src/vicky/schema_pg.sql no banco (idempotente)
pg-init:
	@$(MAKE) -s pg-wait
	$(PYTHON) -c "from vicky.db import init_pg_schema; init_pg_schema()"
	@echo "✓ schema aplicado em $(DATABASE_URL)"

## seed: Carrega dados do SQLite ($(SQLITE)) no Postgres (não trunca antes)
seed:
	@test -f "$(SQLITE)" || { echo "ERRO: $(SQLITE) não existe. Use SQLITE=caminho/do/.db"; exit 2; }
	@$(MAKE) -s pg-wait
	$(PYTHON) scripts/migrate_to_pg.py --sqlite "$(SQLITE)"

## seed-truncate: Como seed, mas TRUNCATE antes (idempotente, sem confirmação)
seed-truncate:
	@test -f "$(SQLITE)" || { echo "ERRO: $(SQLITE) não existe. Use SQLITE=caminho/do/.db"; exit 2; }
	@$(MAKE) -s pg-wait
	$(PYTHON) scripts/migrate_to_pg.py --sqlite "$(SQLITE)" --truncate

## pg-fresh: pg-reset + pg-init + seed (workflow completo "do zero")
pg-fresh: pg-reset pg-init seed
	@echo "✓ Postgres limpo, schema aplicado e dados carregados de $(SQLITE)."

## deploy: git pull + rebuild + restart na VPS de produção
deploy:
	@echo "→ Conectando em $(VPS_USER)@$(VPS_HOST):$(VPS_PATH)..."
	@ssh -t $(VPS_USER)@$(VPS_HOST) ' \
		set -e; \
		cd $(VPS_PATH); \
		echo "→ git pull..."; git pull; \
		echo "→ docker compose up -d --build..."; \
		$(PROD_COMPOSE) up -d --build; \
		echo "→ Status:"; $(PROD_COMPOSE) ps; \
		echo "✓ Deploy concluído."'

## prod-logs: Tail dos logs do vicky-web em produção (Ctrl+C pra sair)
prod-logs:
	@ssh -t $(VPS_USER)@$(VPS_HOST) 'cd $(VPS_PATH) && $(PROD_COMPOSE) logs -f --tail=50 vicky-web'

## prod-status: Mostra status dos containers de produção
prod-status:
	@ssh -t $(VPS_USER)@$(VPS_HOST) 'cd $(VPS_PATH) && $(PROD_COMPOSE) ps'

## prod-restart: Reinicia o vicky-web em produção (sem rebuild)
prod-restart:
	@ssh -t $(VPS_USER)@$(VPS_HOST) 'cd $(VPS_PATH) && $(PROD_COMPOSE) restart vicky-web'
