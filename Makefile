.PHONY: up down build test seed logs clean benchmark-auth

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

test:
	docker compose --profile testing run --rm test

benchmark-auth:
	docker compose --profile testing run --rm test python scripts/benchmark_token_revocation.py

seed:
	docker compose run --rm migrate python -m app.auth.seed

logs:
	docker compose logs -f

clean:
	docker compose down -v
