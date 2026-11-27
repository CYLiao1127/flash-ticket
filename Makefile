.PHONY: up down build test seed logs clean benchmark-auth sample-data

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

sample-data:
	python3 scripts/generate_sample_seats.py --rows 100 --format both

seed:
	docker compose run --rm migrate python -m app.auth.seed

logs:
	docker compose logs -f

clean:
	docker compose down -v
