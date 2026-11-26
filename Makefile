.PHONY: up down build test seed logs clean

up:
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

test:
	docker compose --profile testing run --rm test

seed:
	docker compose run --rm migrate python -m app.auth.seed

logs:
	docker compose logs -f

clean:
	docker compose down -v
