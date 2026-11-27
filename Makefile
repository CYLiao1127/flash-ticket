.PHONY: up down build test seed logs clean benchmark-auth sample-data loadtest-prepare loadtest-a loadtest-b verify-consistency

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

loadtest-prepare:
	docker compose exec -T api-1 python loadtests/prepare_loadtest.py --users 3000 --seats 10000

loadtest-a:
	docker run --rm --network flash-ticket_default -v "$(PWD)/loadtests:/loadtests" -e GOGC=20 -e TARGET_VUS=3000 -e DURATION=60s grafana/k6 run --discard-response-bodies /loadtests/scenario_a_booking.js

loadtest-b:
	docker run --rm --network flash-ticket_default -v "$(PWD)/loadtests:/loadtests" -e GOGC=20 -e TARGET_VUS=2000 -e HOLD_SECONDS=20 grafana/k6 run /loadtests/scenario_b_websocket.js

verify-consistency:
	docker compose exec -T api-1 python scripts/verify_consistency.py
