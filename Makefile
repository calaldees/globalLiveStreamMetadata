up:
	docker compose up
run:
	# docker compose up -d nanomq
	uv run app.py
test:
	uv run --dev pytest --doctest-modules
