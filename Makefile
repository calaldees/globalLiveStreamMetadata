up:
	docker compose up --build
run:
	# docker compose up -d nanomq
	uv run app.py
test:
	uv run --dev pytest --doctest-modules
debug:
	uv run -m pdb app.py
clean:
	docker compose down
cloc:
	cloc --vcs=git
