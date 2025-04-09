
clean:
	rm -rf commoncrawl/*
	rm -rf output/*
	rm -rf logs/*

install-local:
	uv venv
	uv sync

db:
	docker-compose up -d postgres

run-airflow:
	docker-compose up -d --build --force-recreate