
clean:
	rm -rf commoncrawl/*
	rm -rf output/*
	rm -rf logs/*

install-local:
	uv venv
	uv sync

db:
	docker-compose up -d postgres

prepare:
	@echo "Creating and fixing permissions for bind-mounts..."
	sudo mkdir -p /tmp/commoncrawl
	sudo chown -R $(shell id -u):$(shell id -g) /tmp/commoncrawl

run-airflow: prepare
	@echo "Exporting UID..."
	export UID=$(shell id -u) && \
	docker-compose up -d --build --force-recreate