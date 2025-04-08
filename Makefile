
clean:
	rm -rf commoncrawl/*
	rm -rf output/*
	rm -rf logs/*

install-local:
	uv venv
	uv sync

run:
	docker-compose up -d