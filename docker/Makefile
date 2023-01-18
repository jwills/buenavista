#! make

build:
	docker build -t buenavista -f Dockerfile ..

up:
	docker-compose up -d

example-data:
	@./download_data.sh
