all:
	docker exec -it scraping python backtest.py
data:
	docker exec -it scraping python scraping.py
build:
	docker-compose build --no-cache
install:
	docker-compose build
up:
	docker-compose up -d
ps:
	docker-compose ps
version:
	docker exec -it scraping python --version
down:
	docker-compose down
bash:
	docker-compose exec scraping bash
ls:
	docker container ls
