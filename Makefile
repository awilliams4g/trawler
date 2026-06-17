# Root Makefile
.PHONY: up down build test smoke

up:
	docker compose up -d

down:
	docker compose down -v

build:
	$(MAKE) -C trawler build

test:
	$(MAKE) -C trawler test

smoke:
	$(MAKE) -C trawler smoke
