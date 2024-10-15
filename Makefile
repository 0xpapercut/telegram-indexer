all:
	python3 -m src.main

drop_db:
	dropdb telegram_indexer

setup_db:
	createdb telegram_indexer
	psql $(DSN) -f schema.sql
