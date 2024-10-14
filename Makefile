all:
	python3 -m src.main

drop_db:
	dropdb telegram

setup_db:
	createdb telegram
	psql $(DSN) -f schema.sql
