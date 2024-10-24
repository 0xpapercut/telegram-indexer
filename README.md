# telegram-indexer
Index Telegram messages with PostgreSQL.

## Disclaimer
This is an experimental project, use at your own risk.

I highlight that all indexed data is stored **unencrypted** (!).

## Setup
1. Define the `API_HASH`, `API_ID` (Telegram) and `DSN` (PostgreSQL) environment variables.
2. `pip install -r requirements.txt`
3. `python -m src.main`

As a bonus, it also opens up a websocket that relays new messages in real time.
