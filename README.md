# telegram-indexer
Index Telegram messages with PostgreSQL.

As a bonus, it also opens up a websocket at allowing other applications to listen to Telegram messages in real time.

## Setup
1. Define the `API_HASH` and `API_ID` environment variables.
2. `pip install -r requirements.txt`
3. `python -m src.main`
