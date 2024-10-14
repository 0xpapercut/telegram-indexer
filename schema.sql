CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    is_bot BOOLEAN,
    is_premium BOOLEAN,
    is_scam BOOLEAN,
    is_fake BOOLEAN,
    is_verified BOOLEAN
);

CREATE TABLE chats (
    chat_id BIGINT PRIMARY KEY,
    title TEXT,
    is_group BOOLEAN,
    is_channel BOOLEAN,
    is_user BOOLEAN,
    latest_historical_message BIGINT DEFAULT NULL
    -- FOREIGN KEY (latest_historical_message) REFERENCES messages(message_id)
);

CREATE TABLE messages (
    message_id BIGINT,
    user_id BIGINT NULL,
    chat_id BIGINT,
    text TEXT,
    date TIMESTAMP,
    is_historical BOOLEAN,
    insertion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, message_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE chats_participants_count(
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT,
    participants_count BIGINT,
    date TIMESTAMP,
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
)
