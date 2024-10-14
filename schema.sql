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
    latest_historical_message_id message_id BIGINT NULL,
);

CREATE TABLE messages (
    message_id BIGINT,
    sender_id BIGINT,
    chat_id BIGINT,
    text TEXT,
    date TIMESTAMP,
    is_historical BOOLEAN,
    insertion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, message_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
);

CREATE TABLE chats_participants_count(
    id BIGSERIAL PRIMARY KEY,
    chat_id BIGINT,
    participants_count BIGINT,
    insertion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
);

CREATE VIEW messages_with_details AS
SELECT
    m.message_id,
    m.text,
    u.username,
    c.title AS chat_title,
    m.date
FROM
    messages m
JOIN
    users u ON m.sender_id = u.user_id
JOIN
    chats c ON m.chat_id = c.chat_id
ORDER BY
    m.date DESC;
