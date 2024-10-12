CREATE TABLE messages (
    message_id INTEGER,
    user_id INTEGER NULL,
    chat_id INTEGER,
    text TEXT,
    date TIMESTAMP,
    insertion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chat_id, message_id),
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE chats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    is_group BOOLEAN,
    is_channel BOOLEAN,
    is_user BOOLEAN,
    message_id_cursor INTEGER DEFAULT NULL,
    FOREIGN KEY (message_id_cursor) REFERENCES messages(message_id)
);

CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    is_bot BOOLEAN,
    is_premium BOOLEAN,
    is_scam BOOLEAN,
    is_fake BOOLEAN,
    is_verified BOOLEAN
);

CREATE TABLE chats_participants_count(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    participants_count INTEGER,
    api_time TIMESTAMP,
    FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
)

-- CREATE TABLE chats_users(
--     chat_id INTEGER,
--     user_id INTEGER,
--     FOREIGN KEY (chat_id) REFERENCES chats(chat_id),
--     FOREIGN KEY (user_id) REFERENCES users(user_id),
--     PRIMARY KEY (chat_id, user_id)
-- );

-- CREATE TABLE chats_user_count (
--     chat_id INTEGER PRIMARY KEY,
--     user_count INTEGER,
--     api_time TIMESTAMP,
--     insertion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (chat_id) REFERENCES chats(chat_id)
-- )

-- CREATE TABLE chat_actions (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     chat_id INTEGER,
--     user_id INTEGER,
--     api_time TIMESTAMP,
--     event TEXT,
--     insertion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );
