-- DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;

-- CREATE TABLE users (
--     id serial PRIMARY KEY,
--     username VARCHAR (150) NOT NULL,
--     first_name VARCHAR (50) NOT NULL,
--     last_name VARCHAR (50) NOT NULL,
--     password TEXT NOT NULL,
--     create_date DATE DEFAULT CURRENT_TIMESTAMP
-- );

CREATE TABLE transactions (
    id serial PRIMARY KEY,
    user_id INTEGER NOT NULL,
    transaction_date DATE NOT NULL,
    description VARCHAR (50) NOT NULL,
    category VARCHAR (50) NOT NULL,
    amount FLOAT8 NOT NULL,
    unique_record_id TEXT NOT NULL,
    create_date DATE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
);