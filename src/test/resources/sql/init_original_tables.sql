CREATE TABLE IF NOT EXISTS users
(
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL,
    birth_date DATE,
    email      TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS products
(
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    price      DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders
(
    id         TEXT PRIMARY KEY,
    user_id    UUID NOT NULL,
    product_id INT  NOT NULL,
    quantity   INT CHECK (quantity > 0),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
);

CREATE TABLE IF NOT EXISTS logs
(
    id         SERIAL PRIMARY KEY,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_type TEXT CHECK (event_type IN ('INFO', 'WARN', 'ERROR')),
    message    TEXT
);

CREATE TABLE IF NOT EXISTS payments
(
    id       BIGSERIAL PRIMARY KEY,
    order_id TEXT           NOT NULL,
    amount   DECIMAL(10, 2) NOT NULL,
    paid_at  TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES orders (id)
);
