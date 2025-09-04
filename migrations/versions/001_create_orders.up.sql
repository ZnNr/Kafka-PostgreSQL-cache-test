-- migrations/versions/001_create_orders.up.sql
-- Создание таблицы заказов
CREATE TABLE IF NOT EXISTS orders
(
    order_uid          VARCHAR(255) PRIMARY KEY NOT NULL,
    track_number       VARCHAR(255),
    entry              TEXT,
    locale             VARCHAR(10),
    internal_signature TEXT,
    customer_id        VARCHAR(255),
    delivery_service   VARCHAR(255),
    shardkey           VARCHAR(255),
    sm_id              INT,
    date_created       TIMESTAMP,
    oof_shard          VARCHAR(255)
    );

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date_created ON orders(date_created);