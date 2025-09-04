-- migrations/versions/002_create_deliveries.up.sql
-- Создание таблицы доставки
CREATE TABLE IF NOT EXISTS deliveries
(
    order_uid VARCHAR(255) PRIMARY KEY NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    name      VARCHAR(255),
    phone     VARCHAR(50),
    zip       VARCHAR(20),
    city      VARCHAR(100),
    address   TEXT,
    region    VARCHAR(100),
    email     VARCHAR(100)
    );