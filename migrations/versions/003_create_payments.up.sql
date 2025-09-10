-- migrations/versions/003_create_payments.up.sql
-- Создание таблицы платежей
CREATE TABLE IF NOT EXISTS payments
(
    order_uid     VARCHAR(255) PRIMARY KEY NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    transaction   VARCHAR(255),
    request_id    VARCHAR(255),
    currency      VARCHAR(10),
    provider      VARCHAR(50),
    amount        DECIMAL(10, 2),
    payment_dt    BIGINT,
    bank          VARCHAR(50),
    delivery_cost DECIMAL(10, 2),
    goods_total   DECIMAL(10, 2),
    custom_fee    DECIMAL(10, 2)
    );