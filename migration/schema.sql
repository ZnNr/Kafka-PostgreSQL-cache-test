--Таблица заказов (orders)
DROP TABLE IF EXISTS orders cascade;
CREATE TABLE IF NOT EXISTS orders
(
    order_uid          VARCHAR(255) PRIMARY KEY not null,
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

--Таблица доставки (deliveries)
CREATE TABLE IF NOT EXISTS deliveries
(
    order_uid     VARCHAR(255) PRIMARY KEY NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    name      VARCHAR(255),
    phone     VARCHAR(50),
    zip       VARCHAR(20),
    city      VARCHAR(100),
    address   TEXT,
    region    VARCHAR(100),
    email     VARCHAR(100)
    );

--Таблица платежа (payments)
CREATE TABLE IF NOT EXISTS payments
(
    order_uid     VARCHAR(255) PRIMARY KEY NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    transaction   VARCHAR(255),
    request_id    VARCHAR(255),
    currency      VARCHAR(10),
    provider      VARCHAR(50),
    amount        INTEGER,
    payment_dt    BIGINT,
    bank          VARCHAR(50),
    delivery_cost DECIMAL(10, 2),
    goods_total   DECIMAL(10, 2),
    custom_fee    DECIMAL(10, 2)

    );

--Таблица товаров (items)
CREATE TABLE IF NOT EXISTS items
(
    order_uid    VARCHAR(255)   NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    chrt_id      BIGINT         NOT NULL PRIMARY KEY,
    track_number VARCHAR(255),
    price        DECIMAL(10, 2) NOT NULL,
    rid          VARCHAR(255),
    name         VARCHAR(255)   NOT NULL,
    sale         DECIMAL(5, 2),
    size         VARCHAR(50),
    total_price  DECIMAL(10, 2) NOT NULL,
    nm_id        INTEGER,
    brand        VARCHAR(100),
    status       INTEGER
    );