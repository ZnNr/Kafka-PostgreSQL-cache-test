-- migrations/versions/004_create_items.up.sql
-- Создание таблицы товаров
CREATE TABLE IF NOT EXISTS items
(
    order_uid    VARCHAR(255)   NOT NULL REFERENCES orders (order_uid) ON DELETE CASCADE,
    chrt_id      BIGINT         NOT NULL,
    track_number VARCHAR(255),
    price        DECIMAL(10, 2) NOT NULL,
    rid          VARCHAR(255),
    name         VARCHAR(255)   NOT NULL,
    sale         DECIMAL(5, 2),
    size         VARCHAR(50),
    total_price  DECIMAL(10, 2) NOT NULL,
    nm_id        BIGINT,
    brand        VARCHAR(100),
    status       INTEGER,
    PRIMARY KEY (order_uid, chrt_id)
    );

CREATE INDEX IF NOT EXISTS idx_items_brand ON items(brand);
CREATE INDEX IF NOT EXISTS idx_items_status ON items(status);