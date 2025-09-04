-- migrations/versions/005_add_indexes.down.sql
-- Дополнительные индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_deliveries_city ON deliveries(city);
CREATE INDEX IF NOT EXISTS idx_payments_provider ON payments(provider);
CREATE INDEX IF NOT EXISTS idx_items_nm_id ON items(nm_id);