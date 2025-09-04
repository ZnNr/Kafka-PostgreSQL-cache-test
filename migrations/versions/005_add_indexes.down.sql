-- migrations/versions/005_add_indexes.down.sql
DROP INDEX IF EXISTS idx_deliveries_city;
DROP INDEX IF EXISTS idx_payments_provider;
DROP INDEX IF EXISTS idx_items_nm_id;