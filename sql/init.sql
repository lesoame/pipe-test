CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. Tabela de Controle
CREATE TABLE IF NOT EXISTS analytics.sys_batch_log (
    file_name VARCHAR(255) PRIMARY KEY,
    batch_date DATE,
    file_type VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rows INT DEFAULT 0,
    valid_rows INT DEFAULT 0,
    invalid_rows INT DEFAULT 0
);

-- 2. Dimensão Lojas (SCD Type 1)
CREATE TABLE IF NOT EXISTS analytics.dim_stores (
    store_token UUID PRIMARY KEY,
    store_group VARCHAR(50),
    store_name VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Fato Vendas
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    transaction_id UUID,
    store_token UUID,
    receipt_token VARCHAR(50),
    transaction_time TIMESTAMP,
    amount NUMERIC(12,2),
    user_role VARCHAR(50),
    batch_date DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_token, transaction_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sales_time ON analytics.fact_sales(transaction_time);
CREATE INDEX IF NOT EXISTS idx_sales_batch ON analytics.fact_sales(batch_date);