-- ============================================================
--  DATAMART S.A.S. — Repositorio Analítico
--  Modelo estrella: una tabla de hechos central + 3 dimensiones
-- ============================================================

-- ── Dimensión: Productos ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_products (
    product_code        VARCHAR(20)     PRIMARY KEY,
    canonical_name      VARCHAR(255)    NOT NULL,
    category            VARCHAR(100)    NOT NULL DEFAULT 'UNCATEGORIZED',
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Dimensión: Clientes ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id         VARCHAR(20)     PRIMARY KEY,
    country             VARCHAR(100),
    is_anonymous        BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Dimensión: Tiempo ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_id             DATE            PRIMARY KEY,
    year                SMALLINT        NOT NULL,
    quarter             SMALLINT        NOT NULL,
    month               SMALLINT        NOT NULL,
    month_name          VARCHAR(20)     NOT NULL,
    week                SMALLINT        NOT NULL,
    day                 SMALLINT        NOT NULL,
    day_of_week         SMALLINT        NOT NULL,
    is_weekend          BOOLEAN         NOT NULL
);

-- ── Hechos: Transacciones (tabla central del esquema estrella) ───────────────
-- Contiene tanto ventas (SALE) como devoluciones (RETURN) en una sola tabla.
-- El revenue es negativo para devoluciones, lo que permite calcular el neto
-- con un simple SUM(gross_revenue).
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      BIGSERIAL       PRIMARY KEY,
    invoice_no          VARCHAR(20)     NOT NULL,
    product_code        VARCHAR(20)     NOT NULL REFERENCES dim_products(product_code),
    customer_id         VARCHAR(20)     NOT NULL REFERENCES dim_customers(customer_id),
    date_id             DATE            NOT NULL REFERENCES dim_date(date_id),
    invoice_date        TIMESTAMPTZ     NOT NULL,
    quantity            INTEGER         NOT NULL,
    unit_price          NUMERIC(10, 4)  NOT NULL,
    gross_revenue       NUMERIC(14, 4)  NOT NULL,   -- positivo=venta, negativo=devolución
    transaction_type    VARCHAR(10)     NOT NULL,   -- 'SALE' | 'RETURN'
    country             VARCHAR(100),
    source              VARCHAR(50)     NOT NULL,
    loaded_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transaction UNIQUE (invoice_no, product_code, source)
);

-- ── Métricas: Revenue neto diario por producto ───────────────────────────────
CREATE TABLE IF NOT EXISTS agg_daily_revenue (
    date_id             DATE            NOT NULL REFERENCES dim_date(date_id),
    product_code        VARCHAR(20)     NOT NULL REFERENCES dim_products(product_code),
    gross_revenue       NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    return_amount       NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    net_revenue         NUMERIC(14, 4)  NOT NULL DEFAULT 0,
    total_sales_qty     INTEGER         NOT NULL DEFAULT 0,
    total_return_qty    INTEGER         NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_id, product_code)
);

-- ── Log de registros rechazados ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS reject_log (
    reject_id           BIGSERIAL       PRIMARY KEY,
    source              VARCHAR(50)     NOT NULL,
    raw_invoice_no      VARCHAR(50),
    raw_product_code    VARCHAR(50),
    raw_data            TEXT,
    reject_reason       VARCHAR(255)    NOT NULL,
    pipeline_run_date   DATE            NOT NULL,
    logged_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ── Índices ───────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_trx_date        ON fact_transactions(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_trx_product     ON fact_transactions(product_code);
CREATE INDEX IF NOT EXISTS idx_fact_trx_customer    ON fact_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_trx_type        ON fact_transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_agg_revenue_date     ON agg_daily_revenue(date_id);
CREATE INDEX IF NOT EXISTS idx_reject_log_source    ON reject_log(source);

-- ── Cliente anónimo seed ──────────────────────────────────────────────────────
INSERT INTO dim_customers (customer_id, country, is_anonymous)
VALUES ('ANONYMOUS', NULL, TRUE)
ON CONFLICT (customer_id) DO NOTHING;
