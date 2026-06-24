import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

CONN_ID = "datamart_dw"


def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)


# ── Dimensión: Productos ──────────────────────────────────────────────────────

def load_dim_products(df: pd.DataFrame) -> None:
    """Upsert de productos únicos con nombre canónico y categoría."""
    products = (
        df[["stock_code", "description", "category"]]
        .drop_duplicates(subset=["stock_code"])
        .rename(columns={"stock_code": "product_code", "description": "canonical_name"})
    )
    hook = _get_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO dim_products (product_code, canonical_name, category)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_code)
        DO UPDATE SET canonical_name = EXCLUDED.canonical_name,
                      category       = EXCLUDED.category,
                      updated_at     = NOW()
    """
    rows = list(products.itertuples(index=False, name=None))
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    log.info("dim_products: %d productos cargados/actualizados", len(rows))


# ── Dimensión: Clientes ───────────────────────────────────────────────────────

def load_dim_customers(df: pd.DataFrame) -> None:
    """Upsert de clientes únicos. ANONYMOUS ya existe por seed."""
    customers = df[["customer_id", "country"]].drop_duplicates(subset=["customer_id"])
    hook = _get_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO dim_customers (customer_id, country, is_anonymous)
        VALUES (%s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """
    rows = [
        (row.customer_id, row.country, row.customer_id == "ANONYMOUS")
        for row in customers.itertuples(index=False)
    ]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    log.info("dim_customers: %d clientes cargados", len(rows))


# ── Dimensión: Tiempo ─────────────────────────────────────────────────────────

def load_dim_date(dates: pd.Series) -> None:
    """Inserta fechas nuevas en dim_date. Idempotente por PRIMARY KEY."""
    unique_dates = pd.to_datetime(dates.dropna().unique())
    hook = _get_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO dim_date (date_id, year, quarter, month, month_name, week, day, day_of_week, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
    """
    rows = [
        (
            d.date(), d.year, d.quarter, d.month,
            d.strftime("%B"), int(d.strftime("%W")), d.day,
            d.weekday(), d.weekday() >= 5,
        )
        for d in unique_dates
    ]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    log.info("dim_date: %d fechas cargadas", len(rows))


# ── Hechos: Transacciones (tabla central del esquema estrella) ────────────────

def load_fact_transactions(sales_df: pd.DataFrame, returns_df: pd.DataFrame) -> None:
    """
    Inserta ventas y devoluciones en la tabla única fact_transactions.
    transaction_type = 'SALE' para ventas, 'RETURN' para devoluciones.
    gross_revenue es negativo para devoluciones → SUM() da el neto directo.
    Idempotente por UNIQUE (invoice_no, product_code, source).
    """
    hook = _get_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO fact_transactions
            (invoice_no, product_code, customer_id, date_id, invoice_date,
             quantity, unit_price, gross_revenue, transaction_type, country, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (invoice_no, product_code, source) DO NOTHING
    """

    def _build_rows(df, trx_type):
        # Para devoluciones el gross_revenue va negativo para que SUM() dé el neto
        sign = 1 if trx_type == "SALE" else -1
        revenue_col = "gross_revenue" if trx_type == "SALE" else "return_amount"
        return [
            (
                str(r["invoice_no"]),
                str(r["stock_code"]),
                str(r["customer_id"]),
                r["date_id"],
                r["invoice_date"],
                int(r["quantity"]),
                float(r["unit_price"]),
                float(r[revenue_col]) * sign,
                trx_type,
                str(r["country"]) if pd.notna(r["country"]) else None,
                str(r["_source"]),
            )
            for r in df.to_dict("records")
        ]

    rows = _build_rows(sales_df, "SALE") + _build_rows(returns_df, "RETURN")
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    log.info("fact_transactions: %d filas insertadas (%d ventas, %d devoluciones)",
             len(rows), len(sales_df), len(returns_df))


# ── Métricas: Revenue neto diario ─────────────────────────────────────────────

def load_agg_daily_revenue(run_date: str) -> None:
    """Recalcula agg_daily_revenue desde fact_transactions. Idempotente por UPSERT."""
    hook = _get_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO agg_daily_revenue
            (date_id, product_code, gross_revenue, return_amount, net_revenue,
             total_sales_qty, total_return_qty, updated_at)
        SELECT
            date_id,
            product_code,
            SUM(CASE WHEN transaction_type = 'SALE'   THEN gross_revenue  ELSE 0 END) AS gross_revenue,
            SUM(CASE WHEN transaction_type = 'RETURN'  THEN ABS(gross_revenue) ELSE 0 END) AS return_amount,
            SUM(gross_revenue)                                                          AS net_revenue,
            SUM(CASE WHEN transaction_type = 'SALE'   THEN quantity       ELSE 0 END) AS total_sales_qty,
            SUM(CASE WHEN transaction_type = 'RETURN'  THEN ABS(quantity) ELSE 0 END) AS total_return_qty,
            NOW()
        FROM fact_transactions
        GROUP BY date_id, product_code
        ON CONFLICT (date_id, product_code)
        DO UPDATE SET
            gross_revenue    = EXCLUDED.gross_revenue,
            return_amount    = EXCLUDED.return_amount,
            net_revenue      = EXCLUDED.net_revenue,
            total_sales_qty  = EXCLUDED.total_sales_qty,
            total_return_qty = EXCLUDED.total_return_qty,
            updated_at       = NOW()
    """
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()
    log.info("agg_daily_revenue: recalculado correctamente")


# ── Log de rechazados ─────────────────────────────────────────────────────────

def load_reject_log(df: pd.DataFrame, run_date: str) -> None:
    """Inserta los registros rechazados con su motivo en reject_log."""
    if df.empty:
        log.info("reject_log: sin rechazados en esta ejecución")
        return
    hook = _get_hook()
    conn = hook.get_conn()
    cur = conn.cursor()
    sql = """
        INSERT INTO reject_log
            (source, raw_invoice_no, raw_product_code, raw_data, reject_reason, pipeline_run_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            str(r.get("_source", "unknown")),
            str(r.get("invoice_no")) if r.get("invoice_no") else None,
            str(r.get("stock_code")) if r.get("stock_code") else None,
            str(r),
            str(r.get("_reject_reason", "unknown")),
            run_date,
        )
        for r in df.to_dict("records")
    ]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    log.info("reject_log: %d registros rechazados guardados", len(rows))
