-- ============================================================
--  DATAMART S.A.S. — Consultas de negocio
--  Ejecutar contra la base de datos: datamart_dw
-- ============================================================


-- ── Q1: Evolución mensual de ventas netas ────────────────────────────────────
-- ¿Cuál fue la evolución mensual de las ventas netas (descontando devoluciones)?

SELECT
    d.year,
    d.month,
    d.month_name,
    ROUND(SUM(a.gross_revenue)::NUMERIC, 2)  AS ventas_brutas,
    ROUND(SUM(a.return_amount)::NUMERIC, 2)  AS devoluciones,
    ROUND(SUM(a.net_revenue)::NUMERIC, 2)    AS ventas_netas,
    SUM(a.total_sales_qty)                   AS unidades_vendidas
FROM agg_daily_revenue a
JOIN dim_date d ON a.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- ── Q2: Revenue bruto y tasa de devolución por categoría ────────────────────
-- ¿Qué categorías generaron más revenue bruto y cuáles tuvieron mayor proporción de devoluciones?

SELECT
    p.category,
    ROUND(SUM(a.gross_revenue)::NUMERIC, 2)  AS revenue_bruto,
    ROUND(SUM(a.return_amount)::NUMERIC, 2)  AS total_devoluciones,
    ROUND(
        CASE WHEN SUM(a.gross_revenue) > 0
             THEN SUM(a.return_amount) / SUM(a.gross_revenue) * 100
             ELSE 0 END
    ::NUMERIC, 2)                            AS tasa_devolucion_pct
FROM agg_daily_revenue a
JOIN dim_products p ON a.product_code = p.product_code
GROUP BY p.category
ORDER BY revenue_bruto DESC;


-- ── Q3a: Top 10 productos con mayor revenue neto ────────────────────────────
-- ¿Cuáles son los 10 productos con mayor revenue neto?

SELECT
    a.product_code,
    p.canonical_name,
    p.category,
    ROUND(SUM(a.gross_revenue)::NUMERIC, 2)  AS revenue_bruto,
    ROUND(SUM(a.return_amount)::NUMERIC, 2)  AS devoluciones,
    ROUND(SUM(a.net_revenue)::NUMERIC, 2)    AS revenue_neto
FROM agg_daily_revenue a
JOIN dim_products p ON a.product_code = p.product_code
GROUP BY a.product_code, p.canonical_name, p.category
ORDER BY revenue_neto DESC
LIMIT 10;


-- ── Q3b: Top 10 productos con mayor tasa de devolución ──────────────────────
-- ¿Cuáles son los 10 productos con mayor tasa de devolución?
-- Solo productos con al menos 10 ventas para evitar outliers estadísticos.

SELECT
    a.product_code,
    p.canonical_name,
    p.category,
    SUM(a.total_sales_qty)                   AS unidades_vendidas,
    SUM(a.total_return_qty)                  AS unidades_devueltas,
    ROUND(
        CASE WHEN SUM(a.total_sales_qty) > 0
             THEN SUM(a.total_return_qty)::NUMERIC / SUM(a.total_sales_qty) * 100
             ELSE 0 END
    , 2)                                     AS tasa_devolucion_pct
FROM agg_daily_revenue a
JOIN dim_products p ON a.product_code = p.product_code
GROUP BY a.product_code, p.canonical_name, p.category
HAVING SUM(a.total_sales_qty) >= 10
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;


-- ── Q4: Transacciones y ticket promedio por país ─────────────────────────────
-- ¿Qué países concentran la mayor parte de las transacciones?
-- ¿Cómo varía el ticket promedio entre ellos?

SELECT
    ft.country,
    COUNT(DISTINCT ft.invoice_no)                        AS num_facturas,
    COUNT(*)                                              AS num_lineas,
    ROUND(SUM(ft.gross_revenue)::NUMERIC, 2)              AS revenue_total,
    ROUND(AVG(ft.gross_revenue)::NUMERIC, 2)              AS ticket_promedio_por_linea,
    ROUND(
        SUM(ft.gross_revenue) / COUNT(DISTINCT ft.invoice_no)
    ::NUMERIC, 2)                                         AS ticket_promedio_por_factura
FROM fact_transactions ft
WHERE ft.transaction_type = 'SALE'
  AND ft.country IS NOT NULL
GROUP BY ft.country
ORDER BY revenue_total DESC;


-- ── Q5: Comportamiento de clientes identificados vs anónimos ────────────────
-- ¿Existe diferencia entre clientes identificados y transacciones sin customer_id?

SELECT
    c.is_anonymous,
    CASE WHEN c.is_anonymous THEN 'Sin customer_id (ANONYMOUS)'
         ELSE 'Cliente identificado' END                  AS tipo_cliente,
    COUNT(DISTINCT ft.invoice_no)                         AS num_facturas,
    COUNT(*)                                              AS num_lineas,
    ROUND(SUM(ft.gross_revenue)::NUMERIC, 2)              AS revenue_total,
    ROUND(AVG(ft.gross_revenue)::NUMERIC, 2)              AS ticket_promedio_linea,
    ROUND(
        SUM(ft.gross_revenue) / COUNT(DISTINCT ft.invoice_no)
    ::NUMERIC, 2)                                         AS ticket_promedio_factura,
    ROUND(AVG(ft.quantity)::NUMERIC, 2)                   AS cantidad_promedio
FROM fact_transactions ft
JOIN dim_customers c ON ft.customer_id = c.customer_id
WHERE ft.transaction_type = 'SALE'
GROUP BY c.is_anonymous
ORDER BY c.is_anonymous;


-- ── Q6: Productos sin descripción consistente y total de códigos ─────────────
-- ¿Qué productos tienen múltiples descripciones? ¿Cuántos códigos únicos hay?

-- Total de códigos únicos de producto
SELECT COUNT(DISTINCT product_code) AS total_productos_unicos
FROM dim_products;

-- Productos con más de una descripción registrada en las transacciones
SELECT
    ft.product_code,
    p.canonical_name,
    COUNT(DISTINCT ft.product_code)  AS apariciones,
    -- Contamos cuántas descripciones distintas existían antes de canonizar
    -- (visible en reject_log si alguna fue rechazada, o en la variación de nombres)
    p.category
FROM fact_transactions ft
JOIN dim_products p ON ft.product_code = p.product_code
GROUP BY ft.product_code, p.canonical_name, p.category
HAVING COUNT(*) > 0
ORDER BY apariciones DESC
LIMIT 20;


-- ── Q7: Recomendación concreta al equipo de producto ────────────────────────
-- Identifica productos de alta devolución con revenue bruto significativo:
-- candidatos a revisión de calidad o descripción engañosa.

WITH producto_stats AS (
    SELECT
        a.product_code,
        p.canonical_name,
        p.category,
        SUM(a.gross_revenue)                          AS revenue_bruto,
        SUM(a.return_amount)                          AS devoluciones,
        SUM(a.net_revenue)                            AS revenue_neto,
        SUM(a.total_sales_qty)                        AS unidades_vendidas,
        SUM(a.total_return_qty)                       AS unidades_devueltas,
        CASE WHEN SUM(a.total_sales_qty) > 0
             THEN ROUND(SUM(a.total_return_qty)::NUMERIC / SUM(a.total_sales_qty) * 100, 2)
             ELSE 0 END                               AS tasa_devolucion_pct
    FROM agg_daily_revenue a
    JOIN dim_products p ON a.product_code = p.product_code
    GROUP BY a.product_code, p.canonical_name, p.category
),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY revenue_bruto) AS p75_revenue,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tasa_devolucion_pct) AS p75_devolucion
    FROM producto_stats
    WHERE unidades_vendidas >= 10
)
-- Productos en el cuartil superior de revenue Y de tasa de devolución:
-- alto impacto económico + alto rechazo de clientes = prioridad de revisión.
SELECT
    ps.product_code,
    ps.canonical_name,
    ps.category,
    ROUND(ps.revenue_bruto::NUMERIC, 2)       AS revenue_bruto,
    ROUND(ps.devoluciones::NUMERIC, 2)        AS devoluciones,
    ROUND(ps.revenue_neto::NUMERIC, 2)        AS revenue_neto,
    ps.tasa_devolucion_pct,
    ps.unidades_vendidas,
    ps.unidades_devueltas
FROM producto_stats ps, percentiles p
WHERE ps.revenue_bruto      >= p.p75_revenue
  AND ps.tasa_devolucion_pct >= p.p75_devolucion
  AND ps.unidades_vendidas   >= 10
ORDER BY ps.devoluciones DESC
LIMIT 15;
