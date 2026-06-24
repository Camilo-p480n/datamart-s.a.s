# Decisiones Técnicas — DataMart ETL Pipeline

---

## 1. Modelo del repositorio analítico

Se adoptó un **esquema estrella** con una única tabla de hechos central y tres dimensiones:

| Tabla | Tipo | Descripción |
|---|---|---|
| `dim_products` | Dimensión | Catálogo de productos con nombre canónico y categoría |
| `dim_customers` | Dimensión | Clientes únicos, incluye el cliente especial ANONYMOUS |
| `dim_date` | Dimensión | Calendario con atributos de tiempo para análisis temporal |
| `fact_transactions` | Hechos | Todas las transacciones: ventas (`SALE`) y devoluciones (`RETURN`) |
| `agg_daily_revenue` | Agregado | Revenue bruto, devoluciones y neto por día y producto |
| `reject_log` | Log | Registros rechazados con motivo y fuente |

**Por qué esquema estrella con tabla única de hechos:**
- Una sola tabla de hechos es el patrón canónico del modelo estrella: simplifica los joins al unir siempre la misma tabla con las dimensiones.
- Las ventas y devoluciones son el mismo tipo de evento (una línea de factura) que se distingue por el campo `transaction_type = 'SALE' | 'RETURN'`.
- El `gross_revenue` es positivo para ventas y negativo para devoluciones, lo que permite calcular el neto con un simple `SUM(gross_revenue)` sin joins adicionales.
- Separar en dos tablas de hechos produce un **esquema constelación**, no estrella.

**Por qué `agg_daily_revenue`:** Es una tabla derivada que precalcula el revenue por día y producto. Evita hacer GROUP BY sobre millones de filas en cada consulta de negocio y responde directamente las preguntas Q1, Q2 y Q3.

---

## 2. Casos ambiguos — Decisiones tomadas

### 2.1 Transacciones sin customer_id

**Decisión: incluirlas con el valor `ANONYMOUS`.**

Motivo: excluirlas eliminaría aproximadamente el 25% de las transacciones, lo que distorsionaría el análisis de revenue. El cliente ANONYMOUS se crea como registro especial en `dim_customers` con `is_anonymous = TRUE`, lo que permite segmentar o excluirlo según la necesidad en cada consulta (ver Q5).

Impacto documentado: las métricas de clientes únicos y ticket promedio por cliente no son aplicables al segmento anónimo. La Q5 expone esta diferencia explícitamente.

### 2.2 Variaciones de escritura en descripciones del producto

**Decisión: usar la descripción más frecuente por stock_code, normalizada a Title Case.**

Por ejemplo, para el código `85123A` que aparece como `WHITE HANGING HEART T-LIGHT HOLDER`, `white hanging heart t-light holder` y `White Hanging Heart T-Light Holder`, se elige la forma más frecuente en Title Case.

Motivo: la moda estadística representa el nombre tal como lo ingresó el sistema más veces, reduciendo el riesgo de elegir una versión con typos. Title Case mejora la legibilidad en reportes.

### 2.3 Solapamiento de fechas entre las dos fuentes

**Decisión: priorizar `sales_csv` (data.csv) sobre `history_csv` (online_retail_II.xlsx) para registros con la misma clave `invoice_no + stock_code`.**

Motivo: `data.csv` representa el volcado operacional actual del sistema; es la fuente de verdad más reciente. El historial sirve para ampliar el rango temporal pero no debe sobreescribir datos del sistema activo.

La deduplicación ocurre en `transform.py` antes de cargar, usando `drop_duplicates` con orden de prioridad explícito.

---

## 3. Reglas de rechazo

| Condición | Acción | Motivo |
|---|---|---|
| Fecha no parseable | Rechazar | Sin fecha no es posible ubicar la transacción en el tiempo |
| `quantity` o `unit_price` no numérico | Rechazar | No se puede calcular revenue |
| Venta con `unit_price <= 0` | Rechazar | Regla de negocio explícita |
| `stock_code` vacío | Rechazar | No se puede asociar a ningún producto del catálogo |

Los registros rechazados se almacenan en `reject_log` con el campo `reject_reason` para auditoría.

---

## 4. Normalización de stock_code

Todos los códigos se convierten a **mayúsculas sin espacios** antes de cualquier operación. Esto unifica variantes como `85123a`, `85123A` y ` 85123A ` en un único identificador `85123A`.

Los códigos que empiezan con letras (como `POST`, `DOT`, `BANK CHARGES`) no se descartan automáticamente, ya que pueden representar servicios o ajustes legítimos. Se incluyen con categoría `UNCATEGORIZED` si no hacen match con ningún keyword.

---

## 5. Asignación de categorías

No se implementó la API de catálogo (plus opcional). En su lugar se usa un mapa de palabras clave aplicado sobre la descripción canónica del producto.

Las 5 categorías del negocio son: `PAPELERIA`, `ELECTRONICA`, `ROPA`, `DEPORTES`, `HOGAR`.

El orden de evaluación importa para resolver ambigüedades: Papelería tiene prioridad sobre Hogar para evitar que `gift wrap` caiga en Hogar en lugar de Papelería.

Los productos sin match quedan como `UNCATEGORIZED`, lo que permite identificarlos y mejorar el mapa en iteraciones futuras.

---

## 6. Idempotencia del DAG

El pipeline puede ejecutarse múltiples veces el mismo día con los mismos datos y producir exactamente el mismo resultado. Esto se garantiza por tres mecanismos:

1. **Constraint UNIQUE en tablas de hechos:** `(invoice_no, product_code, source)`. Los `INSERT ... ON CONFLICT DO NOTHING` ignoran duplicados sin error.

2. **UPSERT en dimensiones:** `dim_products` y `dim_customers` usan `ON CONFLICT DO UPDATE`, actualizando el valor más reciente sin duplicar filas.

3. **Recálculo completo de `agg_daily_revenue`:** En lugar de acumular, la tarea `load_aggregates` recalcula el agregado completo desde las tablas de hechos y hace UPSERT. Así el resultado siempre refleja el estado actual de los hechos.

---

## 7. Manejo de datos entre tareas del DAG

Los DataFrames intermedios se serializan en archivos **Parquet temporales** dentro de `/opt/airflow/data/`. El XCom solo transporta las rutas a esos archivos (strings).

Motivo: el dataset combinado supera las 500.000 filas. Almacenar esos datos en XCom saturaría la base de datos de metadatos de Airflow, que no está diseñada para volúmenes de datos.

Los archivos temporales tienen el patrón `_tmp_<tipo>_<run_date>.parquet` y pueden limpiarse manualmente o con una tarea de cleanup si se requiere.

---

## 8. Formato de fechas

Todas las fechas se normalizan a **UTC** con `tz_localize("UTC")`. Las dos fuentes no especifican timezone explícitamente, pero al ser datos del Reino Unido se asume que están en UTC o en GMT sin ajuste de horario de verano (dado que el dataset cubre principalmente operaciones en hora estándar).

Las fechas inválidas o no parseables se rechazan y se registran en `reject_log`.
