# Guía de Sustentación — Pipeline ETL DataMart S.A.S.

Este documento explica **cada archivo, cada función y cada decisión** del proyecto.
Está escrito para que puedas leerlo, entenderlo y responder cualquier pregunta técnica en la sustentación.

---

## Índice

1. [Arquitectura general](#1-arquitectura-general)
2. [Estructura de carpetas](#2-estructura-de-carpetas)
3. [.env — Variables de entorno](#3-env--variables-de-entorno)
4. [Dockerfile — La imagen personalizada](#4-dockerfile--la-imagen-personalizada)
5. [docker-compose.yml — Los servicios](#5-docker-composeyml--los-servicios)
6. [sql/init_datamart.sql — El esquema estrella](#6-sqlinit_datamartql--el-esquema-estrella)
7. [plugins/datamart_utils/extract.py — Extracción](#7-pluginsdatamart_utilsextractpy--extracción)
8. [plugins/datamart_utils/transform.py — Transformación](#8-pluginsdatamart_utilstransformpy--transformación)
9. [plugins/datamart_utils/categories.py — Categorización](#9-pluginsdatamart_utilscategoriespy--categorización)
10. [plugins/datamart_utils/load.py — Carga a PostgreSQL](#10-pluginsdatamart_utilsloadpy--carga-a-postgresql)
11. [dags/datamart_pipeline.py — El DAG de Airflow](#11-dagsdatamart_pipelinepy--el-dag-de-airflow)
12. [sql/business_queries.sql — Las 7 consultas de negocio](#12-sqlbusiness_queriessql--las-7-consultas-de-negocio)
13. [Preguntas frecuentes en sustentación](#13-preguntas-frecuentes-en-sustentación)

---

## 1. Arquitectura general

```
Fuentes de datos          Airflow (orquestador)         PostgreSQL analítico
─────────────────         ──────────────────────         ────────────────────
data.csv (541k)    ──►   extract ──► transform ──►   dim_products
online_retail.xlsx ──►   (parquet)    (parquet)   ──► dim_customers
                                                   ──► dim_date
                                                   ──► fact_transactions  ◄── tabla central
                                                   ──► agg_daily_revenue
                                                   ──► reject_log
```

**¿Por qué Airflow?**
Airflow es un orquestador de pipelines de datos. Permite definir tareas, sus dependencias, reintentos automáticos, logs por tarea y programación diaria. En empresas reales es el estándar para ETL.

**¿Por qué Docker?**
Docker garantiza que el proyecto corra igual en cualquier máquina. Con un solo `docker compose up -d` se levantan 6 servicios sin instalar nada en el sistema operativo del host.

**¿Qué es ETL?**
- **E**xtract: leer los archivos CSV/XLSX originales
- **T**ransform: limpiar, validar, deduplicar y enriquecer los datos
- **L**oad: insertar en el modelo estrella de PostgreSQL

---

## 2. Estructura de carpetas

```
datamart_etl/
├── .env                          # Credenciales (nunca en git)
├── Dockerfile                    # Imagen personalizada de Airflow
├── docker-compose.yml            # Todos los servicios
├── dags/
│   └── datamart_pipeline.py      # El DAG principal (la orquestación)
├── plugins/
│   └── datamart_utils/
│       ├── __init__.py           # Marca el paquete (vacío)
│       ├── extract.py            # Lee CSV y XLSX
│       ├── transform.py          # Limpia y valida datos
│       ├── categories.py         # Asigna categorías por keywords
│       └── load.py               # Inserta en PostgreSQL
├── sql/
│   ├── init_datamart.sql         # Crea las tablas del modelo estrella
│   └── business_queries.sql     # 7 consultas de negocio
├── data/                         # Archivos CSV/XLSX y parquets temporales
├── logs/                         # Logs de Airflow
├── config/                       # Configuración de Airflow
└── docs/
    ├── README.md                 # Instrucciones de instalación y uso
    ├── decisiones_tecnicas.md    # Por qué se tomó cada decisión
    └── guia_sustentacion.md      # Este archivo
```

**¿Por qué `plugins/`?**
Airflow tiene un mecanismo llamado Plugin Manager que carga automáticamente cualquier módulo en la carpeta `plugins/`. Así el código utilitario (`datamart_utils`) queda disponible dentro de las tareas del DAG sin necesidad de instalar paquetes adicionales.

---

## 3. .env — Variables de entorno

El archivo `.env` centraliza **todas las credenciales y configuraciones sensibles**. Nunca se sube a git (está en `.gitignore`).

```ini
AIRFLOW_UID=50000               # UID del usuario dentro del contenedor
AIRFLOW_DB_USER=airflow         # Usuario de la BD interna de Airflow
AIRFLOW_DB_PASSWORD=airflow123
AIRFLOW_DB_NAME=airflow
AIRFLOW_FERNET_KEY=...          # Clave para cifrar conexiones en Airflow
AIRFLOW_SECRET_KEY=...          # Clave para firmar tokens JWT entre servicios
AIRFLOW_ADMIN_USER=admin        # Usuario web de Airflow
AIRFLOW_ADMIN_PASSWORD=admin123
DW_DB_USER=datamart             # Usuario del data warehouse analítico
DW_DB_PASSWORD=datamart123
DW_DB_NAME=datamart_dw
```

**¿Por qué variables de entorno y no valores hardcodeados?**
Si alguien accede al código fuente (GitHub, etc.) no obtiene acceso a las bases de datos. Es una práctica de seguridad estándar. El `docker-compose.yml` lee estas variables con la sintaxis `${VARIABLE}`.

**¿Qué es el Fernet Key?**
Es una clave de cifrado simétrico que Airflow usa para guardar contraseñas de conexiones en su base de datos de forma cifrada. Sin esta clave, Airflow no puede descifrar las contraseñas guardadas.

**¿Qué es el Secret Key / JWT Secret?**
En Airflow 3, cuando el scheduler lanza una tarea, el proceso hijo necesita autenticarse con el servidor de ejecución (`api-server`) mediante un token JWT. Si todos los servicios no comparten la misma clave de firma, los tokens son rechazados con error `JWT Signature verification failed`.

---

## 4. Dockerfile — La imagen personalizada

```dockerfile
FROM apache/airflow:3.2.2-python3.11

RUN pip install --no-cache-dir \
    pandas==2.2.2 \
    openpyxl \
    psycopg2-binary \
    apache-airflow-providers-postgres \
    apache-airflow-providers-common-sql \
    apache-airflow-providers-fab
```

**Línea a línea:**

| Línea | Qué hace | Por qué |
|---|---|---|
| `FROM apache/airflow:3.2.2-python3.11` | Parte de la imagen oficial de Airflow 3.2.2 con Python 3.11 | No construimos Airflow desde cero, extendemos la imagen oficial |
| `pandas==2.2.2` | Librería para manipular DataFrames | Leer CSV, XLSX, transformar datos, escribir Parquet |
| `openpyxl` | Motor para leer archivos `.xlsx` | pandas necesita openpyxl para `read_excel()` |
| `psycopg2-binary` | Driver de PostgreSQL para Python | Permite conectarse a PostgreSQL desde Python |
| `apache-airflow-providers-postgres` | Proveedor oficial de Airflow para Postgres | Da el `PostgresHook` que usa `load.py` para conectarse |
| `apache-airflow-providers-common-sql` | Dependencia del proveedor Postgres | Requerida para que el proveedor funcione |
| `apache-airflow-providers-fab` | Proveedor del gestor de autenticación FAB | **Crítico en Airflow 3**: sin esto no hay UI de login |

**¿Por qué FAB?**
En Airflow 3 el sistema de autenticación fue separado del núcleo. Flask App Builder (FAB) es el gestor de usuarios. Si no se instala, Airflow usa un `SimpleAuthManager` que genera contraseñas aleatorias en cada arranque y no permite crear usuarios permanentes.

---

## 5. docker-compose.yml — Los servicios

El proyecto levanta **6 servicios** con un solo comando:

```
airflow-db        → PostgreSQL para metadatos internos de Airflow
datamart-db       → PostgreSQL para el repositorio analítico
airflow-init      → Se ejecuta una sola vez: migra BD, crea usuario, crea conexiones
airflow-api-server → Servidor web + API REST (puerto 8080)
airflow-scheduler  → Programa las ejecuciones del DAG
airflow-dag-processor → Parsea y valida los archivos DAG
airflow-triggerer  → Maneja tareas diferidas (deferrable tasks)
```

### El bloque `x-airflow-common`

```yaml
x-airflow-common: &airflow-common
  build: .
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    ...
```

El prefijo `x-` es una extensión de Docker Compose para definir **fragmentos reutilizables**. Con `&airflow-common` se nombra el bloque y con `<<: *airflow-common` se incluye en cada servicio que lo necesita. Evita repetir las mismas 20 líneas de configuración en cada servicio.

### Variables de entorno clave de Airflow

| Variable | Valor | Significado |
|---|---|---|
| `AIRFLOW__CORE__EXECUTOR` | `LocalExecutor` | Las tareas corren como subprocesos en la misma máquina (no en workers remotos) |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://...` | URL de conexión a la BD de metadatos de Airflow |
| `AIRFLOW__CORE__FERNET_KEY` | `${AIRFLOW_FERNET_KEY}` | Clave para cifrar contraseñas en la BD |
| `AIRFLOW__CORE__AUTH_MANAGER` | `...FabAuthManager` | Activa el sistema de login con FAB |
| `AIRFLOW__API__AUTH_BACKENDS` | `...basic_auth` | Permite autenticación HTTP básica en la API |
| `AIRFLOW__API_AUTH__JWT_SECRET` | `${AIRFLOW_SECRET_KEY}` | Clave compartida para firmar tokens JWT de tareas |
| `AIRFLOW__CORE__EXECUTION_API_SERVER_URL` | `http://airflow-api-server:8080/execution/` | **Crítico en Airflow 3**: le dice al subproceso de cada tarea dónde está el servidor de ejecución |
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | No cargar los DAGs de ejemplo que vienen por defecto |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `false` | Los DAGs nuevos quedan activos automáticamente |

### `airflow-init` — El servicio de inicialización

```yaml
command:
  - -c
  - |
    airflow db migrate                    # Crea las tablas internas de Airflow
    airflow users create --role Admin ... # Crea el usuario admin
    airflow connections add 'datamart_dw' # Registra la conexión a la BD analítica
    airflow variables set ...             # Inicializa variables del pipeline
```

Este servicio corre **una sola vez** (`restart: "no"`) antes de que arranquen los demás. Los demás servicios tienen `depends_on: airflow-init: condition: service_completed_successfully` — esperan a que este termine exitosamente.

### Los dos PostgreSQL

```yaml
airflow-db:   puerto interno 5432  (solo accesible dentro de Docker)
datamart-db:  puerto 5434:5432     (accesible desde DBeaver en localhost:5434)
```

La BD `datamart-db` monta el archivo SQL de inicialización:
```yaml
volumes:
  - ./sql/init_datamart.sql:/docker-entrypoint-initdb.d/01_init_datamart.sql
```
PostgreSQL ejecuta automáticamente cualquier archivo en `/docker-entrypoint-initdb.d/` la primera vez que arranca con un volumen vacío. Así las tablas del modelo estrella se crean solas.

---

## 6. sql/init_datamart.sql — El esquema estrella

Este archivo define toda la estructura del repositorio analítico.

### ¿Qué es un esquema estrella?

Un modelo dimensional donde:
- Hay **una tabla de hechos central** con las métricas numéricas (revenue, cantidad)
- Hay **tablas de dimensiones** con los atributos descriptivos (quién, qué, cuándo, dónde)
- Las dimensiones se conectan a la tabla de hechos con llaves foráneas

Se llama "estrella" porque en un diagrama ER la tabla de hechos queda en el centro y las dimensiones cuelgan alrededor como puntas de estrella.

### Tablas del modelo

#### `dim_products` — Catálogo de productos
```sql
CREATE TABLE IF NOT EXISTS dim_products (
    product_code    VARCHAR(20)  PRIMARY KEY,   -- ej: '85123A'
    canonical_name  VARCHAR(255) NOT NULL,       -- nombre más frecuente en Title Case
    category        VARCHAR(100) NOT NULL DEFAULT 'UNCATEGORIZED',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```
`IF NOT EXISTS` hace que el `CREATE TABLE` sea idempotente: si la tabla ya existe no da error.

#### `dim_customers` — Clientes
```sql
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id  VARCHAR(20)  PRIMARY KEY,   -- ej: '17850' o 'ANONYMOUS'
    country      VARCHAR(100),
    is_anonymous BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```
El cliente especial `ANONYMOUS` representa todas las transacciones sin customer_id.

#### `dim_date` — Calendario
```sql
CREATE TABLE IF NOT EXISTS dim_date (
    date_id      DATE     PRIMARY KEY,   -- ej: '2011-11-15'
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    month_name   VARCHAR(20) NOT NULL,   -- 'November'
    week         SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,      -- 0=lunes, 6=domingo
    is_weekend   BOOLEAN  NOT NULL
);
```
Almacena atributos de tiempo calculados una sola vez. Permite hacer `WHERE year = 2011` o `GROUP BY quarter` sin recalcular en cada consulta.

#### `fact_transactions` — La tabla central (hechos)
```sql
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id   BIGSERIAL    PRIMARY KEY,
    invoice_no       VARCHAR(20)  NOT NULL,
    product_code     VARCHAR(20)  NOT NULL REFERENCES dim_products(product_code),
    customer_id      VARCHAR(20)  NOT NULL REFERENCES dim_customers(customer_id),
    date_id          DATE         NOT NULL REFERENCES dim_date(date_id),
    invoice_date     TIMESTAMPTZ  NOT NULL,
    quantity         INTEGER      NOT NULL,
    unit_price       NUMERIC(10,4) NOT NULL,
    gross_revenue    NUMERIC(14,4) NOT NULL,   -- positivo=venta, negativo=devolución
    transaction_type VARCHAR(10)  NOT NULL,    -- 'SALE' | 'RETURN'
    country          VARCHAR(100),
    source           VARCHAR(50)  NOT NULL,    -- 'sales_csv' | 'history_csv'
    loaded_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_transaction UNIQUE (invoice_no, product_code, source)
);
```

**Puntos clave a recordar:**
- `BIGSERIAL`: entero autoincremental de 64 bits — permite hasta 9 quintillones de filas
- `REFERENCES`: llave foránea — garantiza integridad referencial. No se puede insertar un `product_code` que no exista en `dim_products`
- `UNIQUE (invoice_no, product_code, source)`: la clave de negocio natural que garantiza idempotencia
- `gross_revenue` negativo para devoluciones: `SUM(gross_revenue)` da el neto directamente
- `transaction_type`: distingue ventas de devoluciones con un campo, no con tablas separadas

#### `agg_daily_revenue` — Tabla agregada
```sql
CREATE TABLE IF NOT EXISTS agg_daily_revenue (
    date_id          DATE        NOT NULL REFERENCES dim_date(date_id),
    product_code     VARCHAR(20) NOT NULL REFERENCES dim_products(product_code),
    gross_revenue    NUMERIC(14,4) NOT NULL DEFAULT 0,
    return_amount    NUMERIC(14,4) NOT NULL DEFAULT 0,
    net_revenue      NUMERIC(14,4) NOT NULL DEFAULT 0,
    total_sales_qty  INTEGER      NOT NULL DEFAULT 0,
    total_return_qty INTEGER      NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_id, product_code)
);
```
No es una dimensión ni una tabla de hechos brutos. Es un **agregado precalculado** por fecha y producto. Las consultas de negocio Q1, Q2 y Q3 leen de aquí porque es mucho más rápido que hacer GROUP BY sobre los 530k registros de `fact_transactions` en cada consulta.

#### `reject_log` — Log de rechazados
```sql
CREATE TABLE IF NOT EXISTS reject_log (
    reject_id          BIGSERIAL PRIMARY KEY,
    source             VARCHAR(50) NOT NULL,
    raw_invoice_no     VARCHAR(50),
    raw_product_code   VARCHAR(50),
    raw_data           TEXT,
    reject_reason      VARCHAR(255) NOT NULL,
    pipeline_run_date  DATE        NOT NULL,
    logged_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```
Guarda los registros que no pasaron las validaciones. Permite auditoría: saber cuántos registros se descartaron, por qué motivo y de qué fuente.

---

## 7. plugins/datamart_utils/extract.py — Extracción

Este módulo lee las dos fuentes y las entrega con columnas unificadas (columnas canónicas).

### `CANONICAL_COLS`
```python
CANONICAL_COLS = ["invoice_no", "stock_code", "description", "quantity",
                  "invoice_date", "unit_price", "customer_id", "country"]
```
Las dos fuentes tienen nombres de columnas diferentes. Se define un conjunto canónico y ambas fuentes se renombran a este estándar antes de pasarlas al siguiente módulo.

### `extract_sales_csv(path)`
```python
df = pd.read_csv(path, encoding="latin-1", dtype=str, low_memory=False)
```
- `encoding="latin-1"`: el archivo data.csv fue generado en UK con codificación ISO-8859-1 (latin-1). Si se usa `utf-8` (el default) falla al encontrar caracteres como `é`, `ñ`, `£`.
- `dtype=str`: se leen **todas** las columnas como texto. Así se evita que pandas intente inferir tipos y convierta `customer_id` de `'17850'` a `17850.0` (float) por tener NaN en la misma columna.
- `low_memory=False`: le dice a pandas que no divida el archivo en chunks para inferir tipos, evitando warnings de tipo mixto.

```python
df["_source"] = "sales_csv"
```
Se agrega una columna `_source` para saber de qué archivo vino cada fila después de la concatenación. Es clave para la deduplicación.

### `extract_history_xlsx(path)`
```python
sheets = pd.read_excel(path, sheet_name=None, dtype=str)
```
- `sheet_name=None`: en lugar de leer una sola hoja, retorna un diccionario `{nombre_hoja: DataFrame}`. Así se cargan automáticamente ambas hojas (Year 2009-2010 y Year 2010-2011) sin hardcodear los nombres.

```python
for sheet_name, sheet_df in sheets.items():
    sheet_df = _rename_history_xlsx(sheet_df)
    cols = [c for c in CANONICAL_COLS if c in sheet_df.columns]
```
La list comprehension filtra solo las columnas canónicas que existen en la hoja, por si alguna hoja tuviera columnas distintas.

---

## 8. plugins/datamart_utils/transform.py — Transformación

Es el módulo más importante. Recibe el DataFrame combinado y devuelve tres DataFrames limpios.

### `_parse_dates(series)`
```python
parsed = pd.to_datetime(series, infer_datetime_format=True, errors="coerce")
return parsed.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
```
- `infer_datetime_format=True`: detecta automáticamente el formato (`'12/1/2010 8:26'`, timestamp numérico, ISO 8601, etc.)
- `errors="coerce"`: las fechas que no se puedan parsear se convierten en `NaT` (Not a Time) en vez de lanzar excepción
- `tz_localize("UTC")`: los datos no traen timezone explícita. Se asume UTC para todos
- `ambiguous="NaT"` y `nonexistent="NaT"`: maneja horas ambiguas en cambios de horario de verano convirtiéndolas en NaT en vez de fallar

### `_build_canonical_descriptions(df)`
```python
mode_desc = df_valid.groupby("stock_code")["description"].agg(
    lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else ""
)
```
Para cada `stock_code`, agrupa todas sus descripciones y elige la **moda** (valor más frecuente). Esto resuelve el problema de que el mismo producto aparezca escrito de 5 formas diferentes en el dataset.

### `_deduplicate(df)`
```python
source_order = {"sales_csv": 0, "history_csv": 1}
df["_source_order"] = df["_source"].map(source_order)
df = df.sort_values("_source_order")
df = df.drop_duplicates(subset=["invoice_no", "stock_code"], keep="first")
```
La lógica es: si el mismo `invoice_no + stock_code` aparece en ambas fuentes, se queda con el de `sales_csv` (orden 0). Al ordenar por `_source_order` y hacer `keep="first"`, automáticamente prevalece `sales_csv`.

### `transform(df, run_date)` — El flujo completo

| Paso | Qué hace | Por qué |
|---|---|---|
| 1 | `stock_code.str.upper().str.strip()` | Unifica `85123a`, `85123A`, ` 85123A ` en `85123A` |
| 2 | Descripción canónica por moda | Resuelve variaciones de escritura |
| 3 | Parsear fechas a UTC | Normalizar zona horaria para comparaciones correctas |
| 4 | `pd.to_numeric(..., errors="coerce")` | Convierte texto a número; mal-formados → NaN |
| 5 | Rechazar fechas NaT | Sin fecha no se puede ubicar en el tiempo |
| 6 | Rechazar quantity/price NaN | Sin números no se puede calcular revenue |
| 7 | Rechazar ventas con price ≤ 0 | Regla de negocio explícita del cliente |
| 8 | Rechazar stock_code vacío | Sin código no se puede asociar a un producto |
| 9 | customer_id vacío → `ANONYMOUS` | Incluir el 25% de transacciones sin ID |
| 10 | Deduplicar entre fuentes | Evitar contar dos veces transacciones solapadas |
| 11 | `date_id = invoice_date.dt.date` | Extraer solo la fecha para hacer FK a `dim_date` |
| 12 | Separar `quantity > 0` vs `<= 0` | Ventas vs devoluciones por regla de negocio |
| 13 | Calcular `gross_revenue` y `return_amount` | `quantity × unit_price` |

**¿Por qué `customer_id` tiene decimales como `'17850.0'`?**
Kaggle guardó la columna como float porque tiene NaN. Al convertir float a string en Python, `17850.0` se vuelve `'17850.0'`. Se corrige con:
```python
df["customer_id"].str.replace(r"\.0$", "", regex=True)
```
La expresión regular `\.0$` busca `.0` al final de la cadena y lo elimina.

---

## 9. plugins/datamart_utils/categories.py — Categorización

```python
CATEGORY_KEYWORDS = {
    "PAPELERIA": ["card", "notebook", "book", "pen", ...],
    "ELECTRONICA": ["led", "battery", "cable", ...],
    "ROPA": ["shirt", "dress", "coat", ...],
    "DEPORTES": ["ball", "sport", "gym", ...],
    "HOGAR": ["candle", "holder", "cushion", ...],
}
```

### `_assign_category(description)`
```python
for category, keywords in CATEGORY_KEYWORDS.items():
    if any(kw in desc_lower for kw in keywords):
        return category
return "UNCATEGORIZED"
```
Itera el diccionario en orden. La primera categoría cuyo keyword aparezca en la descripción (en minúsculas) gana. El orden importa:
- `PAPELERIA` antes de `HOGAR` porque `"gift wrap"` contiene `"wrap"` (HOGAR) pero también es papel → debe ser PAPELERIA
- `ELECTRONICA` antes de `HOGAR` porque `"led light"` tiene `"light"` (HOGAR) pero el led es lo relevante

`any(kw in desc_lower for kw in keywords)` es un generator expression: evalúa los keywords uno a uno y se detiene en el primero que encuentre (cortocircuito), lo que lo hace eficiente.

---

## 10. plugins/datamart_utils/load.py — Carga a PostgreSQL

### `_get_hook()`
```python
def _get_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=CONN_ID)
```
`PostgresHook` es la clase del proveedor oficial de Airflow para PostgreSQL. Lee la conexión `datamart_dw` que fue registrada en `airflow-init`. No necesita saber host, puerto ni contraseña — Airflow los gestiona centralizadamente.

### `load_dim_products(df)`
```python
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
```
- `ON CONFLICT DO UPDATE`: si el producto ya existe (ej: segunda ejecución del DAG), actualiza el nombre y la categoría con los valores nuevos. Si no existe, lo inserta. Esto es un **UPSERT**.
- `EXCLUDED`: en PostgreSQL, dentro del `ON CONFLICT`, `EXCLUDED` referencia los valores que se iban a insertar (pero que generaron conflicto).
- `executemany`: ejecuta el INSERT una vez por cada fila del DataFrame en un solo batch, mucho más eficiente que un loop con `execute` individual.

### `load_dim_customers(df)`
```python
ON CONFLICT (customer_id) DO NOTHING
```
Para clientes se usa `DO NOTHING` porque no queremos actualizar datos de un cliente que ya existe. El cliente `ANONYMOUS` se inserta en el SQL de inicialización como seed.

### `load_fact_transactions(sales_df, returns_df)`
```python
def _build_rows(df, trx_type):
    sign = 1 if trx_type == "SALE" else -1
    revenue_col = "gross_revenue" if trx_type == "SALE" else "return_amount"
    return [
        (..., float(r[revenue_col]) * sign, trx_type, ...)
        for r in df.to_dict("records")
    ]

rows = _build_rows(sales_df, "SALE") + _build_rows(returns_df, "RETURN")
```
- Las ventas (`SALE`) tienen `gross_revenue` positivo
- Las devoluciones (`RETURN`) tienen `gross_revenue` negativo (multiplicado por `-1`)
- Así `SUM(gross_revenue)` sobre toda la tabla da el **revenue neto** sin ningún JOIN

**¿Por qué `to_dict("records")` y no `itertuples()`?**
`itertuples()` renombra las columnas que empiezan con `_` (como `_source`) a un nombre interno de Python. `to_dict("records")` convierte cada fila en un diccionario `{"column": value}` y preserva todos los nombres exactamente como están.

### `load_agg_daily_revenue(run_date)`
```sql
INSERT INTO agg_daily_revenue (...)
SELECT
    date_id, product_code,
    SUM(CASE WHEN transaction_type = 'SALE'   THEN gross_revenue  ELSE 0 END),
    SUM(CASE WHEN transaction_type = 'RETURN' THEN ABS(gross_revenue) ELSE 0 END),
    SUM(gross_revenue),  -- neto directo
    ...
FROM fact_transactions
GROUP BY date_id, product_code
ON CONFLICT (date_id, product_code) DO UPDATE SET ...
```
Recalcula **todo** el agregado desde cero en cada ejecución. Esto es idempotente: si el DAG corre dos veces el mismo día, el resultado es idéntico porque el UPSERT sobreescribe con los mismos valores.

---

## 11. dags/datamart_pipeline.py — El DAG de Airflow

### ¿Qué es un DAG?
**D**irected **A**cyclic **G**raph — Grafo Dirigido Acíclico. En Airflow es la representación del pipeline como un grafo de tareas con dependencias. "Dirigido" porque las dependencias tienen dirección (A→B). "Acíclico" porque no puede haber ciclos (A→B→A).

### Decoradores TaskFlow API
```python
@dag(...)
def datamart_pipeline():
    @task
    def extract_sales(**context) -> str:
        ...
    @task
    def transform_data(sales_path: str, history_path: str) -> dict:
        ...
```
La **TaskFlow API** (introducida en Airflow 2.0) permite definir tareas como funciones Python decoradas con `@task`. Airflow infiere automáticamente las dependencias a partir de los argumentos de función: si `transform_data` recibe `sales_path` que retorna `extract_sales`, Airflow sabe que `transform_data` depende de `extract_sales`.

### Parámetros del DAG
```python
@dag(
    dag_id="datamart_etl_pipeline",
    schedule="@daily",          # Corre una vez al día
    start_date=datetime(2024, 1, 1),
    catchup=False,              # No recupera ejecuciones pasadas
    max_active_runs=1,          # Solo un run activo a la vez
    default_args={
        "retries": 2,           # Reintenta 2 veces si falla
        "retry_delay": timedelta(minutes=5),  # Espera 5 min entre reintentos
    }
)
```
- `catchup=False`: si el DAG tiene `start_date` en 2024 y hoy es 2026, NO intenta ejecutar cada día desde 2024. Solo ejecuta desde hoy.
- `max_active_runs=1`: previene que dos ejecuciones concurrentes intenten escribir en las mismas tablas al mismo tiempo (deadlock).

### Flujo de tareas
```
extract_sales_csv ──┐
                    ├──► transform_and_split ──► load_dimensions ──► load_facts ──► load_aggregates ──► log_rejected_records
extract_history_xlsx─┘
```

### ¿Por qué Parquet como intermediario?

```python
df.to_parquet(out_path, index=False)
return out_path   # XCom transporta solo el string con la ruta
```

Los **XComs** (Cross-Communications) son el mecanismo de Airflow para pasar datos entre tareas. Se almacenan en la base de datos de metadatos de Airflow. El problema: el dataset tiene 500,000+ filas. Si se intentara serializar ese DataFrame completo como XCom, colapsaría la base de datos de metadatos.

La solución: guardar el DataFrame en un archivo **Parquet** (formato columnar comprimido eficiente) y pasar solo la **ruta al archivo** como XCom. La siguiente tarea lee el archivo desde disco.

**¿Por qué Parquet y no CSV?**
- Parquet es 4-10x más pequeño que CSV para el mismo dato
- Preserva los tipos de datos (fechas, números) sin conversión
- Es mucho más rápido de leer que CSV

### Tarea `load_dimensions`
```python
df_all = pd.concat([df_sales, df_returns], ignore_index=True)
load_dim_products(df_all)
load_dim_customers(df_all)
load_dim_date(df_all["date_id"])
```
Se combinan ventas y devoluciones para tener el universo completo de productos, clientes y fechas antes de cargar la tabla de hechos. Si se cargaran primero los hechos, fallarían las llaves foráneas.

---

## 12. sql/business_queries.sql — Las 7 consultas de negocio

### Q1: Evolución mensual de ventas netas
```sql
SELECT d.year, d.month, d.month_name,
    SUM(a.gross_revenue) AS ventas_brutas,
    SUM(a.return_amount) AS devoluciones,
    SUM(a.net_revenue)   AS ventas_netas
FROM agg_daily_revenue a
JOIN dim_date d ON a.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```
Lee de `agg_daily_revenue` (no de `fact_transactions`) para mayor velocidad. El JOIN con `dim_date` añade el nombre del mes y el año.

### Q2: Revenue y tasa de devolución por categoría
```sql
ROUND(SUM(a.return_amount) / SUM(a.gross_revenue) * 100, 2) AS tasa_devolucion_pct
```
Calcula el porcentaje de devolución sobre el bruto. El `CASE WHEN SUM(a.gross_revenue) > 0` evita división por cero si una categoría tuviera solo devoluciones.

### Q3a y Q3b: Top 10 productos
Q3a ordena por `net_revenue DESC` para los más rentables.
Q3b usa `HAVING SUM(total_sales_qty) >= 10` para excluir productos con pocas ventas que podrían tener 100% de devolución por ser un outlier estadístico (1 venta, 1 devolución = 100%).

### Q4: Ticket promedio por país
```sql
FROM fact_transactions ft
WHERE ft.transaction_type = 'SALE'
```
Ahora que las ventas y devoluciones están en la misma tabla, se filtra con `WHERE transaction_type = 'SALE'` para analizar solo transacciones positivas.

```sql
SUM(ft.gross_revenue) / COUNT(DISTINCT ft.invoice_no) AS ticket_promedio_por_factura
```
Divide el revenue total entre el número de facturas únicas (no líneas). Una factura puede tener múltiples líneas de productos.

### Q5: Clientes identificados vs ANONYMOUS
```sql
JOIN dim_customers c ON ft.customer_id = c.customer_id
GROUP BY c.is_anonymous
```
El campo `is_anonymous` permite segmentar en dos grupos. El resultado suele mostrar que los clientes identificados tienen ticket promedio mayor que los anónimos.

### Q6: Productos únicos y variaciones de descripción
```sql
SELECT COUNT(DISTINCT product_code) AS total_productos_unicos FROM dim_products;
```
Consulta directa al catálogo dimensional. Ya en `dim_products` cada `product_code` tiene una sola descripción canónica (la moda elegida en `transform.py`).

### Q7: Recomendación al equipo de producto
```sql
WITH producto_stats AS (...),
percentiles AS (
    SELECT
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY revenue_bruto) AS p75_revenue,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY tasa_devolucion_pct) AS p75_devolucion
    FROM producto_stats
    WHERE unidades_vendidas >= 10
)
SELECT ... FROM producto_stats ps, percentiles p
WHERE ps.revenue_bruto >= p.p75_revenue
  AND ps.tasa_devolucion_pct >= p.p75_devolucion
```
Usa dos CTEs (Common Table Expressions — bloques `WITH`):
1. `producto_stats`: calcula métricas por producto
2. `percentiles`: calcula el percentil 75 de revenue y devolución

El resultado son productos en el **cuartil superior de revenue Y de tasa de devolución**: los que más venden pero también más se devuelven → candidatos prioritarios a revisión de calidad.

`PERCENTILE_CONT(0.75)` es una función de ventana que retorna el valor en el percentil 75 de la distribución.

---

## 13. Preguntas frecuentes en sustentación

**¿Por qué dos bases de datos PostgreSQL y no una?**
La BD de metadatos de Airflow (airflow-db) es la BD interna del orquestador — guarda DAGs, logs, XComs, conexiones. Es parte del sistema operativo del pipeline. La BD analítica (datamart-db) es el repositorio de negocio. Separarlas evita que las operaciones del pipeline afecten el rendimiento de las consultas de negocio y viceversa.

**¿Qué pasa si el DAG falla a mitad?**
Gracias a los mecanismos de idempotencia:
- `ON CONFLICT DO NOTHING/UPDATE` en todas las tablas
- La tarea puede relanzarse y producir el mismo resultado sin duplicar datos
- Airflow reintenta automáticamente 2 veces (`retries: 2`) esperando 5 minutos entre intentos

**¿Cómo se garantiza que los datos no se dupliquen?**
Con la constraint `UNIQUE (invoice_no, product_code, source)` en `fact_transactions`. Si se intenta insertar la misma combinación dos veces, el `ON CONFLICT DO NOTHING` simplemente ignora el duplicado sin error.

**¿Por qué `gross_revenue` negativo para devoluciones?**
Porque permite calcular el revenue neto con `SUM(gross_revenue)` en una sola operación sobre toda la tabla. Si las devoluciones fueran positivas se necesitaría: `SUM(ventas) - SUM(devoluciones)` con dos subqueries o dos tablas.

**¿Qué es un Parquet?**
Formato de almacenamiento columnar de Apache. En lugar de guardar fila por fila como CSV, guarda columna por columna. Ventajas: compresión muy eficiente (valores similares juntos se comprimen mejor), lectura rápida cuando solo necesitas algunas columnas, preserva tipos de datos sin conversión.

**¿Qué pasaría si se corriera el DAG dos veces el mismo día?**
El resultado sería idéntico. La primera ejecución inserta. La segunda ejecución intenta insertar los mismos registros, todos hacen conflicto con el UNIQUE y se ignoran con `DO NOTHING`. Las dimensiones se actualizan con `DO UPDATE` pero con los mismos valores. `agg_daily_revenue` se recalcula y el UPSERT sobreescribe con los mismos números.

**¿Por qué `max_active_runs=1`?**
Si dos ejecuciones del DAG corren en paralelo, ambas intentan hacer UPDATE en `agg_daily_revenue` simultáneamente. PostgreSQL podría entrar en deadlock (ambas transacciones esperando que la otra libere el lock). Con `max_active_runs=1` solo puede haber una ejecución activa a la vez, eliminando ese riesgo.

**¿Qué es el Fernet Key y por qué es importante?**
AES-128 en modo Fernet (librería de criptografía de Python). Airflow cifra las contraseñas de conexiones antes de guardarlas en su BD. Sin la misma Fernet Key, Airflow no puede descifrar las contraseñas guardadas → el DAG no puede conectarse a PostgreSQL.

**¿Qué es `LocalExecutor`?**
El ejecutor determina cómo Airflow lanza las tareas. `LocalExecutor` las lanza como subprocesos en la misma máquina. La alternativa es `CeleryExecutor` o `KubernetesExecutor` para correr tareas en workers remotos. Para este proyecto `LocalExecutor` es suficiente y más simple de configurar.

**¿Por qué `dtype=str` al leer el CSV?**
Para evitar que pandas infiera tipos automáticamente. El problema: `customer_id` tiene valores como `'17850'` y también `NaN`. Si pandas intenta convertir la columna a número, los valores válidos se vuelven `17850.0` (float). Al leer todo como string se preservan los valores exactos y la conversión de tipos se hace explícitamente en `transform.py`.
