# DataMart ETL Pipeline

Pipeline de datos de extremo a extremo para DataMart S.A.S.  
Ingesta transacciones desde archivos CSV/XLSX y las carga en un repositorio analítico PostgreSQL, todo orquestado con Apache Airflow 3 en Docker.

---

## Requisitos previos

- Docker Desktop o Docker Engine + Docker Compose Plugin
- 4 GB de RAM disponibles para los contenedores
- Los archivos de datos en `data/`:
  - `data.csv` — transacciones de ventas (Kaggle: carrie1/ecommerce-data)
  - `online_retail_II.xlsx` — historial extendido (Kaggle: thedevastator/online-retail-transaction-dataset)

---

## Levantar el entorno (desde cero)

```bash
# 1. Clonar el repositorio
git clone <url-del-repo>
cd datamart_etl

# 2. Copiar el archivo de variables de entorno
cp .env.example .env
# Editar .env si se desean cambiar credenciales

# 3. Copiar los archivos de datos al directorio data/
cp /ruta/a/data.csv data/
cp /ruta/a/online_retail_II.xlsx data/

# 4. Levantar todos los servicios
docker compose up -d --build

# 5. Esperar ~2 minutos a que airflow-init termine
docker compose logs -f airflow-init
# Esperar el mensaje: ✅ Airflow init completado. Entorno listo.
```

Airflow UI disponible en: **http://localhost:8080**  
Usuario: `admin` | Contraseña: `admin123` (o la configurada en `.env`)

---

## Ejecutar el pipeline

### Desde la UI de Airflow

1. Entrar a http://localhost:8080
2. Buscar el DAG `datamart_etl_pipeline`
3. Activarlo con el toggle
4. Hacer clic en **Trigger DAG** para ejecutarlo manualmente

### Desde la terminal

```bash
docker exec airflow-scheduler airflow dags trigger datamart_etl_pipeline
```

### Verificar que terminó correctamente

```bash
# Ver el estado de la última ejecución
docker exec airflow-scheduler airflow dags list-runs -d datamart_etl_pipeline

# Ver logs de una tarea específica
docker compose logs airflow-scheduler | grep "datamart_etl"
```

---

## Verificar que los datos llegaron al repositorio analítico

```bash
# Conectarse a la base de datos analítica
docker exec -it datamart-db psql -U datamart -d datamart_dw

# Dentro de psql:
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM fact_returns;
SELECT COUNT(*) FROM agg_daily_revenue;
SELECT COUNT(*) FROM reject_log;
SELECT COUNT(*) FROM dim_products;
\q
```

También se puede conectar con cualquier cliente SQL externo:
- **Host:** localhost | **Puerto:** 5434
- **Base de datos:** datamart_dw
- **Usuario:** datamart | **Contraseña:** datamart123

---

## Validar Connections y Variables de Airflow

```bash
# Verificar conexiones
docker exec airflow-scheduler airflow connections get datamart_dw
docker exec airflow-scheduler airflow connections get datamart_csv_source

# Verificar variables
docker exec airflow-scheduler airflow variables get csv_sales_filename
docker exec airflow-scheduler airflow variables get csv_history_filename
docker exec airflow-scheduler airflow variables get pipeline_batch_size
```

---

## Consultas de negocio

Las consultas SQL para responder las 7 preguntas de negocio están en `sql/business_queries.sql`.

```bash
# Ejecutar todas las consultas de validación
docker exec -i datamart-db psql -U datamart -d datamart_dw < sql/business_queries.sql
```

---

## Estructura del repositorio

```
datamart_etl/
├── dags/
│   └── datamart_pipeline.py      # DAG principal de Airflow
├── plugins/
│   └── datamart_utils/
│       ├── extract.py            # Lectura de CSV y XLSX
│       ├── transform.py          # Limpieza, validación y separación
│       ├── categories.py         # Asignación de categorías por keywords
│       └── load.py               # Carga a PostgreSQL
├── sql/
│   ├── init_datamart.sql         # DDL: creación de tablas del repositorio analítico
│   └── business_queries.sql      # Consultas para las 7 preguntas de negocio
├── data/                         # Archivos CSV/XLSX (no versionados)
├── docs/
│   ├── README.md                 # Este archivo
│   ├── decisiones_tecnicas.md    # Documento de decisiones de diseño
│   └── data_model.png            # Diagrama del modelo de datos
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── .gitignore
```

---

## Servicios Docker

| Servicio | Descripción | Puerto |
|---|---|---|
| airflow-api-server | UI y API de Airflow | 8080 |
| airflow-scheduler | Ejecutor de DAGs | — |
| airflow-dag-processor | Procesador de archivos DAG | — |
| airflow-triggerer | Manejo de tareas diferidas | — |
| airflow-db | PostgreSQL metadatos de Airflow | interno |
| datamart-db | PostgreSQL repositorio analítico | 5434 |

---

## Detener el entorno

```bash
# Detener sin borrar datos
docker compose down

# Detener y borrar todos los datos (reset completo)
docker compose down -v
```
