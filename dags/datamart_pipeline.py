"""
DAG principal del pipeline ETL de DataMart S.A.S.
Orquesta la ingesta, transformación y carga de transacciones
desde dos fuentes CSV/XLSX hacia el repositorio analítico en PostgreSQL.
"""

import os
import pandas as pd
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

# ── Parámetros por defecto del DAG ───────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "datamart",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Directorio base de datos dentro del contenedor ───────────────────────────
DATA_PATH = "/opt/airflow/data"


@dag(
    dag_id="datamart_etl_pipeline",
    description="Pipeline ETL: CSV/XLSX → transformación → PostgreSQL analítico",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,       # evita deadlocks por runs concurrentes
    default_args=DEFAULT_ARGS,
    tags=["datamart", "etl"],
)
def datamart_pipeline():

    # ── TAREA 1: Extraer data.csv ─────────────────────────────────────────────
    @task(task_id="extract_sales_csv")
    def extract_sales(**context) -> str:
        """Lee data.csv y guarda un parquet temporal. Retorna la ruta."""
        from datamart_utils.extract import extract_sales_csv

        # Nombre del archivo viene de Airflow Variables para facilitar cambios
        filename = Variable.get("csv_sales_filename", default_var="data.csv")
        path = os.path.join(DATA_PATH, filename)

        df = extract_sales_csv(path)

        # Guardamos en parquet para no saturar XCom con 500k filas
        run_date = context["ds"]  # 'YYYY-MM-DD' del día lógico del DAG
        out_path = os.path.join(DATA_PATH, f"_tmp_sales_{run_date}.parquet")
        df.to_parquet(out_path, index=False)
        return out_path

    # ── TAREA 2: Extraer online_retail_II.xlsx ────────────────────────────────
    @task(task_id="extract_history_xlsx")
    def extract_history(**context) -> str:
        """Lee el xlsx histórico (2 hojas) y guarda un parquet temporal."""
        from datamart_utils.extract import extract_history_xlsx

        filename = Variable.get("csv_history_filename", default_var="online_retail_II.xlsx")
        path = os.path.join(DATA_PATH, filename)

        df = extract_history_xlsx(path)

        run_date = context["ds"]
        out_path = os.path.join(DATA_PATH, f"_tmp_history_{run_date}.parquet")
        df.to_parquet(out_path, index=False)
        return out_path

    # ── TAREA 3: Transformar y separar ventas / devoluciones / rechazados ─────
    @task(task_id="transform_and_split")
    def transform_data(sales_path: str, history_path: str, **context) -> dict:
        """
        Combina ambas fuentes, aplica reglas de negocio y separa en
        ventas, devoluciones y rechazados. Retorna rutas a los parquets.
        """
        from datamart_utils.transform import transform
        from datamart_utils.categories import assign_categories

        run_date = context["ds"]

        # Cargar los parquets de extracción
        df_sales   = pd.read_parquet(sales_path)
        df_history = pd.read_parquet(history_path)
        df_combined = pd.concat([df_sales, df_history], ignore_index=True)

        # Transformar: limpieza, validación, dedup, separación
        sales_df, returns_df, rejects_df = transform(df_combined, run_date)

        # Asignar categorías usando las descripciones canónicas
        sales_df   = assign_categories(sales_df)
        returns_df = assign_categories(returns_df)

        # Guardar resultados en parquets temporales
        sales_out   = os.path.join(DATA_PATH, f"_tmp_fact_sales_{run_date}.parquet")
        returns_out = os.path.join(DATA_PATH, f"_tmp_fact_returns_{run_date}.parquet")
        rejects_out = os.path.join(DATA_PATH, f"_tmp_rejects_{run_date}.parquet")

        sales_df.to_parquet(sales_out, index=False)
        returns_df.to_parquet(returns_out, index=False)
        if not rejects_df.empty:
            rejects_df.to_parquet(rejects_out, index=False)

        return {
            "sales":   sales_out,
            "returns": returns_out,
            "rejects": rejects_out if not rejects_df.empty else None,
        }

    # ── TAREA 4: Cargar dimensiones ───────────────────────────────────────────
    @task(task_id="load_dimensions")
    def load_dimensions(paths: dict, **context) -> None:
        """Carga dim_products, dim_customers y dim_date."""
        from datamart_utils.load import (
            load_dim_products, load_dim_customers, load_dim_date,
        )

        # Combinamos ventas y devoluciones para tener todos los productos/clientes
        df_sales   = pd.read_parquet(paths["sales"])
        df_returns = pd.read_parquet(paths["returns"])
        df_all     = pd.concat([df_sales, df_returns], ignore_index=True)

        load_dim_products(df_all)
        load_dim_customers(df_all)
        load_dim_date(df_all["date_id"])

    # ── TAREA 5: Cargar hechos ────────────────────────────────────────────────
    @task(task_id="load_facts")
    def load_facts(paths: dict) -> None:
        """Carga ventas y devoluciones en fact_transactions. Idempotente por UNIQUE constraint."""
        from datamart_utils.load import load_fact_transactions

        df_sales   = pd.read_parquet(paths["sales"])
        df_returns = pd.read_parquet(paths["returns"])

        load_fact_transactions(df_sales, df_returns)

    # ── TAREA 6: Calcular métricas agregadas ──────────────────────────────────
    @task(task_id="load_aggregates")
    def load_aggregates(**context) -> None:
        """Recalcula agg_daily_revenue desde fact_sales y fact_returns."""
        from datamart_utils.load import load_agg_daily_revenue
        load_agg_daily_revenue(context["ds"])

    # ── TAREA 7: Registrar rechazados ─────────────────────────────────────────
    @task(task_id="log_rejected_records")
    def log_rejects(paths: dict, **context) -> None:
        """Guarda los registros rechazados en reject_log."""
        from datamart_utils.load import load_reject_log

        rejects_path = paths.get("rejects")
        if rejects_path and os.path.exists(rejects_path):
            df_rejects = pd.read_parquet(rejects_path)
            load_reject_log(df_rejects, context["ds"])
        else:
            import logging
            logging.getLogger(__name__).info("Sin rechazados en esta ejecución")

    # ── Dependencias entre tareas ─────────────────────────────────────────────
    # extract_sales  ──┐
    #                  ├──→ transform ──→ load_dims ──→ load_facts ──→ load_agg ──→ log_rejects
    # extract_history ─┘

    sales_path   = extract_sales()
    history_path = extract_history()
    paths        = transform_data(sales_path, history_path)
    load_dimensions(paths) >> load_facts(paths) >> load_aggregates() >> log_rejects(paths)


datamart_pipeline()
