import pandas as pd
import numpy as np
import logging
from datetime import timezone

log = logging.getLogger(__name__)


# ── Normalización de fechas ───────────────────────────────────────────────────

def _parse_dates(series: pd.Series) -> pd.Series:
    """Convierte fechas a datetime UTC. Acepta múltiples formatos."""
    # data.csv usa '12/1/2010 8:26', xlsx usa timestamps numéricos o ISO
    parsed = pd.to_datetime(series, infer_datetime_format=True, errors="coerce")
    # Si no tiene timezone, asumimos que viene en UTC
    return parsed.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")


# ── Descripción canónica por producto ────────────────────────────────────────

def _build_canonical_descriptions(df: pd.DataFrame) -> dict:
    """
    Por cada stock_code elige la descripción más frecuente normalizada a Title Case.
    Decisión: usamos la moda para evitar variaciones de escritura (mayúsculas/minúsculas).
    """
    df = df.copy()
    df["description"] = df["description"].fillna("").str.strip().str.title()
    # ignoramos descripciones vacías para el conteo
    df_valid = df[df["description"] != ""]
    mode_desc = (
        df_valid.groupby("stock_code")["description"]
        .agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else "")
    )
    return mode_desc.to_dict()


# ── Deduplicación entre fuentes ───────────────────────────────────────────────

def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina duplicados entre sales_csv e history_csv.
    Clave: invoice_no + stock_code. Si existe en ambas fuentes,
    se conserva sales_csv porque es la fuente operacional más reciente.
    """
    # ordenamos para que sales_csv quede primero al hacer drop_duplicates
    source_order = {"sales_csv": 0, "history_csv": 1}
    df = df.copy()
    df["_source_order"] = df["_source"].map(source_order)
    df = df.sort_values("_source_order")
    before = len(df)
    df = df.drop_duplicates(subset=["invoice_no", "stock_code"], keep="first")
    after = len(df)
    log.info("Deduplicación entre fuentes: %d duplicados eliminados", before - after)
    return df.drop(columns=["_source_order"])


# ── Transformación principal ──────────────────────────────────────────────────

def transform(df: pd.DataFrame, run_date: str) -> tuple:
    """
    Recibe el DataFrame combinado de extract y devuelve:
      - sales_df:   transacciones válidas (quantity > 0, unit_price > 0)
      - returns_df: devoluciones (quantity <= 0)
      - rejects_df: registros rechazados con motivo
    """
    df = df.copy()
    rejects = []

    # 1. Normalizar stock_code: mayúsculas, sin espacios
    df["stock_code"] = df["stock_code"].fillna("").str.strip().str.upper()

    # 2. Calcular descripciones canónicas antes de cualquier filtro
    canonical_desc = _build_canonical_descriptions(df)
    df["description"] = df["stock_code"].map(canonical_desc).fillna("UNKNOWN")

    # 3. Parsear fechas a UTC
    df["invoice_date"] = _parse_dates(df["invoice_date"])

    # 4. Convertir quantity y unit_price a numérico
    df["quantity"]   = pd.to_numeric(df["quantity"],   errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"],  errors="coerce")

    # 5. Rechazar filas con fechas inválidas
    mask_bad_date = df["invoice_date"].isna()
    if mask_bad_date.any():
        bad = df[mask_bad_date].copy()
        bad["_reject_reason"] = "fecha inválida o no parseable"
        rejects.append(bad)
        df = df[~mask_bad_date]
        log.info("Rechazados por fecha inválida: %d", mask_bad_date.sum())

    # 6. Rechazar filas con quantity o unit_price no numéricos
    mask_bad_nums = df["quantity"].isna() | df["unit_price"].isna()
    if mask_bad_nums.any():
        bad = df[mask_bad_nums].copy()
        bad["_reject_reason"] = "quantity o unit_price no numérico"
        rejects.append(bad)
        df = df[~mask_bad_nums]
        log.info("Rechazados por valores no numéricos: %d", mask_bad_nums.sum())

    # 7. Rechazar ventas con unit_price <= 0 (regla de negocio)
    #    Solo aplica a ventas (quantity > 0); las devoluciones pueden tener precio 0
    mask_bad_price = (df["quantity"] > 0) & (df["unit_price"] <= 0)
    if mask_bad_price.any():
        bad = df[mask_bad_price].copy()
        bad["_reject_reason"] = "venta con unit_price <= 0"
        rejects.append(bad)
        df = df[~mask_bad_price]
        log.info("Rechazados por precio inválido en venta: %d", mask_bad_price.sum())

    # 8. Rechazar filas sin stock_code válido
    mask_no_code = df["stock_code"].str.len() == 0
    if mask_no_code.any():
        bad = df[mask_no_code].copy()
        bad["_reject_reason"] = "stock_code vacío"
        rejects.append(bad)
        df = df[~mask_no_code]

    # 9. customer_id vacío → ANONYMOUS (decisión: incluir con cliente especial)
    df["customer_id"] = (
        df["customer_id"]
        .fillna("ANONYMOUS")
        .str.strip()
        .replace("", "ANONYMOUS")
        .replace("nan", "ANONYMOUS")
    )
    # Normalizar customer_id a string sin decimales (Kaggle los guarda como '17850.0')
    df["customer_id"] = df["customer_id"].str.replace(r"\.0$", "", regex=True)

    # 10. Deduplicar entre fuentes
    df = _deduplicate(df)

    # 11. date_id para la dimensión de tiempo
    df["date_id"] = df["invoice_date"].dt.date

    # 12. Separar ventas y devoluciones (regla de negocio)
    sales_df   = df[df["quantity"] > 0].copy()
    returns_df = df[df["quantity"] <= 0].copy()

    # 13. Calcular revenue
    sales_df["gross_revenue"]  = sales_df["quantity"] * sales_df["unit_price"]
    returns_df["return_amount"] = returns_df["quantity"].abs() * returns_df["unit_price"]

    # 14. Consolidar rechazados
    rejects_df = pd.concat(rejects, ignore_index=True) if rejects else pd.DataFrame()
    if not rejects_df.empty:
        rejects_df["_pipeline_run_date"] = run_date

    log.info("Resultado: %d ventas | %d devoluciones | %d rechazados",
             len(sales_df), len(returns_df), len(rejects_df))

    return sales_df, returns_df, rejects_df
