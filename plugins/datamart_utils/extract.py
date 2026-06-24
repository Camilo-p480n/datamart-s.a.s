import pandas as pd
import logging

log = logging.getLogger(__name__)

# Nombres de columna unificados que usará el resto del pipeline
CANONICAL_COLS = ["invoice_no", "stock_code", "description", "quantity",
                  "invoice_date", "unit_price", "customer_id", "country"]


def _rename_sales_csv(df: pd.DataFrame) -> pd.DataFrame:
    # data.csv usa PascalCase con nombres distintos a online_retail_II
    return df.rename(columns={
        "InvoiceNo":   "invoice_no",
        "StockCode":   "stock_code",
        "Description": "description",
        "Quantity":    "quantity",
        "InvoiceDate": "invoice_date",
        "UnitPrice":   "unit_price",   # en xlsx se llama 'Price'
        "CustomerID":  "customer_id",  # en xlsx se llama 'Customer ID' (con espacio)
        "Country":     "country",
    })


def _rename_history_xlsx(df: pd.DataFrame) -> pd.DataFrame:
    # online_retail_II.xlsx tiene columnas con nombres distintos a data.csv
    return df.rename(columns={
        "Invoice":     "invoice_no",
        "StockCode":   "stock_code",
        "Description": "description",
        "Quantity":    "quantity",
        "InvoiceDate": "invoice_date",
        "Price":       "unit_price",
        "Customer ID": "customer_id",
        "Country":     "country",
    })


def extract_sales_csv(path: str) -> pd.DataFrame:
    """Lee data.csv y devuelve un DataFrame con columnas canónicas."""
    log.info("Extrayendo data.csv desde %s", path)
    # latin-1 porque el archivo original del UK contiene caracteres especiales
    df = pd.read_csv(path, encoding="latin-1", dtype=str, low_memory=False)
    df = _rename_sales_csv(df)
    df["_source"] = "sales_csv"
    log.info("data.csv: %d filas extraídas", len(df))
    return df[CANONICAL_COLS + ["_source"]]


def extract_history_xlsx(path: str) -> pd.DataFrame:
    """Lee las dos hojas de online_retail_II.xlsx y las concatena."""
    log.info("Extrayendo online_retail_II.xlsx desde %s", path)
    # sheet_name=None carga todas las hojas en un dict {nombre: DataFrame}
    sheets = pd.read_excel(path, sheet_name=None, dtype=str)
    frames = []
    for sheet_name, sheet_df in sheets.items():
        sheet_df = _rename_history_xlsx(sheet_df)
        # filtramos solo las columnas canónicas que existan en la hoja
        cols = [c for c in CANONICAL_COLS if c in sheet_df.columns]
        sheet_df = sheet_df[cols]
        sheet_df["_source"] = "history_csv"
        frames.append(sheet_df)
        log.info("  Hoja '%s': %d filas", sheet_name, len(sheet_df))
    df = pd.concat(frames, ignore_index=True)
    log.info("online_retail_II.xlsx: %d filas totales", len(df))
    return df
