import pandas as pd
import logging

log = logging.getLogger(__name__)

# ── Mapa de palabras clave por categoría ────────────────────────────────────
# Decisión: asignación basada en keywords de la descripción normalizada.
# Se usa porque no se implementó la API de catálogo (plus opcional).
# El orden importa: la primera categoría que haga match gana.
CATEGORY_KEYWORDS = {
    "PAPELERIA": [
        "card", "notebook", "book", "pen", "pencil", "sticker", "tape",
        "paper", "envelope", "postcard", "sign", "letter", "tag", "label",
        "note", "diary", "calendar", "wrapping",
    ],
    "ELECTRONICA": [
        "led", "battery", "batteries", "cable", "charger", "plug", "usb",
        "solar", "electric", "light string", "fairy light", "string light",
    ],
    "ROPA": [
        "shirt", "dress", "coat", "hanger", "scarf", "hat", "sock", "glove",
        "shoe", "belt", "jacket", "blouse", "skirt", "trouser", "pant",
        "apron", "bib", "babygrow",
    ],
    "DEPORTES": [
        "ball", "sport", "gym", "fitness", "yoga", "run", "bike", "swim",
        "game", "play", "toy", "puzzle", "dart", "skipping", "kite",
    ],
    "HOGAR": [
        "candle", "holder", "lantern", "heart", "cushion", "tin", "box",
        "jar", "frame", "clock", "mirror", "lamp", "kitchen", "garden",
        "storage", "basket", "bowl", "plate", "mug", "cup", "glass",
        "vase", "bottle", "tray", "rack", "shelf", "drawer", "light",
        "bag", "bunting", "wreath", "garland", "decoration", "decor",
        "hanging", "metal", "wooden", "ceramic", "vintage", "retro",
        "floral", "rose", "polka", "spotty", "stripe", "wrap",
    ],
}


def _assign_category(description: str) -> str:
    """Retorna la categoría para una descripción de producto."""
    if not description or description.upper() in ("", "UNKNOWN"):
        return "UNCATEGORIZED"
    desc_lower = description.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in desc_lower for kw in keywords):
            return category
    return "UNCATEGORIZED"


def assign_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega la columna 'category' al DataFrame usando la descripción del producto.
    Opera sobre el DataFrame de productos únicos para no recalcular por cada fila.
    """
    df = df.copy()
    df["category"] = df["description"].apply(_assign_category)
    counts = df["category"].value_counts().to_dict()
    log.info("Categorías asignadas: %s", counts)
    return df
