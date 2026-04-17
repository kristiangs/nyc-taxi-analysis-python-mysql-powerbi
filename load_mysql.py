from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

# ── Conexión ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DB_URL = "mysql+pymysql://kristian:taxi2024@127.0.0.1:3306/nyc_taxi_db"
engine = create_engine(DB_URL)

COLUMNS_TO_KEEP = [
    "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "tolls_amount", "total_amount",
    "congestion_surcharge", "taxi_type", "trip_duration_min",
    "year", "month", "hour", "day_of_week"
]

# ── Funciones ─────────────────────────────────────────────────────────────
def load_and_rename(path: Path, taxi_type: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if taxi_type == "yellow":
        df.rename(columns={
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime"
        }, inplace=True)
    else:
        df.rename(columns={
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime"
        }, inplace=True)
    df["taxi_type"] = taxi_type
    return df

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime",
                            "PULocationID", "DOLocationID"])
    df = df[df["trip_distance"] > 0]
    df = df[df["fare_amount"] > 0]
    df = df[df["total_amount"] > 0]
    df = df[df["passenger_count"] > 0]
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
    df["trip_duration_min"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)
    df = df[df["trip_duration_min"] > 0]
    df = df[df["trip_duration_min"] < 300]
    df["year"]        = df["pickup_datetime"].dt.year
    df["month"]       = df["pickup_datetime"].dt.month
    df["hour"]        = df["pickup_datetime"].dt.hour
    df["day_of_week"] = df["pickup_datetime"].dt.dayofweek
    df = df[[c for c in COLUMNS_TO_KEEP if c in df.columns]]

    # Filtrar años inválidos
    df = df[df["year"].isin([2019, 2020])]
    return df

def load_to_mysql(df: pd.DataFrame, first_batch: bool):
    mode = "replace" if first_batch else "append"
    df.to_sql(
        name="fact_trips",
        con=engine,
        if_exists=mode,
        index=False,
        chunksize=10000
    )

# ── Main ──────────────────────────────────────────────────────────────────
def main():
    first_batch = True
    total_loaded = 0

    for taxi_type in ["yellow", "green"]:
        for year in [2019, 2020]:
            folder = BASE_DIR / "data/raw" / taxi_type / str(year)
            files = sorted(folder.glob("*.parquet"))

            for file in files:
                print(f"Procesando: {file.name}...")
                try:
                    df = load_and_rename(file, taxi_type)
                    df = clean_data(df)
                    load_to_mysql(df, first_batch)
                    first_batch = False
                    total_loaded += len(df)
                    print(f"  ✓ {len(df):,} filas cargadas — Total: {total_loaded:,}")
                except Exception as e:
                    print(f"  ✗ Error en {file.name}: {e}")

    print(f"\n=== Carga completa: {total_loaded:,} filas totales ===")

if __name__ == "__main__":
    main()