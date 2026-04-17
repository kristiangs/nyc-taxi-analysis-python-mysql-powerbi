from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

COLUMNS_TO_KEEP = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
    "congestion_surcharge",
    "taxi_type"
]

def load_and_rename(path: Path, taxi_type: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    
    # Homologar fechas
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
    # Quedarnos solo con columnas necesarias
    df = df[[c for c in COLUMNS_TO_KEEP if c in df.columns]].copy()

    # Eliminar nulos en columnas clave
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime", 
                            "PULocationID", "DOLocationID"])

    # Filtrar valores inválidos
    df = df[df["trip_distance"] > 0]
    df = df[df["fare_amount"] > 0]
    df = df[df["total_amount"] > 0]
    df = df[df["passenger_count"] > 0]

    # Columnas de tiempo derivadas
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
    df["trip_duration_min"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"])
        .dt.total_seconds() / 60
    ).round(2)

    # Filtrar viajes con duración inválida
    df = df[df["trip_duration_min"] > 0]
    df = df[df["trip_duration_min"] < 300]  # máximo 5 horas

    # Columnas de fecha para análisis
    df["year"]  = df["pickup_datetime"].dt.year
    df["month"] = df["pickup_datetime"].dt.month
    df["hour"]  = df["pickup_datetime"].dt.hour
    df["day_of_week"] = df["pickup_datetime"].dt.dayofweek

    # Filtrar años inválidos
    df = df[df["year"].isin([2019, 2020])]

    return df

def main():
    yellow_path = BASE_DIR / "data/raw/yellow/2019/yellow_tripdata_2019-01.parquet"
    green_path  = BASE_DIR / "data/raw/green/2019/green_tripdata_2019-01.parquet"

    print("Cargando Yellow...")
    yellow = load_and_rename(yellow_path, "yellow")
    yellow = clean_data(yellow)
    print(f"Yellow limpio: {len(yellow):,} filas")

    print("Cargando Green...")
    green = load_and_rename(green_path, "green")
    green = clean_data(green)
    print(f"Green limpio: {len(green):,} filas")

    print("\n=== MUESTRA YELLOW ===")
    print(yellow.head(3))

    print("\n=== MUESTRA GREEN ===")
    print(green.head(3))

if __name__ == "__main__":
    main()