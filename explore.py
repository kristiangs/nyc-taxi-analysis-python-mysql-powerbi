from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent

# Rutas de los archivos de enero 2019
yellow_path = BASE_DIR / "data/raw/yellow/2019/yellow_tripdata_2019-01.parquet"
green_path  = BASE_DIR / "data/raw/green/2019/green_tripdata_2019-01.parquet"

# Cargar solo 1000 filas para explorar rápido
yellow = pd.read_parquet(yellow_path).head(1000)
green  = pd.read_parquet(green_path).head(1000)

# Homologar fechas
yellow.rename(columns={
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime"
}, inplace=True)

green.rename(columns={
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime"
}, inplace=True)

print("=== YELLOW TAXI ===")
print(f"Filas totales muestra: {len(yellow)}")
print(f"Columnas: {list(yellow.columns)}")
print(yellow.dtypes)

print("\n=== GREEN TAXI ===")
print(f"Filas totales muestra: {len(green)}")
print(f"Columnas: {list(green.columns)}")
print(green.dtypes)

print("\n=== COLUMNAS SOLO EN YELLOW ===")
print(set(yellow.columns) - set(green.columns))

print("\n=== COLUMNAS SOLO EN GREEN ===")
print(set(green.columns) - set(yellow.columns))

print("\n=== COLUMNAS EN COMÚN ===")
print(set(yellow.columns) & set(green.columns))