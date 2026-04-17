from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parent
DB_URL = "mysql+pymysql://kristian:taxi2024@127.0.0.1:3306/nyc_taxi_db"
engine = create_engine(DB_URL)

def load_zones():
    # Cargar el CSV de zonas
    zones_path = BASE_DIR / "data/raw/zones/taxi_zone_lookup.csv"
    df = pd.read_csv(zones_path)
    
    print("=== ZONAS ===")
    print(f"Filas: {len(df)}")
    print(f"Columnas: {list(df.columns)}")
    print(df.head())
    
    # Cargar a MySQL
    df.to_sql(
        name="dim_zones",
        con=engine,
        if_exists="replace",
        index=False
    )
    print(f"\nTabla dim_zones cargada: {len(df)} zonas")

if __name__ == "__main__":
    load_zones()