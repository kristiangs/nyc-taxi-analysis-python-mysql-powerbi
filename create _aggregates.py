"""
NYC Taxi Analysis — Sesion 3
Script: create_aggregates.py
Descripcion: Crea las tablas agregadas en MySQL para Power BI
Autor: Cristian Guevara
"""

from sqlalchemy import create_engine, text
import pandas as pd
import time

# ─────────────────────────────────────────
# CONEXION
# ─────────────────────────────────────────
engine = create_engine("mysql+pymysql://user:password@host/db_name")

def run_query(nombre, sql):
    """Ejecuta una query de creacion y muestra el tiempo que tardo."""
    print(f"\n>>> Creando {nombre}...")
    inicio = time.time()
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    segundos = round(time.time() - inicio, 1)
    print(f"    ✓ {nombre} lista en {segundos}s")


# ─────────────────────────────────────────
# 1. AGG_BY_HOUR — demanda y revenue por hora del dia
# ─────────────────────────────────────────
AGG_BY_HOUR = """
CREATE TABLE IF NOT EXISTS agg_by_hour AS
SELECT
    year,
    month,
    hour,
    taxi_type,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(total_amount), 2)     AS avg_revenue_per_trip,
    ROUND(AVG(trip_distance), 2)    AS avg_distance_miles,
    ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
    ROUND(AVG(tip_amount), 2)       AS avg_tip,
    ROUND(AVG(passenger_count), 2)  AS avg_passengers
FROM fact_trips
WHERE year IN (2019, 2020)
GROUP BY year, month, hour, taxi_type
ORDER BY year, month, hour;
"""

# ─────────────────────────────────────────
# 2. AGG_BY_DAY — tendencia diaria (para graficas de linea en Power BI)
# ─────────────────────────────────────────
AGG_BY_DAY = """
CREATE TABLE IF NOT EXISTS agg_by_day AS
SELECT
    year,
    month,
    day_of_week,
    taxi_type,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(total_amount), 2)     AS avg_revenue_per_trip,
    ROUND(AVG(trip_distance), 2)    AS avg_distance_miles,
    ROUND(AVG(trip_duration_min), 2) AS avg_duration_min,
    ROUND(AVG(tip_amount), 2)       AS avg_tip
FROM fact_trips
WHERE year IN (2019, 2020)
GROUP BY year, month, day_of_week, taxi_type
ORDER BY year, month, day_of_week;
"""

# ─────────────────────────────────────────
# 3. AGG_BY_ZONE — viajes y revenue por zona de origen (PULocationID)
#    Esta se une con dim_zones para saber el nombre de la zona
# ─────────────────────────────────────────
AGG_BY_ZONE = """
CREATE TABLE IF NOT EXISTS agg_by_zone AS
SELECT
    t.year,
    t.taxi_type,
    t.PULocationID,
    z.Borough                       AS borough,
    z.Zone                          AS zone_name,
    z.service_zone,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(t.total_amount), 2)   AS total_revenue,
    ROUND(AVG(t.total_amount), 2)   AS avg_revenue_per_trip,
    ROUND(AVG(t.trip_distance), 2)  AS avg_distance_miles,
    ROUND(AVG(t.trip_duration_min), 2) AS avg_duration_min,
    ROUND(AVG(t.tip_amount), 2)     AS avg_tip
FROM fact_trips t
LEFT JOIN dim_zones z ON t.PULocationID = z.LocationID
WHERE t.year IN (2019, 2020)
GROUP BY t.year, t.taxi_type, t.PULocationID, z.Borough, z.Zone, z.service_zone
ORDER BY total_trips DESC;
"""

# ─────────────────────────────────────────
# 4. AGG_BY_BOROUGH — resumen ejecutivo por borough
#    KPIs de alto nivel para las tarjetas del dashboard
# ─────────────────────────────────────────
AGG_BY_BOROUGH = """
CREATE TABLE IF NOT EXISTS agg_by_borough AS
SELECT
    t.year,
    t.month,
    t.taxi_type,
    z.Borough                       AS borough,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(t.total_amount), 2)   AS total_revenue,
    ROUND(AVG(t.total_amount), 2)   AS avg_revenue_per_trip,
    ROUND(AVG(t.trip_distance), 2)  AS avg_distance_miles,
    ROUND(AVG(t.trip_duration_min), 2) AS avg_duration_min,
    ROUND(AVG(t.tip_amount), 2)     AS avg_tip,
    ROUND(SUM(t.tip_amount), 2)     AS total_tips,
    COUNT(DISTINCT t.PULocationID)  AS zones_active
FROM fact_trips t
LEFT JOIN dim_zones z ON t.PULocationID = z.LocationID
WHERE t.year IN (2019, 2020)
GROUP BY t.year, t.month, t.taxi_type, z.Borough
ORDER BY t.year, t.month, z.Borough;
"""

# ─────────────────────────────────────────
# 5. AGG_COVID_IMPACT — comparativa 2019 vs 2020 por mes
#    Para visualizar el impacto del COVID directamente en Power BI
# ─────────────────────────────────────────
AGG_COVID_IMPACT = """
CREATE TABLE IF NOT EXISTS agg_covid_impact AS
SELECT
    month,
    taxi_type,
    SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END)                        AS trips_2019,
    SUM(CASE WHEN year = 2020 THEN 1 ELSE 0 END)                        AS trips_2020,
    ROUND(SUM(CASE WHEN year = 2019 THEN total_amount ELSE 0 END), 2)   AS revenue_2019,
    ROUND(SUM(CASE WHEN year = 2020 THEN total_amount ELSE 0 END), 2)   AS revenue_2020,
    ROUND(
        (SUM(CASE WHEN year = 2020 THEN 1 ELSE 0 END) - SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END))
        * 100.0
        / NULLIF(SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END), 0)
    , 1)                                                                  AS pct_change_trips
FROM fact_trips
WHERE year IN (2019, 2020)
GROUP BY month, taxi_type
ORDER BY month, taxi_type;
"""

# ─────────────────────────────────────────
# 6. AGG_TOP_ROUTES — rutas mas populares (origen → destino)
#    Para el mapa de flujos en Power BI
# ─────────────────────────────────────────
AGG_TOP_ROUTES = """
CREATE TABLE IF NOT EXISTS agg_top_routes AS
SELECT
    t.year,
    t.taxi_type,
    t.PULocationID,
    pu.Zone                         AS pickup_zone,
    pu.Borough                      AS pickup_borough,
    t.DOLocationID,
    do_.Zone                        AS dropoff_zone,
    do_.Borough                     AS dropoff_borough,
    COUNT(*)                        AS total_trips,
    ROUND(AVG(t.total_amount), 2)   AS avg_revenue,
    ROUND(AVG(t.trip_distance), 2)  AS avg_distance_miles,
    ROUND(AVG(t.tip_amount), 2)     AS avg_tip
FROM fact_trips t
LEFT JOIN dim_zones pu  ON t.PULocationID = pu.LocationID
LEFT JOIN dim_zones do_ ON t.DOLocationID = do_.LocationID
WHERE t.year IN (2019, 2020)
GROUP BY t.year, t.taxi_type, t.PULocationID, pu.Zone, pu.Borough,
         t.DOLocationID, do_.Zone, do_.Borough
HAVING total_trips >= 100
ORDER BY total_trips DESC
LIMIT 500;
"""

# ─────────────────────────────────────────
# EXPORTAR dim_zones como CSV para Power BI
# ─────────────────────────────────────────
def exportar_zonas():
    print("\n>>> Exportando dim_zones a CSV...")
    df = pd.read_sql("SELECT * FROM dim_zones", engine)
    df.to_csv("dim_zones.csv", index=False)
    print(f"    ✓ dim_zones.csv exportado — {len(df)} filas")


# ─────────────────────────────────────────
# MAIN — ejecutar todo en orden
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  NYC Taxi — Creacion de Tablas Agregadas")
    print("=" * 55)

    tablas = [
        ("agg_by_hour",       AGG_BY_HOUR),
        ("agg_by_day",        AGG_BY_DAY),
        ("agg_by_zone",       AGG_BY_ZONE),
        ("agg_by_borough",    AGG_BY_BOROUGH),
        ("agg_covid_impact",  AGG_COVID_IMPACT),
        ("agg_top_routes",    AGG_TOP_ROUTES),
    ]

    inicio_total = time.time()

    for nombre, sql in tablas:
        run_query(nombre, sql)

    exportar_zonas()

    total = round(time.time() - inicio_total, 1)
    print(f"\n{'=' * 55}")
    print(f"  Todas las tablas listas en {total}s")
    print(f"  Tablas creadas: {len(tablas)}")
    print(f"  CSV exportado:  dim_zones.csv")
    print(f"{'=' * 55}")