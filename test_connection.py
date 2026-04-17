from sqlalchemy import create_engine, text

# Reemplaza TU_IP_WSL con tu IP real
engine = create_engine("mysql+pymysql://kristian:taxi2024@127.0.0.1:3306/nyc_taxi_db")

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 'Conexion exitosa!' as mensaje"))
        for row in result:
            print(row[0])
except Exception as e:
    print(f"Error: {e}")