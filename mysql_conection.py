"""
¡¡¡ Asegurate de tener instaladas las librerías que se utilizan en este script !!!

Aquí te lo dejo pa`ra que lo puedas ejecutar en tu terminal!

pip install os
pip install time
pip install mysql.connector
pip install pathlib
"""

import os
import time
import mysql.connector
from pathlib import Path

# ⚙️ Configuración de la base de datos
DB_CFG = dict(
    host="localhost", # Cambia localhost por el host de tu base de datos
    user="root", # Cambia root por el usuario de tu base de datos
    password="1234pass", # Cambia password por la contraseña de tu base de datos
    database="delivery_project", # Cambia tu_proyecto por el nombre de tu base de datos
    allow_local_infile=True,   # Te permite cargar archivos locales
)

CSV_PATH = Path(r"E:\ProyectosBBDD\proyecto_delivery\orders.csv").resolve() # Sirve para normalizar la ruta

assert CSV_PATH.exists(), f"Archivo no encontrado: {CSV_PATH}" #Lanza un AssertionError si el archivo no existe

TABLE_NAME = "orders"

# 📊 Ajusta las columnas según tu CSV
CREATE_SQL = f"""
    DROP TABLE IF EXISTS {TABLE_NAME};
    CREATE TABLE {TABLE_NAME} (
        order_id                    INT,
        store_id                    INT,
        channel_id                  INT,
        payment_order_id            INT,
        delivery_order_id           INT,
        order_status                VARCHAR(50),
        order_amount                FLOAT,
        order_delivery_fee          FLOAT,
        order_delivery_cost         FLOAT,
        order_created_hour          INT,
        order_created_minute        INT,
        order_created_day           INT,
        order_created_month         INT,
        order_created_year          INT,
        order_moment_created        DATETIME,
        order_moment_accepted       DATETIME,
        order_moment_ready          DATETIME,
        order_moment_collected      DATETIME,
        order_moment_in_expedition  DATETIME,
        order_moment_delivering     DATETIME,
        order_moment_delivered      DATETIME,
        order_moment_finished       DATETIME,
        order_metric_collected_time FLOAT,
        order_metric_paused_time    FLOAT,
        order_metric_production_time FLOAT,
        order_metric_walking_time    FLOAT,
        order_metric_expediton_speed_time FLOAT,
        order_metric_transit_time   FLOAT,
        order_metric_cycle_time     FLOAT,
        PRIMARY KEY (order_id)
    ) ENGINE=InnoDB;
    """

# 📊 Carga de datos
load_path = CSV_PATH.as_posix()
LOAD_SQL = f"""
    LOAD DATA LOCAL INFILE '{load_path}'
    INTO TABLE {TABLE_NAME}
    FIELDS TERMINATED BY ','  ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;            -- salta la cabecera
    """


# 🔌 Conexión a MySQL
print("🔌 Conectando a MySQL…")
try:
    cnx = mysql.connector.connect(**DB_CFG)
    cur = cnx.cursor()
    print("✅ Conexión establecida.")
except mysql.connector.Error as err:
    print(f"❌ ERROR: {err}")
    exit(1)


# 🛠️ Creación de tabla
print(f"🛠️  Creando tabla `{TABLE_NAME}`…")
tic = time.perf_counter()
for stmt in CREATE_SQL.strip().split(";"):
    if stmt.strip():
        cur.execute(stmt)
cnx.commit()
toc = time.perf_counter()
print(f"🏁 Tabla lista en {toc - tic:0.2f} s.")


# 🚚 Ingesta CSV
print(f"🚚 Iniciando LOAD DATA de {os.path.basename(CSV_PATH)}…")
tic = time.perf_counter()
cur.execute("SET GLOBAL local_infile = 1")
cnx.commit()
cur.execute(LOAD_SQL)
cnx.commit()
toc = time.perf_counter()
rows = cur.rowcount
print(f"📈 {rows:,} filas cargadas en {toc - tic:0.2f} s "
      f"({rows/(toc-tic):,.0f} filas/s).")



# ✅ Cierre limpio
cur.close()
cnx.close()
print("🔒 Conexión cerrada. Pipeline completado sin drama.")

