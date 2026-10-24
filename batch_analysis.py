"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con PySpark
Dataset: NYC Yellow Taxi Trip Records (formato Parquet)

Uso:
    python batch_analysis.py data/yellow_tripdata_2024-01.parquet [data/taxi_zone_lookup.csv]

El segundo argumento (catalogo de zonas de la TLC) es opcional: si existe,
las zonas se muestran con su nombre en lugar de solo el identificador.
"""
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")  # permite guardar graficas sin interfaz grafica
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "data/yellow_tripdata_2024-01.parquet"
RUTA_ZONAS = sys.argv[2] if len(sys.argv) > 2 else "data/taxi_zone_lookup.csv"
SALIDA = "resultados"
os.makedirs(SALIDA, exist_ok=True)

# Rangos de limpieza (justificacion en el README)
DIST_MAX_MILLAS = 100      # un viaje urbano nunca llega a esa distancia
TARIFA_MAX_USD = 500       # tarifas mayores son casi seguro errores de captura
PASAJEROS_MIN, PASAJEROS_MAX = 1, 6   # capacidad maxima de un taxi
DURACION_MIN, DURACION_MAX = 1, 180   # minutos
VELOCIDAD_MAX_KMH = 120    # limite fisico razonable en ciudad

spark = (
    SparkSession.builder.appName("Tarea3_Batch_Taxis")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------ 1. CARGA
df = spark.read.parquet(RUTA)
total_inicial = df.count()
print(f"\n=== CARGA ===\nRegistros iniciales: {total_inicial:,}")
df.printSchema()

df = df.select(
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "PULocationID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
)

# ---------------------------------------------------------------- 2. LIMPIEZA
# Se registra cuantos registros elimina cada paso.
log = []
restantes = total_inicial


def paso(nombre, nuevo_df):
    """Aplica un paso de limpieza y registra cuantos registros elimino."""
    global restantes
    nuevo_df = nuevo_df.cache()
    n = nuevo_df.count()
    log.append((nombre, restantes - n, n))
    restantes = n
    return nuevo_df


df = paso("Eliminar nulos", df.dropna())
df = paso("Eliminar duplicados", df.dropDuplicates())
df = paso(f"Distancia entre 0 y {DIST_MAX_MILLAS} millas",
          df.filter((F.col("trip_distance") > 0) & (F.col("trip_distance") < DIST_MAX_MILLAS)))
df = paso(f"Tarifas entre 0 y {TARIFA_MAX_USD} USD",
          df.filter((F.col("fare_amount") > 0) & (F.col("fare_amount") < TARIFA_MAX_USD)
                    & (F.col("total_amount") > 0) & (F.col("total_amount") < TARIFA_MAX_USD)))
df = paso(f"Pasajeros entre {PASAJEROS_MIN} y {PASAJEROS_MAX}",
          df.filter((F.col("passenger_count") >= PASAJEROS_MIN)
                    & (F.col("passenger_count") <= PASAJEROS_MAX)))

# ------------------------------------------------------------ 3. TRANSFORMACION
df = (
    df.withColumn(
        "duracion_min",
        (F.unix_timestamp("tpep_dropoff_datetime")
         - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .withColumn("dia_semana", F.date_format("tpep_pickup_datetime", "E"))
    # Lunes=1 ... Domingo=7 (dayofweek de Spark: Domingo=1)
    .withColumn("dia_num", ((F.dayofweek("tpep_pickup_datetime") + 5) % 7) + 1)
    .withColumn("velocidad_kmh",
                F.col("trip_distance") * 1.609 / (F.col("duracion_min") / 60))
)
df = paso(f"Duracion entre {DURACION_MIN} y {DURACION_MAX} minutos",
          df.filter((F.col("duracion_min") >= DURACION_MIN)
                    & (F.col("duracion_min") <= DURACION_MAX)))
df = paso(f"Velocidad menor a {VELOCIDAD_MAX_KMH} km/h",
          df.filter(F.col("velocidad_kmh") < VELOCIDAD_MAX_KMH))

total_final = restantes
print("\n=== LIMPIEZA: REGISTROS ELIMINADOS POR PASO ===")
print(f"{'Paso':<42}{'Eliminados':>12}{'Restantes':>12}")
for nombre, elim, rest in log:
    print(f"{nombre:<42}{elim:>12,}{rest:>12,}")
print(f"\nTotal descartado: {total_inicial - total_final:,} "
      f"({(total_inicial - total_final) / total_inicial:.1%} del original)")
print(f"Registros finales: {total_final:,}")

with open(f"{SALIDA}/log_limpieza.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["paso", "eliminados", "restantes"])
    w.writerows(log)

# ------------------------------------------------------------------- 4. EDA
print("\n=== ESTADISTICAS DESCRIPTIVAS ===")
df.select("trip_distance", "duracion_min", "fare_amount", "tip_amount", "total_amount").describe().show()

viajes_hora = df.groupBy("hora").count().orderBy("hora")
print("=== VIAJES POR HORA ===")
viajes_hora.show(24)

viajes_dia = (
    df.groupBy("dia_num", "dia_semana")
    .agg(F.count("*").alias("viajes"),
         F.round(F.avg("total_amount"), 2).alias("tarifa_promedio"))
    .orderBy("dia_num")   # Lunes a Domingo
)
print("=== VIAJES POR DIA DE LA SEMANA (Lunes a Domingo) ===")
viajes_dia.show()

top_zonas = df.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(10)
if os.path.exists(RUTA_ZONAS):
    zonas = spark.read.csv(RUTA_ZONAS, header=True, inferSchema=True)
    top_zonas = (
        top_zonas.join(zonas, top_zonas.PULocationID == zonas.LocationID, "left")
        .select("PULocationID", "Borough", "Zone", "count")
        .orderBy(F.desc("count"))
    )
    print("=== TOP 10 ZONAS DE RECOGIDA ===")
else:
    print(f"(No se encontro {RUTA_ZONAS}: se muestran solo los identificadores)")
    print("=== TOP 10 ZONAS DE RECOGIDA ===")
top_zonas.show(truncate=False)

pagos = df.groupBy("payment_type").agg(
    F.count("*").alias("viajes"),
    F.round(F.avg("tip_amount"), 2).alias("propina_promedio"),
)
print("=== METODO DE PAGO ===")
pagos.show()

print("Correlacion distancia vs tarifa:",
      round(df.stat.corr("trip_distance", "fare_amount"), 3))

# ------------------------------------------------------- 5. VISUALIZACION
ph = viajes_hora.toPandas()
plt.figure(figsize=(9, 4))
plt.bar(ph["hora"], ph["count"], color="#1f77b4")
plt.title("Viajes de taxi por hora del dia")
plt.xlabel("Hora")
plt.ylabel("Cantidad de viajes")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.savefig(f"{SALIDA}/viajes_por_hora.png", dpi=150)
plt.close()

pd_dia = viajes_dia.toPandas()
plt.figure(figsize=(8, 4))
plt.bar(pd_dia["dia_semana"], pd_dia["viajes"], color="#ff7f0e")
plt.title("Viajes de taxi por dia de la semana")
plt.xlabel("Dia")
plt.ylabel("Cantidad de viajes")
plt.tight_layout()
plt.savefig(f"{SALIDA}/viajes_por_dia.png", dpi=150)
plt.close()

pz = top_zonas.toPandas()
etiquetas = (pz["Zone"].astype(str) if "Zone" in pz.columns
             else pz["PULocationID"].astype(str))
plt.figure(figsize=(9, 5))
plt.barh(etiquetas, pz["count"], color="#2ca02c")
plt.title("Top 10 zonas de recogida")
plt.xlabel("Cantidad de viajes")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{SALIDA}/top_zonas.png", dpi=150)
plt.close()

muestra = df.select("trip_distance", "fare_amount").sample(False, 0.01, seed=42).toPandas()
plt.figure(figsize=(7, 5))
plt.scatter(muestra["trip_distance"], muestra["fare_amount"], s=3, alpha=0.3)
plt.title("Distancia vs tarifa (muestra del 1%)")
plt.xlabel("Distancia (millas)")
plt.ylabel("Tarifa (USD)")
plt.tight_layout()
plt.savefig(f"{SALIDA}/distancia_vs_tarifa.png", dpi=150)
plt.close()

print(f"\nGraficas y log de limpieza guardados en la carpeta '{SALIDA}/'")
spark.stop()
