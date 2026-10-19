"""
Tarea 3 - Big Data (UNAD) - Procesamiento en batch con PySpark
Dataset: NYC Yellow Taxi Trip Records (formato Parquet)

Uso:
    python batch_analysis.py data/yellow_tripdata_2024-01.parquet
"""
import os
import sys

import matplotlib
matplotlib.use("Agg")  # permite guardar graficas sin interfaz grafica
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "data/yellow_tripdata_2024-01.parquet"
SALIDA = "resultados"
os.makedirs(SALIDA, exist_ok=True)

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

# ---------------------------------------------------------------- 2. LIMPIEZA
df = df.select(
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "PULocationID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
)

df = df.dropna()          # elimina nulos
df = df.dropDuplicates()  # elimina duplicados

# Filtros de calidad: valores imposibles o atipicos
df = df.filter(
    (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100)
    & (F.col("fare_amount") > 0) & (F.col("total_amount") > 0)
    & (F.col("passenger_count") > 0) & (F.col("passenger_count") <= 6)
)

# ------------------------------------------------------------ 3. TRANSFORMACION
df = (
    df.withColumn(
        "duracion_min",
        (F.unix_timestamp("tpep_dropoff_datetime")
         - F.unix_timestamp("tpep_pickup_datetime")) / 60,
    )
    .filter((F.col("duracion_min") > 1) & (F.col("duracion_min") < 180))
    .withColumn("hora", F.hour("tpep_pickup_datetime"))
    .withColumn("dia_semana", F.date_format("tpep_pickup_datetime", "E"))
    .withColumn("velocidad_kmh", F.col("trip_distance") * 1.609 / (F.col("duracion_min") / 60))
    .filter(F.col("velocidad_kmh") < 120)
)
df.cache()

total_final = df.count()
print(f"\n=== LIMPIEZA ===\nRegistros tras limpieza: {total_final:,}")
print(f"Registros descartados: {total_inicial - total_final:,}")

# ------------------------------------------------------------------- 4. EDA
print("\n=== ESTADISTICAS DESCRIPTIVAS ===")
df.select("trip_distance", "duracion_min", "fare_amount", "tip_amount", "total_amount").describe().show()

viajes_hora = df.groupBy("hora").count().orderBy("hora")
print("=== VIAJES POR HORA ===")
viajes_hora.show(24)

orden_dias = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
viajes_dia = df.groupBy("dia_semana").agg(
    F.count("*").alias("viajes"),
    F.round(F.avg("total_amount"), 2).alias("tarifa_promedio"),
)
print("=== VIAJES POR DIA DE LA SEMANA ===")
viajes_dia.show()

top_zonas = (
    df.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(10)
)
print("=== TOP 10 ZONAS DE RECOGIDA ===")
top_zonas.show()

pagos = df.groupBy("payment_type").agg(
    F.count("*").alias("viajes"),
    F.round(F.avg("tip_amount"), 2).alias("propina_promedio"),
)
print("=== METODO DE PAGO ===")
pagos.show()

print("Correlacion distancia vs tarifa:",
      round(df.stat.corr("trip_distance", "fare_amount"), 3))

# ------------------------------------------------------- 5. VISUALIZACION
# Los agregados son pequenos, por eso es seguro pasarlos a pandas.
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
pd_dia["dia_semana"] = pd_dia["dia_semana"].astype("category").cat.set_categories(orden_dias)
pd_dia = pd_dia.sort_values("dia_semana")
plt.figure(figsize=(8, 4))
plt.bar(pd_dia["dia_semana"], pd_dia["viajes"], color="#ff7f0e")
plt.title("Viajes de taxi por dia de la semana")
plt.xlabel("Dia")
plt.ylabel("Cantidad de viajes")
plt.tight_layout()
plt.savefig(f"{SALIDA}/viajes_por_dia.png", dpi=150)
plt.close()

pz = top_zonas.toPandas()
plt.figure(figsize=(8, 4))
plt.barh(pz["PULocationID"].astype(str), pz["count"], color="#2ca02c")
plt.title("Top 10 zonas de recogida (PULocationID)")
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

print(f"\nGraficas guardadas en la carpeta '{SALIDA}/'")
spark.stop()
