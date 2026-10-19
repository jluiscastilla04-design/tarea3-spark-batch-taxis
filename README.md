# Procesamiento en batch con Apache Spark (PySpark)

Tarea 3 - Big Data (UNAD, código 202016911).

## Problema

Analizar el comportamiento de los viajes de taxi amarillo de Nueva York para identificar
en qué horas y días hay más demanda, cuáles son las zonas de recogida más activas y cómo se
relacionan la distancia, la tarifa y las propinas. Este tipo de análisis ayuda a planear la
oferta de vehículos según la demanda.

## Dataset

- Nombre: NYC Yellow Taxi Trip Records
- Fuente: NYC Taxi & Limousine Commission (TLC)
- Formato: Parquet (un archivo mensual tiene más de 2 millones de registros)
- Descarga: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Ejemplo (enero 2024):

```bash
mkdir -p data
wget -P data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

## Requisitos

- Python 3.9 o superior
- Java 8, 11 o 17 (necesario para Spark)

```bash
pip install -r requirements.txt
```

## Ejecución

```bash
python batch_analysis.py data/yellow_tripdata_2024-01.parquet
```

Las gráficas se guardan en la carpeta `resultados/`.

## Qué hace el código

1. **Carga** del archivo Parquet con `spark.read.parquet`.
2. **Limpieza**: elimina nulos y duplicados, y filtra valores imposibles (distancias, tarifas,
   pasajeros y duraciones fuera de rango).
3. **Transformación**: crea las columnas `duracion_min`, `hora`, `dia_semana` y `velocidad_kmh`.
4. **EDA**: estadísticas descriptivas, viajes por hora y por día, top 10 de zonas, método de
   pago y correlación distancia vs. tarifa.
5. **Visualización**: cuatro gráficas con matplotlib guardadas en `resultados/`.

## Estructura

```
.
├── batch_analysis.py
├── requirements.txt
├── README.md
├── data/          (no se sube al repositorio)
└── resultados/    (gráficas generadas)
```

## Autor

Jaime Luis Castilla Cabarcas - Integrante 3 del grupo.
