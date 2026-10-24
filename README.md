# Procesamiento en batch con Apache Spark (PySpark)

Tarea 3 - Big Data (UNAD, código 202016911).

## Problema

Analizar el comportamiento de los viajes de taxi amarillo de Nueva York para identificar
en qué horas y días hay más demanda, cuáles son las zonas de recogida más activas y cómo se
relacionan la distancia, la tarifa y las propinas. Este tipo de análisis ayuda a planear la
oferta de vehículos según la demanda.

## Dataset

- Nombre: NYC Yellow Taxi Trip Records (enero 2024)
- Fuente: NYC Taxi & Limousine Commission (TLC)
- Formato: Parquet, 2.964.624 registros y 19 columnas
- Página de descarga: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Catálogo de zonas (opcional, traduce los identificadores a nombres): `taxi_zone_lookup.csv`,
  disponible en la misma página de la TLC.

Descarga en Windows (cmd):

```
mkdir data
curl -L -C - -o data\yellow_tripdata_2024-01.parquet https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
curl -L -o data\taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

## Requisitos

- Python 3.9 o superior
- Java 17 (con Java 26 Spark falla al iniciar: `ClassNotFoundException: jdk.internal.ref.Cleaner`)

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecución

```
python batch_analysis.py data\yellow_tripdata_2024-01.parquet data\taxi_zone_lookup.csv
```

El segundo argumento es opcional. Los resultados (gráficas y `log_limpieza.csv`) se guardan en `resultados/`.

## Qué hace el código

1. **Carga** del archivo Parquet con `spark.read.parquet`.
2. **Limpieza** en pasos, registrando cuántos registros elimina cada uno (tabla impresa y
   guardada en `resultados/log_limpieza.csv`).
3. **Transformación**: columnas `duracion_min`, `hora`, `dia_semana`, `dia_num` y `velocidad_kmh`.
4. **EDA**: estadísticas descriptivas, viajes por hora, por día (de lunes a domingo), top 10 de
   zonas, método de pago y correlación distancia vs. tarifa.
5. **Visualización**: cuatro gráficas con matplotlib.

## Criterios de limpieza

| Paso | Criterio | Justificación |
|------|----------|---------------|
| Nulos y duplicados | Se eliminan | Registros incompletos o repetidos distorsionan los conteos |
| Distancia | 0 < d < 100 millas | Una distancia de 0 no es un viaje; más de 100 millas es inusual en un servicio urbano |
| Tarifa y total | Entre 0 y 500 USD | Valores negativos o en 0 son ajustes o errores; sobre 500 se consideran errores de captura |
| Pasajeros | De 1 a 6 | 0 indica dato no ingresado; 6 es la capacidad máxima de un taxi |
| Duración | De 1 a 180 minutos | Menos de 1 minuto suele ser una cancelación; más de 3 horas es poco realista en la ciudad |
| Velocidad | Menor a 120 km/h | Por encima no es físicamente razonable en tráfico urbano |

Los umbrales son decisiones de criterio del análisis, definidas como constantes al inicio del script
para poder ajustarlas.

## Estructura

```
.
├── batch_analysis.py
├── requirements.txt
├── README.md
├── data/          (no se sube al repositorio)
└── resultados/    (gráficas y log de limpieza)
```

## Autor

Jaime Luis Castilla Cabarcas - Integrante 3 del grupo.
