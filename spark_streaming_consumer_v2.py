# -*- coding: utf-8 -*-
"""
Consumidor ampliado de Spark Structured Streaming (Tarea 3 - Big Data UNAD)
Nestor Miguel Pacheco Ferrer - 1129502093

Lee el topic sensor_data de Kafka y por cada ventana de 1 minuto y sensor calcula:
numero de lecturas, temperatura promedio, maxima y minima, humedad promedio y
cuantas lecturas superan 28 grados (alerta). Cada micro-lote se muestra en consola
y se guarda en CSV para graficarlo despues con graficar_streaming.py.

Ejecucion:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer_v2.py
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

SALIDA = "resultados_streaming"
os.makedirs(SALIDA, exist_ok=True)
UMBRAL = 28.0

spark = SparkSession.builder.appName("SensorDataAnalysisV2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType())
])

# fuente: Kafka (o una carpeta con JSON si se pasa como argumento, solo para pruebas)
if len(sys.argv) > 1:
    parsed = spark.readStream.schema(schema).json(sys.argv[1])
else:
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("subscribe", "sensor_data")
           .option("startingOffsets", "latest")
           .load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
                 .select("data.*"))

stats = (parsed
         .withWatermark("timestamp", "2 minutes")
         .groupBy(F.window("timestamp", "1 minute"), "sensor_id")
         .agg(F.count("*").alias("lecturas"),
              F.round(F.avg("temperature"), 2).alias("temp_prom"),
              F.round(F.max("temperature"), 2).alias("temp_max"),
              F.round(F.min("temperature"), 2).alias("temp_min"),
              F.round(F.avg("humidity"), 2).alias("hum_prom"),
              F.sum(F.when(F.col("temperature") > UMBRAL, 1).otherwise(0)).alias("alertas"))
         .select(F.col("window.start").alias("inicio"), F.col("window.end").alias("fin"),
                 "sensor_id", "lecturas", "temp_prom", "temp_max", "temp_min",
                 "hum_prom", "alertas"))

def procesar_lote(lote, epoch_id):
    lote = lote.orderBy("inicio", "sensor_id")
    total = lote.agg(F.sum("lecturas").alias("t"), F.sum("alertas").alias("a")).collect()[0]
    print(f"\n===== Micro-lote {epoch_id} | lecturas: {total['t']} | alertas > {UMBRAL} C: {total['a']} =====")
    lote.show(30, truncate=False)
    lote.toPandas().to_csv(f"{SALIDA}/lote_{epoch_id:04d}.csv", index=False)

query = (stats.writeStream
         .outputMode("complete")
         .foreachBatch(procesar_lote)
         .trigger(processingTime="10 seconds")
         .start())
query.awaitTermination()
