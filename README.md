# Tarea 3 - Procesamiento de datos con Apache Spark

Curso Big Data (202016911), UNAD
Estudiante: Néstor Miguel Pacheco Ferrer, código 1129502093

## Descripción

Análisis en batch del conjunto de datos de COVID-19 de Our World in Data (429.435 filas, 67 columnas, enero de 2020 a agosto de 2024) con PySpark. El script limpia los datos, calcula la letalidad y la mortalidad por millón, compara a Colombia con otros países de América Latina y mide la correlación entre vacunación, edad mediana, PIB y mortalidad. Al final genera cuatro gráficas y guarda las tablas de resultados en CSV.

La carpeta `streaming` tiene el productor de Kafka y los consumidores de Spark Structured Streaming del Anexo 3.

## Archivos

| Archivo | Contenido |
|---|---|
| `batch_covid_spark.py` | Procesamiento en batch con Spark |
| `Tarea3_batch_spark_colab.ipynb` | Misma solución para Google Colab |
| `streaming/kafka_producer.py` | Generador de lecturas simuladas de sensores |
| `streaming/spark_streaming_consumer.py` | Consumidor del Anexo 3 (promedios por ventana de 1 minuto) |
| `streaming/spark_streaming_consumer_v2.py` | Consumidor ampliado (conteo, máximos, mínimos, alertas, CSV por micro-lote) |
| `streaming/graficar_streaming.py` | Grafica el último micro-lote guardado |
| `Tarea3_streaming_kafka_colab.ipynb` | Proceso completo del Anexo 3 (Kafka + Spark Streaming) en Google Colab |

## Requisitos

- Apache Spark 3.5 o superior con Python 3
- matplotlib y pandas
- Para streaming: Apache Kafka 3.9.2 y `kafka-python`

## Ejecución del procesamiento en batch

```bash
wget https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv
spark-submit batch_covid_spark.py
```

Los resultados quedan en la carpeta `resultados/` (cuatro figuras PNG y cuatro CSV).

En Google Colab basta con abrir el notebook y ejecutar las celdas en orden.

## Ejecución del procesamiento en tiempo real

En Google Colab: abrir `Tarea3_streaming_kafka_colab.ipynb` y ejecutar las celdas en orden.

En una máquina con Kafka instalado:

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data

# terminal 1
python3 streaming/kafka_producer.py

# terminal 2 (la version del paquete debe coincidir con la de Spark: spark-submit --version)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/spark_streaming_consumer.py
# o el consumidor ampliado
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 streaming/spark_streaming_consumer_v2.py
python3 streaming/graficar_streaming.py
```

La interfaz web de Spark queda en `http://IP-de-la-maquina:4040`.

## Fuente de datos

Mathieu, E., Ritchie, H., Rodés-Guirao, L., Appel, C., Giattino, C., Hasell, J., Macdonald, B., Dattani, S., Beltekian, D., Ortiz-Ospina, E., & Roser, M. (2020). *Coronavirus pandemic (COVID-19)*. Our World in Data. https://ourworldindata.org/coronavirus
