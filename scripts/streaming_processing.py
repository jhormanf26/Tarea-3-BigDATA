"""
streaming_processing.py
------------------------
Tarea 3 - Big Data (UNAD, 202016911)
Autor: Jhorman Fuentes

Procesamiento en tiempo real con Spark Structured Streaming: consume los
eventos de tráfico de red publicados por kafka_producer.py en el topic
"network_traffic" y realiza un conteo de eventos por tipo de ataque en
ventanas de tiempo de 1 minuto.

Uso:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
        streaming_processing.py --bootstrap-server localhost:9092

Nota:
    El paquete spark-sql-kafka debe coincidir con la versión de Spark y
    la versión de Scala con la que fue compilado (2.13 en este caso,
    según Spark 3.5.1).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)


# Esquema del evento JSON publicado por kafka_producer.py
EVENT_SCHEMA = StructType(
    [
        StructField("timestamp", DoubleType(), True),
        StructField("destination_port", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("flow_duration", StringType(), True),
        StructField("total_fwd_packets", StringType(), True),
        StructField("total_backward_packets", StringType(), True),
        StructField("attack_type", StringType(), True),
    ]
)


def build_spark_session(app_name="CICIDS2017-StreamingProcessing"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    parser = argparse.ArgumentParser(
        description="Procesamiento en tiempo real de tráfico de red con Spark Structured Streaming"
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Dirección del broker de Kafka (host:puerto)",
    )
    parser.add_argument("--topic", default="network_traffic", help="Topic de origen")
    parser.add_argument(
        "--window",
        default="1 minute",
        help="Tamaño de la ventana de tiempo para el conteo (por defecto: 1 minute)",
    )
    parser.add_argument(
        "--watermark",
        default="2 minutes",
        help="Tolerancia a eventos tardíos (watermark)",
    )
    parser.add_argument(
        "--checkpoint",
        default="/tmp/spark-checkpoints/network_traffic",
        help="Ruta de checkpoint para el streaming",
    )
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Suscribiéndose al topic '{args.topic}' en {args.bootstrap_server} ...")

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # El valor del mensaje Kafka llega como bytes (columna "value"); se
    # convierte a texto y se parsea como JSON según el esquema definido.
    df_eventos = (
        df_kafka.selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp").cast("double")))
    )

    # Conteo de eventos por tipo de ataque, agrupado en ventanas de tiempo
    conteo_por_ventana = (
        df_eventos.withWatermark("event_time", args.watermark)
        .groupBy(
            F.window(F.col("event_time"), args.window),
            F.col("attack_type"),
        )
        .count()
        .orderBy("window")
    )

    query = (
        conteo_por_ventana.writeStream.outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", args.checkpoint)
        .start()
    )

    print("[INFO] Procesamiento en streaming iniciado. Esperando eventos... (Ctrl+C para detener)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
