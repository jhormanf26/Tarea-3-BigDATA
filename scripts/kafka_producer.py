"""
kafka_producer.py
------------------
Tarea 3 - Big Data (UNAD, 202016911)
Autor: Jhorman Fuentes

Productor de Kafka que simula la llegada en tiempo real de eventos de
tráfico de red, tomando muestras del dataset CICIDS2017 y publicándolas
en el topic "network_traffic".

Uso:
    python kafka_producer.py --csv /ruta/al/archivo.csv --bootstrap-server localhost:9092 --delay 0.5

Requisitos:
    pip install kafka-python
"""

import argparse
import csv
import json
import time
import random

from kafka import KafkaProducer


def limpiar_clave(nombre: str) -> str:
    return nombre.strip().replace(" ", "_").replace("/", "_")


def construir_evento(fila: dict) -> dict:
    """Extrae y normaliza los campos relevantes de una fila del CSV
    para construir el evento de red que se publicará en Kafka."""

    fila_normalizada = {limpiar_clave(k): v for k, v in fila.items() if k is not None}

    evento = {
        "timestamp": time.time(),
        "destination_port": fila_normalizada.get("Destination_Port", ""),
        "protocol": fila_normalizada.get("Protocol", ""),
        "flow_duration": fila_normalizada.get("Flow_Duration", ""),
        "total_fwd_packets": fila_normalizada.get("Total_Fwd_Packets", ""),
        "total_backward_packets": fila_normalizada.get("Total_Backward_Packets", ""),
        "attack_type": fila_normalizada.get("Label", "BENIGN"),
    }
    return evento


def main():
    parser = argparse.ArgumentParser(
        description="Productor Kafka que simula tráfico de red en tiempo real"
    )
    parser.add_argument("--csv", required=True, help="Archivo CSV de origen (CICIDS2017)")
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Dirección del broker de Kafka (host:puerto)",
    )
    parser.add_argument("--topic", default="network_traffic", help="Topic de destino")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Segundos de espera entre el envío de cada evento (simula tiempo real)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Número máximo de eventos a enviar (0 = todo el archivo)",
    )
    args = parser.parse_args()

    print(f"[INFO] Conectando al broker Kafka en {args.bootstrap_server} ...")
    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    enviados = 0
    with open(args.csv, newline="", encoding="utf-8", errors="ignore") as f:
        lector = csv.DictReader(f)
        for fila in lector:
            evento = construir_evento(fila)
            producer.send(args.topic, value=evento)
            enviados += 1

            if enviados % 50 == 0:
                print(f"[INFO] {enviados} eventos enviados... último: {evento}")

            # Pequeña variación aleatoria para simular llegada real (no perfectamente uniforme)
            time.sleep(args.delay * random.uniform(0.7, 1.3))

            if args.limit and enviados >= args.limit:
                break

    producer.flush()
    producer.close()
    print(f"[INFO] Finalizado. Total de eventos publicados en '{args.topic}': {enviados}")


if __name__ == "__main__":
    main()
