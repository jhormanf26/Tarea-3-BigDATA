# Detección de tráfico de red malicioso — Big Data (Tarea 3)

**Curso:** Big Data — UNAD (202016911)
**Autor:** Jhorman Fuentes
**Dataset:** [CICIDS2017](https://www.unb.ca/cic/datasets/ids-2017.html) — University of New Brunswick (UNB–CIC)

## Descripción del problema

Análisis de patrones en tráfico de red para identificar comportamientos
anómalos asociados a ciberataques (DDoS, PortScan, Botnet, Brute Force,
Web Attacks), combinando procesamiento **batch** (análisis exploratorio
sobre el histórico completo) y procesamiento **en tiempo real**
(simulación de eventos de red mediante Kafka y conteo en ventanas de
tiempo con Spark Structured Streaming).

## Estructura del repositorio

```
.
├── README.md
├── requirements.txt
└── scripts/
    ├── batch_processing.py       # Procesamiento batch con PySpark
    ├── kafka_producer.py         # Productor Kafka (simula tráfico en tiempo real)
    └── streaming_processing.py   # Spark Structured Streaming (consumer)
```

## Requisitos

- Apache Spark 3.5.1
- Java 17 (Kafka) / Java 11 (Spark)
- Python 3.x
- Apache Kafka 4.2.2 (modo KRaft, sin Zookeeper)
- Dependencias de Python: `pip install -r requirements.txt`

## 1. Procesamiento batch

Descargar los archivos CSV del dataset CICIDS2017 y ejecutar:

```bash
spark-submit scripts/batch_processing.py \
    --input /ruta/a/los/csv \
    --output ./output
```

Esto genera en `./output`:
- `distribucion_ataques.csv` y `distribucion_ataques.png`: distribución de tipos de tráfico/ataque.
- `estadisticas_protocolo.csv`: estadísticas agregadas por protocolo.
- `resumen_estadistico.csv`: estadísticas descriptivas de las variables numéricas clave.

## 2. Procesamiento en tiempo real (Kafka + Spark Streaming)

**Paso 1 — Iniciar el broker de Kafka** (modo KRaft, ya configurado):

```bash
bin/kafka-server-start.sh config/server.properties
```

**Paso 2 — Crear el topic** (una sola vez):

```bash
bin/kafka-topics.sh --create --topic network_traffic --bootstrap-server localhost:9092
```

**Paso 3 — Iniciar el consumidor de Spark Streaming** (en una terminal):

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
    scripts/streaming_processing.py --bootstrap-server localhost:9092
```

**Paso 4 — Iniciar el productor** (en otra terminal), simulando la llegada de eventos de red a partir de un archivo del dataset:

```bash
pip install kafka-python
python3 scripts/kafka_producer.py \
    --csv /ruta/a/Tuesday-WorkingHours.pcap_ISCX.csv \
    --bootstrap-server localhost:9092 \
    --delay 0.3
```

El consumidor de Spark Streaming mostrará en consola el conteo de
eventos por tipo de ataque agrupados en ventanas de 1 minuto, actualizado
en tiempo real a medida que el productor publica nuevos eventos.

## Resultados esperados

- **Batch:** distribución general de tráfico benigno vs. distintos tipos de ataque en todo el histórico del dataset, y estadísticas por protocolo (TCP, UDP, etc.).
- **Streaming:** conteo incremental, por ventana de 1 minuto, de cuántos eventos de cada tipo de ataque se reciben, simulando un escenario de monitoreo de seguridad en tiempo real.

## Autor

Jhorman Fuentes — Grupo 202016911_46
