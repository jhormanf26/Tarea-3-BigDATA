"""
batch_processing.py
--------------------
Tarea 3 - Big Data (UNAD, 202016911)
Autor: Jhorman Fuentes

Procesamiento en batch del dataset CICIDS2017 usando Apache Spark (PySpark).

Objetivo:
    Cargar los archivos CSV del dataset CICIDS2017, limpiar los datos
    (nulos y duplicados), realizar un análisis exploratorio (EDA) y
    generar visualizaciones sobre la distribución de ataques y
    estadísticas por protocolo de red.

Uso:
    spark-submit batch_processing.py --input /ruta/a/los/csv --output /ruta/de/salida

Requisitos:
    - Apache Spark 3.5.x
    - Python 3.x con pyspark, pandas y matplotlib instalados en el driver
      (para la etapa de visualización, que se hace sobre un resumen ya
      agregado y por lo tanto liviano para el driver)
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def build_spark_session(app_name="CICIDS2017-BatchProcessing"):
    """Crea (o recupera) la sesión de Spark."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def clean_column_name(name: str) -> str:
    """Normaliza nombres de columna: quita espacios y caracteres especiales.

    El dataset CICIDS2017 trae nombres de columna con espacios al inicio
    (' Destination Port', ' Protocol', ' Label', etc.), lo cual es
    inconveniente para referenciarlas en Spark SQL.
    """
    return (
        name.strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace(".", "")
        .replace("(", "")
        .replace(")", "")
    )


def load_dataset(spark, input_path: str):
    """Carga todos los archivos CSV del dataset en un único DataFrame."""
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Normalizar nombres de columnas
    renamed_cols = [clean_column_name(c) for c in df.columns]
    df = df.toDF(*renamed_cols)
    return df


def clean_dataset(df):
    """Limpieza de nulos, duplicados y valores no numéricos (Infinity/NaN).

    El dataset CICIDS2017 es conocido por contener valores 'Infinity' y
    'NaN' como texto en columnas numéricas (p. ej. Flow Bytes/s), producto
    de divisiones por cero en el cálculo de los flujos de red.
    """
    total_inicial = df.count()

    # Eliminar filas totalmente duplicadas
    df = df.dropDuplicates()

    # Reemplazar valores no numéricos conocidos por null en columnas numéricas
    numeric_cols = [
        f.name for f in df.schema.fields if isinstance(f.dataType, DoubleType)
    ]
    for c in numeric_cols:
        df = df.withColumn(
            c,
            F.when(F.col(c).isin("Infinity", "-Infinity", "NaN"), None).otherwise(
                F.col(c)
            ).cast(DoubleType()),
        )

    # Eliminar filas con nulos en la columna de etiqueta (Label) y en columnas clave
    if "Label" in df.columns:
        df = df.filter(F.col("Label").isNotNull())

    # Eliminar filas con nulos en todas las columnas numéricas relevantes
    df = df.dropna(how="any", subset=numeric_cols) if numeric_cols else df

    total_final = df.count()
    print(f"[INFO] Registros iniciales: {total_inicial}")
    print(f"[INFO] Registros después de limpieza: {total_final}")
    print(f"[INFO] Registros eliminados: {total_inicial - total_final}")

    return df


def analisis_exploratorio(df, output_path):
    """Genera estadísticas descriptivas y las guarda como CSV."""
    os.makedirs(output_path, exist_ok=True)

    # 1. Distribución de tipos de ataque (incluye BENIGN como tráfico normal)
    if "Label" in df.columns:
        distribucion_ataques = (
            df.groupBy("Label")
            .count()
            .withColumnRenamed("count", "total_registros")
            .orderBy(F.desc("total_registros"))
        )
        distribucion_ataques.show(truncate=False)
        distribucion_ataques.toPandas().to_csv(
            os.path.join(output_path, "distribucion_ataques.csv"), index=False
        )

    # 2. Estadísticas por protocolo
    if "Protocol" in df.columns:
        stats_protocolo = (
            df.groupBy("Protocol")
            .agg(
                F.count("*").alias("total_flujos"),
                F.avg("Flow_Duration").alias("duracion_promedio")
                if "Flow_Duration" in df.columns
                else F.lit(None).alias("duracion_promedio"),
            )
            .orderBy(F.desc("total_flujos"))
        )
        stats_protocolo.show(truncate=False)
        stats_protocolo.toPandas().to_csv(
            os.path.join(output_path, "estadisticas_protocolo.csv"), index=False
        )

    # 3. Estadísticas descriptivas generales de columnas numéricas clave
    columnas_interes = [
        c
        for c in [
            "Flow_Duration",
            "Total_Fwd_Packets",
            "Total_Backward_Packets",
            "Flow_Bytes_s",
            "Flow_Packets_s",
        ]
        if c in df.columns
    ]
    if columnas_interes:
        resumen = df.select(columnas_interes).describe()
        resumen.show(truncate=False)
        resumen.toPandas().to_csv(
            os.path.join(output_path, "resumen_estadistico.csv"), index=False
        )

    return distribucion_ataques if "Label" in df.columns else None


def generar_visualizaciones(distribucion_ataques_df, output_path):
    """Genera una gráfica de barras con la distribución de ataques.

    Se usa pandas + matplotlib sobre el resultado ya agregado (pequeño),
    nunca sobre el DataFrame distribuido completo.
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        pdf = distribucion_ataques_df.toPandas()
        pdf = pdf.sort_values("total_registros", ascending=False).head(15)

        plt.figure(figsize=(10, 6))
        plt.bar(pdf["Label"], pdf["total_registros"], color="#2E75B6")
        plt.xticks(rotation=45, ha="right")
        plt.ylabel("Cantidad de registros")
        plt.title("Distribución de tipos de tráfico / ataques - CICIDS2017")
        plt.tight_layout()

        ruta_imagen = os.path.join(output_path, "distribucion_ataques.png")
        plt.savefig(ruta_imagen, dpi=150)
        print(f"[INFO] Gráfica guardada en: {ruta_imagen}")
    except ImportError:
        print(
            "[WARN] matplotlib no está disponible en el driver; se omite la "
            "generación de la gráfica. Puede generarse luego con los CSV "
            "exportados en la carpeta de salida."
        )


def main():
    parser = argparse.ArgumentParser(
        description="Procesamiento batch del dataset CICIDS2017 con PySpark"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Ruta a la carpeta con los archivos CSV del dataset CICIDS2017",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Ruta de salida para los resultados del análisis",
    )
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Cargando dataset desde: {args.input}")
    df = load_dataset(spark, args.input)
    print(f"[INFO] Columnas detectadas: {df.columns}")

    print("[INFO] Limpiando dataset (nulos y duplicados)...")
    df_limpio = clean_dataset(df)

    print("[INFO] Ejecutando análisis exploratorio (EDA)...")
    distribucion_ataques = analisis_exploratorio(df_limpio, args.output)

    if distribucion_ataques is not None:
        print("[INFO] Generando visualizaciones...")
        generar_visualizaciones(distribucion_ataques, args.output)

    print("[INFO] Procesamiento batch finalizado correctamente.")
    spark.stop()


if __name__ == "__main__":
    main()
