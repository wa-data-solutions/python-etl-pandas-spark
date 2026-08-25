"""
extract.py

Responsável pela etapa de EXTRAÇÃO do pipeline ETL.

Fluxo:
Excel (.xlsx)
        ↓
Pandas DataFrame
        ↓
Spark DataFrame
"""

from pathlib import Path
import logging

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def create_spark_session() -> SparkSession:
    """
    Cria e retorna uma SparkSession.
    """

    spark = (
        SparkSession.builder
        .appName("ETL Python Pandas Spark")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


def extract_excel(
    spark: SparkSession,
    file_path: str
) -> DataFrame:
    """
    Lê um arquivo Excel utilizando Pandas e converte
    para um Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        Sessão Spark.

    file_path : str
        Caminho do arquivo Excel.

    Returns
    -------
    DataFrame
        Spark DataFrame.
    """

    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: {file_path}"
        )

    logging.info("Lendo arquivo Excel...")

    pandas_df = pd.read_excel(path)

    logging.info(
        "Arquivo carregado com sucesso (%s linhas).",
        len(pandas_df)
    )

    logging.info("Convertendo Pandas DataFrame para Spark DataFrame...")

    spark_df = spark.createDataFrame(pandas_df)

    logging.info(
        "Conversão concluída (%s registros).",
        spark_df.count()
    )

    return spark_df


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

    spark = create_spark_session()

    df = extract_excel(
        spark,
        "data/input/clientes.xlsx"
    )

    df.printSchema()

    df.show(10, truncate=False)

    spark.stop()