"""
load.py

Responsável pela etapa de LOAD do pipeline ETL.

O Spark DataFrame transformado é persistido em diferentes
formatos de arquivo:

    Spark DataFrame
          │
          ├──► Parquet
          │
          ├──► CSV
          │
          └──► JSON

Diretório de saída:
    data/output/
"""

import logging
from pathlib import Path

from pyspark.sql import DataFrame


def create_output_directory(output_directory: str) -> Path:
    """
    Cria o diretório de saída caso ele ainda não exista.

    Parameters
    ----------
    output_directory : str
        Caminho do diretório de saída.

    Returns
    -------
    Path
        Caminho do diretório de saída.
    """

    output_path = Path(output_directory)

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    logging.info(
        "Diretório de saída: %s",
        output_path
    )

    return output_path


def load_parquet(
    df: DataFrame,
    output_directory: str
) -> None:
    """
    Salva o DataFrame no formato Parquet.

    O Parquet será utilizado como principal formato
    de armazenamento analítico do pipeline.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame transformado.

    output_directory : str
        Diretório onde o arquivo será salvo.
    """

    output_path = Path(output_directory) / "clientes_parquet"

    logging.info(
        "Salvando dados no formato Parquet..."
    )

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    logging.info(
        "Parquet salvo com sucesso: %s",
        output_path
    )


def load_csv(
    df: DataFrame,
    output_directory: str
) -> None:
    """
    Salva o DataFrame no formato CSV.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame transformado.

    output_directory : str
        Diretório onde os dados serão salvos.
    """

    output_path = Path(output_directory) / "clientes_csv"

    logging.info(
        "Salvando dados no formato CSV..."
    )

    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(str(output_path))
    )

    logging.info(
        "CSV salvo com sucesso: %s",
        output_path
    )


def load_json(
    df: DataFrame,
    output_directory: str
) -> None:
    """
    Salva o DataFrame no formato JSON.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame transformado.

    output_directory : str
        Diretório onde os dados serão salvos.
    """

    output_path = Path(output_directory) / "clientes_json"

    logging.info(
        "Salvando dados no formato JSON..."
    )

    (
        df.write
        .mode("overwrite")
        .json(str(output_path))
    )

    logging.info(
        "JSON salvo com sucesso: %s",
        output_path
    )


def load_data(
    df: DataFrame,
    output_directory: str
) -> None:
    """
    Executa a etapa completa de LOAD.

    Os dados são persistidos nos formatos:

        - Parquet
        - CSV
        - JSON

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame transformado.

    output_directory : str
        Diretório de destino.
    """

    logging.info("==========================================")
    logging.info("INICIANDO LOAD")
    logging.info("==========================================")

    create_output_directory(output_directory)

    load_parquet(
        df,
        output_directory
    )

    load_csv(
        df,
        output_directory
    )

    load_json(
        df,
        output_directory
    )

    logging.info("==========================================")
    logging.info("LOAD CONCLUÍDO COM SUCESSO")
    logging.info("==========================================")