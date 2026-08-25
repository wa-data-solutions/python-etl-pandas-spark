"""
transform.py

Responsável pela etapa de TRANSFORMAÇÃO do pipeline ETL.

As transformações são realizadas utilizando Apache Spark.

Fluxo:
Spark DataFrame
        ↓
Padronização das colunas
        ↓
Limpeza dos dados
        ↓
Tratamento de datas
        ↓
Remoção de duplicidades
        ↓
Criação de novas colunas
        ↓
Spark DataFrame transformado
"""

import logging
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Padroniza os nomes das colunas utilizando snake_case.

    Exemplos:
        Customer_Id -> customer_id
        First_Name -> first_name
        Phone 1 -> phone_1
        Subscription_Date -> subscription_date

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame de entrada.

    Returns
    -------
    DataFrame
        Spark DataFrame com nomes de colunas padronizados.
    """

    logging.info("Padronizando nomes das colunas...")

    standardized_columns = []

    for column in df.columns:
        column_name = column.strip().lower()

        # Substitui qualquer sequência de caracteres
        # que não seja letra ou número por "_".
        column_name = re.sub(r"[^a-z0-9]+", "_", column_name)

        # Remove "_" no início e no final.
        column_name = column_name.strip("_")

        standardized_columns.append(column_name)

    df = df.toDF(*standardized_columns)

    logging.info(
        "Colunas padronizadas: %s",
        ", ".join(df.columns)
    )

    return df


def clean_string_columns(df: DataFrame) -> DataFrame:
    """
    Remove espaços desnecessários das colunas de texto
    e transforma strings vazias em NULL.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    Returns
    -------
    DataFrame
        Spark DataFrame com campos de texto tratados.
    """

    logging.info("Realizando limpeza das colunas de texto...")

    string_columns = [
        field.name
        for field in df.schema.fields
        if field.dataType.simpleString() == "string"
    ]

    for column in string_columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(column))
            )
        )

    logging.info(
        "Limpeza realizada em %s colunas de texto.",
        len(string_columns)
    )

    return df


def transform_subscription_date(df: DataFrame) -> DataFrame:
    """
    Converte a coluna subscription_date para o tipo DATE.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    Returns
    -------
    DataFrame
        Spark DataFrame com subscription_date como DATE.
    """

    logging.info("Convertendo subscription_date para DATE...")

    if "subscription_date" in df.columns:
        df = df.withColumn(
            "subscription_date",
            F.to_date(F.col("subscription_date"))
        )

    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove registros duplicados.

    A coluna customer_id é utilizada como chave do cliente
    quando estiver disponível.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    Returns
    -------
    DataFrame
        Spark DataFrame sem duplicidades.
    """

    logging.info("Removendo registros duplicados...")

    if "customer_id" in df.columns:
        df = df.dropDuplicates(["customer_id"])
    else:
        df = df.dropDuplicates()

    logging.info("Remoção de duplicidades concluída.")

    return df


def create_full_name(df: DataFrame) -> DataFrame:
    """
    Cria a coluna full_name utilizando first_name e last_name.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    Returns
    -------
    DataFrame
        Spark DataFrame contendo full_name.
    """

    logging.info("Criando coluna full_name...")

    if "first_name" in df.columns and "last_name" in df.columns:
        df = df.withColumn(
            "full_name",
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name")
            )
        )

    return df


def create_processing_date(df: DataFrame) -> DataFrame:
    """
    Cria a coluna processing_date com a data do processamento.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    Returns
    -------
    DataFrame
        Spark DataFrame contendo processing_date.
    """

    logging.info("Criando coluna processing_date...")

    df = df.withColumn(
        "processing_date",
        F.current_date()
    )

    return df


def reorder_columns(df: DataFrame) -> DataFrame:
    """
    Organiza as colunas do DataFrame em uma ordem lógica.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame.

    Returns
    -------
    DataFrame
        Spark DataFrame com as colunas organizadas.
    """

    logging.info("Organizando colunas...")

    desired_order = [
        "index",
        "customer_id",
        "first_name",
        "last_name",
        "full_name",
        "company",
        "city",
        "country",
        "phone_1",
        "phone_2",
        "email",
        "subscription_date",
        "website",
        "processing_date",
    ]

    existing_columns = [
        column
        for column in desired_order
        if column in df.columns
    ]

    remaining_columns = [
        column
        for column in df.columns
        if column not in existing_columns
    ]

    final_order = existing_columns + remaining_columns

    return df.select(*final_order)


def transform_data(df: DataFrame) -> DataFrame:
    """
    Executa todas as transformações do pipeline.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame extraído.

    Returns
    -------
    DataFrame
        Spark DataFrame transformado.
    """

    logging.info("==========================================")
    logging.info("INICIANDO TRANSFORMAÇÃO")
    logging.info("==========================================")

    df = standardize_column_names(df)

    df = clean_string_columns(df)

    df = transform_subscription_date(df)

    df = remove_duplicates(df)

    df = create_full_name(df)

    df = create_processing_date(df)

    df = reorder_columns(df)

    logging.info("Transformação concluída com sucesso.")

    return df