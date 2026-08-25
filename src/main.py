"""
main.py

Ponto de entrada do pipeline ETL.

Fluxo:

    EXTRACT
        Excel
          ↓
        Pandas
          ↓
    Spark DataFrame

    TRANSFORM
        Spark DataFrame
          ↓
        Limpeza
        Padronização
        Tratamento de datas
        Deduplicação
        Criação de colunas

    LOAD
        Spark DataFrame
          ↓
        Parquet
        CSV
        JSON

Execução:

    python src/main.py
"""

import logging
import sys
from pathlib import Path

from extract import create_spark_session, extract_excel
from transform import transform_data
from load import load_data


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = PROJECT_ROOT / "data" / "input" / "clientes.xlsx"

OUTPUT_DIRECTORY = PROJECT_ROOT / "data" / "output"


# ============================================================
# LOGGING
# ============================================================

def configure_logging() -> None:
    """
    Configura o sistema de logging do pipeline.

    Os logs são enviados simultaneamente para:

        - Console
        - logs/etl.log
    """

    log_directory = PROJECT_ROOT / "logs"

    log_directory.mkdir(
        parents=True,
        exist_ok=True
    )

    log_file = log_directory / "etl.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                log_file,
                encoding="utf-8"
            ),
            logging.StreamHandler()
        ]
    )


# ============================================================
# PIPELINE
# ============================================================

def run_pipeline() -> None:
    """
    Executa o pipeline completo de ETL.

    Fluxo:

        Extract → Transform → Load
    """

    spark = None

    try:

        logging.info("==========================================")
        logging.info("INICIANDO PIPELINE ETL")
        logging.info("==========================================")

        # ====================================================
        # VALIDAÇÃO DO ARQUIVO DE ENTRADA
        # ====================================================

        if not INPUT_FILE.exists():

            raise FileNotFoundError(
                f"Arquivo de entrada não encontrado: {INPUT_FILE}"
            )

        logging.info(
            "Arquivo de entrada encontrado: %s",
            INPUT_FILE
        )

        # ====================================================
        # SPARK SESSION
        # ====================================================

        logging.info("Criando SparkSession...")

        spark = create_spark_session()

        logging.info(
            "SparkSession criada com sucesso."
        )

        # ====================================================
        # EXTRACT
        # ====================================================

        logging.info("==========================================")
        logging.info("ETAPA 1 - EXTRACT")
        logging.info("==========================================")

        df = extract_excel(
            spark=spark,
            file_path=str(INPUT_FILE)
        )

        logging.info(
            "ETAPA EXTRACT concluída com sucesso."
        )

        # ====================================================
        # TRANSFORM
        # ====================================================

        logging.info("==========================================")
        logging.info("ETAPA 2 - TRANSFORM")
        logging.info("==========================================")

        transformed_df = transform_data(df)

        logging.info(
            "ETAPA TRANSFORM concluída com sucesso."
        )

        # ====================================================
        # LOAD
        # ====================================================

        logging.info("==========================================")
        logging.info("ETAPA 3 - LOAD")
        logging.info("==========================================")

        load_data(
            df=transformed_df,
            output_directory=str(OUTPUT_DIRECTORY)
        )

        logging.info(
            "ETAPA LOAD concluída com sucesso."
        )

        # ====================================================
        # RESULTADO FINAL
        # ====================================================

        logging.info("==========================================")
        logging.info("VALIDANDO RESULTADO FINAL")
        logging.info("==========================================")

        total_records = transformed_df.count()

        total_columns = len(
            transformed_df.columns
        )

        logging.info(
            "Total de registros processados: %s",
            total_records
        )

        logging.info(
            "Total de colunas: %s",
            total_columns
        )

        logging.info(
            "Colunas finais: %s",
            ", ".join(transformed_df.columns)
        )

        print()
        print("==========================================")
        print("PIPELINE ETL EXECUTADO COM SUCESSO")
        print("==========================================")
        print()
        print(f"Registros processados: {total_records}")
        print(f"Colunas finais: {total_columns}")
        print()
        print(f"Arquivos gerados em:")
        print(f"{OUTPUT_DIRECTORY}")
        print()
        print("Formatos:")
        print("  - Parquet")
        print("  - CSV")
        print("  - JSON")
        print()

    except Exception as error:

        logging.exception(
            "Erro durante a execução do pipeline: %s",
            error
        )

        print()
        print("==========================================")
        print("ERRO NA EXECUÇÃO DO PIPELINE")
        print("==========================================")
        print()
        print(f"Erro: {error}")
        print()

        sys.exit(1)

    finally:

        # ====================================================
        # ENCERRAMENTO DO SPARK
        # ====================================================

        if spark is not None:

            logging.info(
                "Encerrando SparkSession..."
            )

            spark.stop()

            logging.info(
                "SparkSession encerrada."
            )

        logging.info(
            "Pipeline finalizado."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    configure_logging()

    run_pipeline()