# ETL Python + Pandas + Apache Spark

Pipeline de ETL desenvolvido em Python para demonstrar um fluxo completo de **Extração, Transformação e Carga (ETL)** utilizando **Pandas** e **Apache Spark (PySpark)**.

O projeto realiza a leitura de dados a partir de um arquivo Excel, utiliza Pandas para a ingestão inicial, converte os dados para um DataFrame do Spark, executa as transformações utilizando PySpark e, por fim, grava os dados processados nos formatos **Parquet, CSV e JSON**.

O projeto foi desenvolvido com foco em práticas e conceitos utilizados em **Engenharia de Dados**, incluindo processamento de dados, transformação distribuída, logging, organização modular do pipeline e armazenamento em formatos estruturados.

---

## Objetivo

Demonstrar na prática a construção de um pipeline ETL utilizando diferentes tecnologias do ecossistema Python e Big Data.

O pipeline foi estruturado para separar claramente as responsabilidades de cada etapa:

- **Pandas** → leitura e ingestão do arquivo Excel;
- **PySpark** → processamento e transformação dos dados;
- **Spark Write** → persistência dos dados processados;
- **Parquet** → armazenamento orientado a dados analíticos;
- **CSV** → formato de intercâmbio;
- **JSON** → formato estruturado para integração e consumo.

---

## Arquitetura do Pipeline

O fluxo completo do projeto é:

```text
                    clientes.xlsx
                         │
                         ▼
                    EXTRACT
                         │
                         ▼
                       Pandas
                         │
                         ▼
                  Spark DataFrame
                         │
                         ▼
                   TRANSFORM
                         │
              ┌──────────┼──────────┐
              │          │          │
              ▼          ▼          ▼
          Limpeza    Padronização   Datas
              │          │          │
              └──────────┼──────────┘
                         │
                         ▼
                  Deduplicação
                         │
                         ▼
                   Novas colunas
                         │
                         ▼
                      LOAD
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
           Parquet      CSV        JSON