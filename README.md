# ETL Python + Pandas + Spark

Pipeline de ETL desenvolvido em Python para automatizar a extração,
transformação e carregamento de dados utilizando Pandas e Apache Spark.

## Tecnologias

- Python
- Pandas
- PySpark
- OpenPyXL
- JSON
- Parquet

## Estrutura

etl-python-pandas-spark/

├── data/
│   ├── input/
│   └── output/
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
│
├── requirements.txt
├── README.md
└── .gitignore

## Fluxo

Excel
   ↓
Python
   ↓
Pandas
   ↓
Spark
   ↓
Parquet
   ↓
JSON

## Como executar

pip install -r requirements.txt

python src/main.py
