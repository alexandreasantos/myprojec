import duckdb
import pandas as pd
import os
from datetime import datetime

# Conecta (ou cria) o banco DuckDB
con = duckdb.connect(database='dados_duckdb.db', read_only=False)
# Nome do arquivo
arquivo = 'z0019_2.csv'
date_ingestao= datetime.now()
# Lê o CSV com separador ponto e vírgula
df = pd.read_csv(f'./landing/{arquivo}', sep=';')
df['nome_arquivo'] = arquivo
df['data_ingestao'] = date_ingestao
# Exibe as primeiras linhas
#print(df.head())
con.execute ("""
    CREATE TABLE IF NOT EXISTS bronze_produtos (
        NATBR VARCHAR,
        MAKTX VARCHAR,
        WERKS VARCHAR,
        MAINS VARCHAR,
        LABST VARCHAR, 
        nome_arquivo VARCHAR,
        date_ingestao TIMESTAMP   
    )
""")
con.execute("INSERT INTO bronze_produtos SELECT * FROM df")
resultado = con.execute("SELECT * FROM bronze_produtos").fetchdf()
print(resultado.head(6))


con.close()
