import duckdb

con = duckdb.connect(database='dados_duckdb.db', read_only=False)

df = con.execute("""
    SELECT *, ROW_NUMBER() OVER (PARTITION BY NATBR ORDER BY date_ingestao DESC) As row
    FROM bronze_produtos
""").fetchdf()

df_final = df.drop(columns=['nome_arquivo','date_ingestao','row'])
df_final = df_final.rename(columns={"NATBR":"ID"})
df_final = df_final.rename(columns={"MAKTX":"Nome"})
df_final = df_final.rename(columns={"WERKS":"Categoria"})
df_final = df_final.rename(columns={"MAINS":"Fornecedor"})
df_final = df_final.rename(columns={"LABST":"Preço"})


df2 = df_final
df2 = df2.astype(
    {
        'ID': 'int32',
        'Nome': 'string',
        'Categoria': 'string',
        'Fornecedor': 'int32',
        'Preço': 'float32'
        
    }
)
#print(df2)

con.execute ("""
    CREATE TABLE IF NOT EXISTS produtos (
        ID BIGINT,
        Nome TEXT,
        Categoria TEXT,
        Fornecedor BIGINT,
        Preço FLOAT, 
     )
""")

con.execute("INSERT INTO produtos SELECT * FROM df2")
resultado = con.execute("SELECT * FROM produtos").fetchdf()
print(resultado.head(6))

con.close()