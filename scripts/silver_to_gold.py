import duckdb

# Conecta ao banco DuckDB
con = duckdb.connect(database='dados_duckdb.db', read_only=False)

# Lê os dados da tabela produtos
df = con.execute("SELECT * FROM produtos").fetchdf()

# Remove colunas desnecessárias
df2 = df.drop(columns=['Categoria', 'Fornecedor'])

# Remove duplicatas com base no ID
df2 = df2.drop_duplicates(subset='ID', keep='first')

# Cria a tabela dim_produtos se não existir
con.execute("""
    CREATE TABLE IF NOT EXISTS dim_produtos (
        id_produtos BIGINT,
        NM_produto TEXT,
        VL_produto FLOAT
    )
""")

# Limpa a tabela antes de inserir (opcional, para evitar duplicidade)
con.execute("DELETE FROM dim_produtos")

# Registra o dataframe limpo no DuckDB com nome temporário
con.register("df2_temp", df2)

# Insere os dados tratados na tabela dim_produtos
con.execute("""
    INSERT INTO dim_produtos
    SELECT ID AS id_produtos, Nome AS NM_produto, Preço AS VL_produto
    FROM df2_temp
""")

# Consulta e imprime a tabela final
df_dim = con.execute("SELECT * FROM dim_produtos").fetchdf()
print(df_dim.head(10))
con.close