import os
import pandas as pd
import psycopg2
import numpy as np
from   dotenv import load_dotenv

# 🧭 Carrega variáveis de ambiente do arquivo .env
load_dotenv(dotenv_path="/opt/airflow/.env")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# 📂 Caminho dos arquivos processados
PROCESSED_PATH = os.path.join("data", "processed")

# 🧱 Função para conectar ao banco PostgreSQL
def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn

# 🧩 Criação das tabelas (DDL)
TABLES_SQL = {
    "dim_fornecedor": """
        CREATE TABLE IF NOT EXISTS dim_fornecedor (
            id_fornecedor SERIAL PRIMARY KEY,
            cod_fornecedor VARCHAR(15),
            nome_fornecedor VARCHAR(100),
            telefone VARCHAR(20),
            email VARCHAR(100),
            cod_ibge_cidade INT REFERENCES cidade(cod_ibge_cidade),
            CONSTRAINT dim_fornecedor_cod_fornecedor_unique UNIQUE (cod_fornecedor)
        );
    """,
    "dim_produto": """
        CREATE TABLE IF NOT EXISTS dim_produto (
            id_produto SERIAL PRIMARY KEY,
            cod_produto VARCHAR(80),
            nome_produto VARCHAR(100),
            categoria VARCHAR(50),
            tamanho VARCHAR(10),
            cor VARCHAR(30),
            preco_custo NUMERIC(10,2),
            preco_venda NUMERIC(10,2),
            cod_fornecedor VARCHAR(15) REFERENCES dim_fornecedor(cod_fornecedor),
            CONSTRAINT dim_produto_cod_produto_unique UNIQUE (cod_produto)
        );
    """,
    "fato_estoque": """
        CREATE TABLE IF NOT EXISTS fato_estoque (
            id_estoque SERIAL PRIMARY KEY,
            cod_produto VARCHAR(80) REFERENCES dim_produto(cod_produto),
            quantidade INT,
            data_entrada DATE,
            data_saida DATE,
            local_estoque VARCHAR(50),
            CONSTRAINT fato_estoque_unique UNIQUE (cod_produto, data_entrada, local_estoque)
        );
    """,
    "fato_venda": """
        CREATE TABLE IF NOT EXISTS fato_venda (
            id_venda SERIAL PRIMARY KEY,
            cod_produto VARCHAR(80) REFERENCES dim_produto(cod_produto),
            data_venda DATE,
            quantidade INT,
            valor_total NUMERIC(10,2),
            CONSTRAINT fato_venda_unique UNIQUE (cod_produto, data_venda)
        );
    """
}

# 🚀 Criação das tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        for name, ddl in TABLES_SQL.items():
            print(f"🧱 Criando tabela: {name}")
            cur.execute(ddl)
        conn.commit()
    print("✅ Todas as tabelas foram criadas (ou já existiam).")

# 📤 Carregar dados CSV e inserir no banco
def load_data_to_postgres(conn):
    for file in os.listdir(PROCESSED_PATH):
        if file.endswith(".csv"):
            table_name = file.replace(".csv", "")
            csv_path = os.path.join(PROCESSED_PATH, file)
            print(f"📥 Inserindo dados na tabela: {table_name}")
            
            df = pd.read_csv(csv_path)

            # 🔹 Remove todas as colunas de ID que provavelmente são SERIAL no banco
            id_cols = [col for col in df.columns if col.startswith("id_")]
            df = df.drop(columns=id_cols, errors='ignore')  # errors='ignore' evita problema se não existir
            
            # 🧼 Padroniza valores vazios ou inválidos para None (que vira NULL no PostgreSQL)
            df = df.replace(["", " ", "NaN", "nan", "NULL", "None", "-", "--"], np.nan)
            df = df.where(pd.notnull(df), None)

            # Define regras de UPSERT por tabela
            UPSERT_RULES = {
                "dim_fornecedor": "ON CONFLICT (cod_fornecedor) DO UPDATE SET nome_fornecedor = EXCLUDED.nome_fornecedor, telefone = EXCLUDED.telefone, email = EXCLUDED.email, cod_ibge_cidade = EXCLUDED.cod_ibge_cidade",
                "dim_produto": "ON CONFLICT (cod_produto) DO UPDATE SET nome_produto = EXCLUDED.nome_produto, categoria = EXCLUDED.categoria, tamanho = EXCLUDED.tamanho, cor = EXCLUDED.cor, preco_custo = EXCLUDED.preco_custo, preco_venda = EXCLUDED.preco_venda, cod_fornecedor = EXCLUDED.cod_fornecedor",
                "fato_estoque": "ON CONFLICT ON CONSTRAINT fato_estoque_unique DO NOTHING",
                "fato_venda": "ON CONFLICT ON CONSTRAINT fato_venda_unique DO NOTHING"
            }

            # Cria colunas dinamicamente
            columns = ', '.join(df.columns)
            values_placeholders = ', '.join(['%s'] * len(df.columns))
            upsert_clause = UPSERT_RULES.get(table_name, "ON CONFLICT DO NOTHING")
            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({values_placeholders})
                {upsert_clause};
            """

            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    cur.execute(insert_query, tuple(row))
                conn.commit()

            print(f"✅ Dados inseridos em {table_name}")

# 🎯 Função principal para Airflow
def main():
    print("🚀 Iniciando carga no PostgreSQL...")
    conn = get_connection()
    create_tables(conn)
    load_data_to_postgres(conn)
    conn.close()
    print("🏁 Pipeline concluído com sucesso!")

# 🔁 Executa se chamado diretamente
if __name__ == "__main__":
    main()

