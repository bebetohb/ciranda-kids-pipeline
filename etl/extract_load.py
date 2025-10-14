import pandas as pd
import os

# Caminhos dos diretórios
RAW_PATH = "/opt/airflow/data/raw/ciranda_kids.xlsx"
STAGING_DIR = "/opt/airflow/data/staging/"

# Estrutura esperada de cada aba (para validação)
TABELAS_ESPERADAS = {
    "produto": [
        "cod_fornecedor", "cod_produto", "nome_produto", "categoria", "tamanho", "cor",
        "preco_custo", "preco_venda"
    ],
    "estoque": [
         "cod_produto", "quantidade", "data_entrada",
        "data_saida", "local_estoque"
    ],
    "venda": [
        "cod_produto", "data_venda", "quantidade", "valor_total"
    ],
    "fornecedor": [
        "cod_fornecedor", "nome_fornecedor", "telefone", "email", "cod_ibge_cidade"
    ]
}

def main():
    # Criar diretório staging se não existir
    os.makedirs(STAGING_DIR, exist_ok=True)

    # 🔄 Limpar a pasta staging antes da carga
    for arquivo in os.listdir(STAGING_DIR):
        caminho_arquivo = os.path.join(STAGING_DIR, arquivo)
        if os.path.isfile(caminho_arquivo):
            os.remove(caminho_arquivo)
    print("🧹 Pasta 'staging' limpa com sucesso!\n")

    # ------------------------------------------------------------
    # 📥 EXTRAÇÃO DOS DADOS
    # ------------------------------------------------------------
    print("📥 Lendo planilha de dados brutos...")
    dados = pd.read_excel(RAW_PATH, sheet_name=None)
    print(f"✅ {len(dados)} abas carregadas com sucesso!\n")

    # ------------------------------------------------------------
    # 🧩 PADRONIZAÇÃO + VALIDAÇÃO + EXPORTAÇÃO
    # ------------------------------------------------------------
    for aba, df in dados.items():
        print(f"🔎 Processando aba: {aba}")

        if aba not in TABELAS_ESPERADAS:
            print(f"⚠️ Aba '{aba}' não esperada — ignorada.")
            continue

        # 🧼 Padronização de colunas
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        # ✅ Validação das colunas
        esperadas = [c.lower() for c in TABELAS_ESPERADAS[aba]]
        faltando = [c for c in esperadas if c not in df.columns]
        extras = [c for c in df.columns if c not in esperadas]

        if faltando:
            print(f"❌ Colunas faltando: {', '.join(faltando)} — exportação cancelada.")
            continue

        if extras:
            print(f"⚠️ Colunas extras detectadas: {', '.join(extras)} — serão mantidas.")

        # 🗓️ Padroniza colunas de data
        for coluna in df.columns:
            if "data" in coluna:
                try:
                    df[coluna] = pd.to_datetime(df[coluna], errors='coerce').dt.strftime('%Y-%m-%d')
                except Exception as e:
                    print(f"⚠️ Erro ao formatar coluna '{coluna}': {e}")

        # 💾 Exporta para CSV
        output_path = f"{STAGING_DIR}/{aba.lower()}.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Exportado para: {output_path}\n")

    print("✨ Extração, padronização e exportação concluídas com sucesso!")

# 🔁 Executa se chamado diretamente
if __name__ == "__main__":
    main()

