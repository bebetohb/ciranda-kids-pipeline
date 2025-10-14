import pandas as pd
import os
from pathlib import Path

# ------------------------------------------------------
# ⚙️ Caminhos das pastas (corrigido para ambiente Airflow)
# ------------------------------------------------------

BASE_PATH = Path("/opt/airflow/data")
STAGING_PATH = BASE_PATH / "staging"
PROCESSED_PATH = BASE_PATH / "processed"

def main():
    os.makedirs(PROCESSED_PATH, exist_ok=True)

    print("🚀 Iniciando transformação dos dados...")

    # 🔄 Limpar a pasta processed antes da carga
    for arquivo in os.listdir(PROCESSED_PATH):
        caminho_arquivo = os.path.join(PROCESSED_PATH, arquivo)
        if os.path.isfile(caminho_arquivo):
            os.remove(caminho_arquivo)
    print("🧹 Pasta 'processed' limpa com sucesso!\n")

    # ------------------------------------------------------
    # 📥 Leitura dos arquivos da camada staging
    # ------------------------------------------------------

    produto = pd.read_csv(STAGING_PATH / "produto.csv")
    estoque = pd.read_csv(STAGING_PATH / "estoque.csv")
    venda = pd.read_csv(STAGING_PATH / "venda.csv")
    fornecedor = pd.read_csv(STAGING_PATH / "fornecedor.csv")

    # ------------------------------------------------------
    # 🧹 Tratamentos e padronizações básicas
    # ------------------------------------------------------

    # ✅ Normaliza datas
    for col in ["data_venda", "data_entrada", "data_saida"]:
        if col in venda.columns or col in estoque.columns:
            try:
                if col in venda.columns:
                    venda[col] = pd.to_datetime(venda[col], errors="coerce")
                elif col in estoque.columns:
                    estoque[col] = pd.to_datetime(estoque[col], errors="coerce")
            except Exception:
                pass

    # ✅ Corrige valores faltantes em valor_total
    venda["valor_total"] = venda["valor_total"].fillna(
        venda["quantidade"] * venda["cod_produto"].map(
            produto.set_index("cod_produto")["preco_venda"]
        )
    )

    # ------------------------------------------------------
    # 💾 Exporta para camada processed
    # ------------------------------------------------------
    tabelas = {
        "dim_fornecedor": fornecedor,
        "dim_produto": produto,
        "fato_estoque": estoque,
        "fato_venda": venda
    }

    for nome, df in tabelas.items():
        df.to_csv(PROCESSED_PATH / f"{nome}.csv", index=False)

    print("✅ Transformação concluída e dados salvos em 'data/processed/'!")

# 🔁 Executa se chamado diretamente
if __name__ == "__main__":
    main()