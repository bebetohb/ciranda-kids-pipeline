# 🧵 Pipeline de Dados para Loja de Roupas Infantis

Este projeto simula uma pipeline de dados **end-to-end** para uma loja fictícia de roupas infantis, com foco em ingestão, transformação, modelagem e visualização de dados. O objetivo é demonstrar domínio técnico em engenharia de dados e análise de negócios, utilizando ferramentas modernas e boas práticas de versionamento.

## 🚀 Tecnologias Utilizadas

- 🐍 **Python** + **Pandas** — scripts de ingestão e transformação
- 🐘 **PostgreSQL** — banco de dados relacional para armazenamento
- 🛠️ **Apache Airflow** — orquestração de tarefas automatizadas
- 🐳 **Docker** — conteinerização do ambiente de execução
- 📊 **Power BI** — dashboard interativo com insights visuais
- 📁 **Excel** — fonte de dados simulada com tabelas de vendas e produtos

## 🧠 Objetivos do Projeto

- Criar uma pipeline de dados automatizada e modular
- Simular um ambiente de produção com Airflow e Docker
- Modelar dados em formato dimensional (fato e dimensões)
- Gerar insights visuais com KPIs e gráficos no Power BI
- Versionar todo o projeto no GitHub com estrutura clara

## 📁 Estrutura do Repositório

📦 ciranda-kids-pipeline/
├── dags/
│ └── etl_pipeline_dag.py → DAG de orquestração do projeto
│
├── etl/
│ ├── extract_load.py → Script de extração dos dados
│ ├── transform.py → Script de transformação dos dados
│ └── load_postgres.py → Script de carregamento no PostgreSQL
│
├── data/ (criar manualmente)
│ ├── raw/ → Contém o arquivo bruto .xlsx com os dados originais
│ ├── staging/ → Armazena as tabelas quebradas por arquivo .csv
│ └── processed/ → Dados transformados e modelados para o PostgreSQL
│
├── powerbi/ → Pasta para o arquivo do dashboard .pbix
│
├── .gitignore → Arquivo de configuração para ignorar dados sensíveis
├── Dockerfile → Configuração da imagem Docker
├── docker-compose.yaml → Orquestração dos containers (Airflow, PostgreSQL etc.)
├── requirements.txt → Lista de pacotes e dependências do projeto
└── README.md → Documentação principal do projeto

## 📊 Orquestrador Airflow

![alt text](image-1.png)
![alt text](image-2.png)

## 📊 Dashboard Power BI

O dashboard apresenta:

- Total de vendas por categoria
- Evolução mensal das vendas
- Top produtos vendidos
- Mapa de fornecedores por cidade
- KPIs como ticket médio e margem total

![alt text](image.png)

## 🧪 Como Executar Localmente

1. Clone o repositório:
   ```bash
   git clone https://github.com/bebetohb/ciranda-kids-pipeline

2. Suba os containers com Docker:
    docker-compose up --build

3. Acesse o Airflow em: http://localhost:8080/

4. Importe o .pbix no Power BI Desktop para visualizar os insights (favor solicitar)

5. Planilha .xlsx modelo do projeto (favor solicitar)

6. Arquivo .env na raiz do projeto com os dados de conexao do banco de dados e do Airflow:

DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=#nome_do_seu_banco
DB_USER=postgres
DB_PASSWORD=#sua_senha

AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://usuario_airflow:senha_do_airflow@host.docker.internal:5432/banco_do_airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false


**PS: É necessário a instalação do Power BI desktop, Docker e PostgreSQL**

📬 Contato

Humberto Bravo
📍 Olinda, PE
🔗 LinkedIn: www.linkedin.com/in/humbertobravohb
📧 bebetohb@live.com
