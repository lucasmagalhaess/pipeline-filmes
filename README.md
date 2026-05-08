# 🎬 Pipeline de Dados de Filmes — End to End

Pipeline de dados completo construído para aprendizado prático de Engenharia de Dados, consumindo dados da API do TMDB e processando até o BigQuery no GCP.

## 🏗️ Arquitetura
## 🛠️ Tecnologias

| Tecnologia | Função |
|---|---|
| Python | Extração e scripts de ingestão |
| Apache Airflow | Orquestração do pipeline |
| Apache Spark (PySpark) | Transformação e processamento |
| MinIO | Data Lake local (S3 compatível) |
| PostgreSQL | Camada de staging |
| Google Cloud Storage | Data Lake na nuvem |
| BigQuery | Data Warehouse analytics |
| Terraform | Infraestrutura como código |
| Docker + Compose | Containerização do ambiente |
| GitHub Codespaces | Ambiente de desenvolvimento cloud |

## 📋 Fases do Pipeline

1. **Extração** — Coleta dados de filmes populares da API TMDB
2. **Armazenamento Raw** — Salva JSON bruto no MinIO
3. **Transformação** — Processa com Spark (filtra, ordena, limpa)
4. **Staging** — Carrega no PostgreSQL para validação SQL
5. **Data Lake** — Envia para GCS na nuvem
6. **Data Warehouse** — Carrega no BigQuery para analytics
7. **Orquestração** — Airflow agenda e monitora tudo diariamente

## 🚀 Como rodar

### Pré-requisitos
- GitHub Codespaces ou Docker instalado
- Conta GCP com projeto criado
- API Key do TMDB

### 1. Subir o ambiente
```bash
cd docker
docker compose up airflow-init
docker compose up -d
```

### 2. Configurar variáveis
```bash
cp .env.example .env
# Preencha as variáveis no .env
```

### 3. Criar infraestrutura GCP
```bash
cd terraform
terraform init
terraform apply
```

### 4. Rodar o pipeline
Acesse o Airflow em `http://localhost:8080` e ative a DAG `pipeline_filmes`.

## 📊 Resultado

Pipeline processa **100 filmes** da API TMDB diariamente, filtra por qualidade (>100 votos) e disponibiliza **66 filmes** processados no BigQuery para análise.

## 👨‍💻 Autor

**Lucas Magalhães** — Analista de Dados em transição para Engenharia de Dados

[![GitHub](https://img.shields.io/badge/GitHub-lucasmagalhaess-black)](https://github.com/lucasmagalhaess)
