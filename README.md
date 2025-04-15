# Seven E-commerce - Projeto de Data Warehouse

Este documento fornece instruções detalhadas sobre como configurar e executar a solução de data warehouse do Seven E-commerce.

## Pré-requisitos

- Python 3.7 ou superior
- PostgreSQL instalado e em execução
- Acesso ao repositório do projeto


### 1. Clone o Repositório

```bash
git clone [URL_DO_REPOSITÓRIO]
cd prova
```
### 1.1 Configure as váriveis de ambiente no arquivo .env

account_name="{account_name_do_datalake}"
account_key="{valor_da_chave_de_acesso_do_datalake}"
file_system_name="{file_system_name_do_datalake}"
local_file_path="data/desafio/raw"
dest_file_path="desafio/raw"

### 2. Instale as Dependências

```bash
pip install -r requirements.txt
pip install psycopg2-binary sqlalchemy dbt-postgres
```

## Configuração do Banco de Dados

### 1. Configure o PostgreSQL

Certifique-se de que o PostgreSQL esteja em execução e crie um banco de dados para o projeto:

```bash
psql -U postgres
```

No console do PostgreSQL:

```sql
CREATE DATABASE seven_ecommerce_dw;
\q
```

### 2. Ajuste os Parâmetros de Conexão

Abra o arquivo `load_data_to_postgres.py` e ajuste os parâmetros de conexão conforme necessário:

```python
DB_PARAMS = {
    'dbname': 'seven_ecommerce_dw',  # Altere para o nome do seu banco de dados
    'user': 'postgres',              # Altere para o seu usuário
    'password': 'postgres',          # Altere para sua senha
    'host': 'localhost',
    'port': '5432'
}
```

## Carregamento de Dados

### 1. Prepare os Dados Brutos

Certifique-se de que os arquivos de dados brutos estejam disponíveis no diretório:

```
data/desafio/raw/user_raw.csv
data/desafio/raw/produtos_raw.csv
data/desafio/raw/pedidos_raw.csv
```

### 2. Execute o Processamento de Dados

```bash
python local_process.py
```

Este script processará os dados brutos, aplicando transformações e validações.

### 3. Carregue os Dados no PostgreSQL

```bash
python load_data_to_postgres.py
```

Este script criará as tabelas necessárias e carregará os dados processados no banco de dados PostgreSQL.

## Configuração do DBT

### 1. Configure o Perfil do DBT

Crie ou atualize o arquivo `~/.dbt/profiles.yml` com a seguinte configuração:

```yaml
seven_ecommerce_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres          # Altere para o seu usuário
      password: postgres      # Altere para sua senha
      port: 5432
      dbname: seven_ecommerce_dw
      schema: dbt_dev
      threads: 4
```

### 2. Atualize o Arquivo de Projeto DBT

Verifique se o arquivo `dbt_project.yml` está configurado corretamente. O perfil deve corresponder ao configurado no passo anterior:

```yaml
name: 'seven_dw'
version: '1.0.0'
config-version: 2

profile: 'seven_ecommerce_dw'

# Resto da configuração...
```

## Executando o DBT

### 1. Execute os Modelos DBT

```bash
dbt run
```

Para executar modelos específicos:

```bash
dbt run --models staging.stg_users
dbt run --models marts.core
```

### 2. Teste os Modelos

```bash
dbt test
```

### 3. Gere Documentação

```bash
dbt docs generate
dbt docs serve
```

## Executando o Pipeline Completo com Airflow

Se você tiver o Airflow configurado:

```bash
# Inicie o servidor Airflow
airflow webserver -p 8080

# Em outro terminal, inicie o scheduler
airflow scheduler
```

Acesse o painel do Airflow em `http://localhost:8080` e ative a DAG `seven_etl_pipeline`.

## Estrutura do Projeto

```
/
├── airflow/                # Configuração do Airflow
├── dags/                   # Definições de DAGs do Airflow
├── data/                   # Diretório de dados
│   └── desafio/            # Dados do desafio
│       ├── raw/            # Arquivos de dados brutos
│       ├── processed/      # Dados processados (criados pelo pipeline)
│       └── reports/        # Relatórios gerados (criados pelo pipeline)
├── dbt_project.yml         # Configuração do projeto DBT
├── models/                 # Modelos DBT
│   ├── marts/              # Modelos de nível de negócios
│   └── staging/            # Modelos de transformação inicial de dados
├── steps/                  # Etapas de processamento
└── util/                   # Scripts utilitários
```

## Solução de Problemas

### Problemas de Conexão
- Verifique a configuração do arquivo `profiles.yml`
- Verifique as credenciais do banco de dados
- Execute `dbt debug` para testar a conexão

### Erros nos Modelos
- Verifique a sintaxe SQL
- Verifique os nomes das tabelas e colunas de origem
- Procure por referências ausentes

### Problemas de Desempenho
- Considere usar modelos incrementais para tabelas grandes
- Otimize as consultas SQL
- Ajuste a estratégia de materialização (view vs table)