from google.cloud import bigquery
from google.oauth2 import service_account

# Defina o caminho para o arquivo de credenciais do Google Cloud
credentials_path = 'C:/Users/victor.fernandes/Downloads/credentials.json'

# Crie uma instância do cliente BigQuery com as credenciais
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Comando SQL para criar a tabela externa
sql_command = """
CREATE OR REPLACE EXTERNAL TABLE `shopper-datalakehouse-qa.tests.Teste`
OPTIONS (
  format='PARQUET',
  uris=['gs://dataflow-staging-southamerica-east1-71212774978/prod_banco/NotaFiscal/*']
);
"""

def execute_sql_command(sql_command):
    try:
        # Executa o comando SQL
        query_job = client.query(sql_command)
        
        # Espera a conclusão da consulta
        query_job.result()
        
        print("Tabela externa criada com sucesso!")
    except Exception as e:
        print(f'Erro ao criar a tabela externa: {e}')

# Executar o comando SQL
execute_sql_command(sql_command)
