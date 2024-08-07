import os
import shutil
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from tqdm import tqdm

# Carregar configurações dos arquivos config.json e configschema.json
with open('config.json') as config_file:
    config = json.load(config_file)

with open('configschema.json') as configschema_file:
    configschema = json.load(configschema_file)

table_name = configschema['table_name']
dtype_config = configschema['dtypes']

# Diretórios para salvar os arquivos Parquet
table_folder = os.path.join(config['local_folder'], table_name)
os.makedirs(table_folder, exist_ok=True)
os.makedirs(config['out_folder'], exist_ok=True)

# Função para converter e salvar DataFrame em Parquet
def save_to_parquet(df, file_path):
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, file_path, use_deprecated_int96_timestamps=True)

# Função para ajustar tipos de dados do DataFrame
def adjust_dtypes(df, dtype_config):
    for column, dtype in dtype_config.items():
        if dtype == 'string':
            df[column] = df[column].astype('string')
        elif dtype == 'datetime':
            df[column] = pd.to_datetime(df[column])
        elif dtype == 'Int64':
            df[column] = pd.to_numeric(df[column]).astype('Int64')
        else:
            raise ValueError(f'Dtype {dtype} não suportado para a coluna {column}')
    return df

# Função para processar e salvar uma tabela em chunks
def process_and_save_table(table_name, chunk_size, db_config):
    try:
        # Criação da engine de conexão
        engine = create_engine(f'mssql+pyodbc://{db_config["user"]}:{db_config["password"]}@{db_config["server"]}:{db_config["port"]}/{db_config["database"]}?driver=ODBC+Driver+17+for+SQL+Server')

        # Inicializar variáveis para o controle de chunks
        chunk_number = 1
        offset = 0
        
        # Obter o número total de registros
        total_records_query = f'SELECT COUNT(*) FROM {table_name}'
        total_records = pd.read_sql(total_records_query, engine).iloc[0, 0]

        # Inicializar a barra de progresso
        pbar = tqdm(total=total_records, desc=f'Processando {table_name}', unit='records')

        # Processar dados em chunks
        while True:
            query = f'SELECT * FROM {table_name} ORDER BY IDNotaFiscal OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY'
            df = pd.read_sql(query, engine)

            if df.empty:
                break

            # Ajustar tipos de dados
            df = adjust_dtypes(df, dtype_config)

            # Definir o caminho do arquivo Parquet para o chunk atual
            parquet_file = os.path.join(table_folder, f'{table_name}_part_{chunk_number:04d}.parquet')

            # Salvar parquet
            save_to_parquet(df, parquet_file)

            print(f'Tabela {table_name} salva como {parquet_file}')

            # Atualizar variáveis para o próximo chunk
            chunk_number += 1
            offset += chunk_size

            # Atualizar a barra de progresso
            pbar.update(len(df))

        pbar.close()
        # Fechar a conexão
        engine.dispose()

    except Exception as e:
        print(f'Erro ao processar a tabela {table_name}: {e}')

# Função para enviar arquivos para o Google Cloud Storage
def upload_to_gcs(bucket_name, source_folder, credentials_path):
    # Cria um cliente de armazenamento usando o arquivo de credenciais
    storage_client = storage.Client.from_service_account_json(credentials_path)
    bucket = storage_client.bucket(bucket_name)
    
    for root, _, files in os.walk(source_folder):
        for file in files:
            file_path = os.path.join(root, file)
            # Cria o caminho de destino com base na estrutura de diretórios
            relative_path = os.path.relpath(file_path, source_folder)
            destination_blob_name = os.path.join(config['local_folder'], relative_path).replace("\\", "/")
            blob = bucket.blob(destination_blob_name)
            
            # Upload do arquivo
            blob.upload_from_filename(file_path)
            print(f'Arquivo {file_path} enviado para {destination_blob_name} no bucket {bucket_name}.')

# Função para mover a pasta
def move_folder(src_folder, dest_folder):
    try:
        # Verifica se a pasta de destino já existe
        table_folder_dest = os.path.join(dest_folder, os.path.basename(src_folder))
        if os.path.exists(table_folder_dest):
            # Remove a pasta da tabela se ela existir
            shutil.rmtree(table_folder_dest)
            print(f'Pasta {table_folder_dest} removida.')

        # Move a pasta de origem para o destino
        shutil.move(src_folder, dest_folder)
        print(f'Pasta {src_folder} movida para {dest_folder}.')
    except Exception as e:
        print(f'Erro ao mover a pasta {src_folder}: {e}')

# Função para criar uma tabela externa no BigQuery
def create_external_table_in_bigquery(dataset_name, table_name, bucket_name, credentials_path):
    try:
        # Crie uma instância do cliente BigQuery com as credenciais
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        dataset_id = f'{client.project}.{dataset_name}'
        table_id = f'{dataset_id}.{table_name}'

        # Verifica se o dataset já existe
        try:
            client.get_dataset(dataset_id)
            print(f'Dataset {dataset_id} já existe.')
        except:
            # Cria o dataset se ele não existir
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = 'southamerica-east1'
            dataset = client.create_dataset(dataset)
            print(f'Dataset {dataset_id} criado com sucesso.')
        
        sql = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{table_id}`
        OPTIONS (
            format='PARQUET',
            uris=['gs://{bucket_name}/prod_banco/{table_name}/*']
        );
        """
        
        query_job = client.query(sql)
        query_job.result()
        print(f'Tabela externa {table_id} criada com sucesso.')
    
    except Exception as e:
        print(f'Erro ao criar a tabela externa no BigQuery: {e}')

# Execuções
process_and_save_table(table_name, config['chunk_size'], config['db_config'])

upload_to_gcs(config['bucket_name'], config['local_folder'], config['credentials_path'])

move_folder(table_folder, config['out_folder'])

create_external_table_in_bigquery(config['bigquery_dataset'], table_name, config['bucket_name'], config['credentials_path'])