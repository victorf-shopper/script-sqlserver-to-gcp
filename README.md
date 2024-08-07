# Guia de Configuração do Projeto

Este documento fornece um guia para configurar o projeto, incluindo a obtenção de credenciais do Google Cloud Platform (GCP), as dependências necessárias, e as configurações de arquivos.

## Instalação das Dependências

Antes de executar o projeto, certifique-se de instalar todas as dependências necessárias. Execute o seguinte comando para instalar os pacotes listados no arquivo `requirements.txt`:

```bash
pip install -r requirements.txt
```

## Definição da config.json

Ajuste as informações de acesso e configuração no arquivo `config.json`. Certifique-se de atualizar os seguintes campos com os valores apropriados para seu ambiente:

- **bucket_name**: Nome do bucket no Google Cloud Storage.
- **bucket_name**: Nome do dataset no Big Query.
- **credentials_path**: Caminho para o arquivo de credenciais do Google Cloud.
- **local_folder**: Caminho para o diretório local onde os arquivos serão salvos.
- **out_folder**: Caminho para o diretório de destino onde a pasta será movida.
- **chunk_size**: Tamanho dos chunks para processamento dos dados.
- **db_config**: Configurações de conexão com o banco de dados (servidor, porta, banco de dados, usuário e senha).

Exemplo:
```json
{
  "bucket_name": "meu-bucket",
  "bigquery_dataset": "testdataset",
  "credentials_path": "/caminho/para/credenciais.json",
  "local_folder": "/caminho/para/diretório/local",
  "out_folder": "/caminho/para/diretório/destino",
  "chunk_size": 10000,
  "db_config": {
    "server": "localhost",
    "port": 3306,
    "database": "meu_banco",
    "user": "usuario",
    "password": "senha"
  }
}
```

## Definição da configschema.json

Defina o nome da tabela e o esquema correspondente no arquivo `configschema.json`. Atualize os seguintes campos:

- **table_name**: Nome da tabela que será processada.
- **schema**: Estrutura da tabela, com os tipos de dados para cada coluna.

Exemplo de configuração para `configschema.json`:

```json
{
  "table_name": "NotaFiscal",
  "schema": {
    "IDNotaFiscal": "int64",
    "Origem": "string",
    "Referencia": "int64",
    "Situacao": "string",
    "DataSolicitacao": "datetime",
  }
}
```

## Obter Credenciais do Google Cloud Platform (GCP)

Para obter as credenciais do GCP, siga estas etapas:

### Passo 1: Acessar o Console do Google Cloud

1. Acesse [Google Cloud Console](https://console.cloud.google.com/).
2. Faça login na sua conta do Google.
3. Selecione ou crie um projeto.

### Passo 2: Criar uma Conta de Serviço (Caso não tenha uma conta senão passe para o próximo passo e só gere a chave JSON)

1. No menu lateral, vá para **IAM & Admin** > **Service accounts**.
2. Clique em **Criar Conta de Serviço**.
3. Preencha as informações da conta de serviço:
   - **Nome da Conta de Serviço**: Dê um nome descritivo.
   - **ID da Conta de Serviço**: Será gerado automaticamente.
   - **Descrição**: Opcional.
4. Defina permissões para a conta (por exemplo, **Storage Admin** ou **Storage Object Admin**).
5. Clique em **Continuar** e depois em **Concluído**.

### Passo 3: Gerar uma Chave para a Conta de Serviço

1. Encontre a conta de serviço criada e clique no menu de três pontos (⋮).
2. Selecione **Gerar chave**.
3. Escolha o formato **JSON** e clique em **Criar**.
4. O arquivo JSON será baixado automaticamente. Guarde-o em um local seguro.