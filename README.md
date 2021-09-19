# igti-deng-cloud-proc-debate
Debate em grupo sobre consumo de dados abertos

## Pré-requisitos

- tar
- spark
- pyspark
- jupyter

## Configuração

Crie um arquivo `.env` baseado em `.env.dist` e modifique `BUCKET_NAME` com o nome do bucket alvo.
Modifique o arquivo `candidatos-brasil.py` trocando `bucket_name` para o mesmo nome configurado em `.env`.

## Criação da infraestrutura

Executar `up.sh`. O script criará o bucket do datalake e um cluster dataproc.

## Upload de arquivos crus

Executar `upload_to_gcp.sh`. O arquivo carregara os dados crus na pasta `raw` no datalake.

## Conversão de arquivos

Crie e submeta um job spark com o arquivo `candidatos-brasil.py`.

# Fonte de arquivos

Os arquivos foram baixados do site https://www.tse.jus.br/eleicoes/estatisticas/repositorio-de-dados-eleitorais-1
Incluimos um único CSV com os dados de todo o brasil.
