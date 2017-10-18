# Meu Candidato DAGs

Projeto voltado para a criação de workflow para extração e tratamento de dados de várias fontes de dados como:

 - Dados do TSE (Candidatos, Legendas, etc)
 - Outras fontes ainda não encontradas (sugestões são [bem vindas](https://github.com/meucandidato/airflow-dags/issues))

## Instalação

1. Faça o checkout do projeto:

```shell
$ git clone https://github.com/meucandidato/airflow-dags.git meucandidato-dags
```

2. Crie um ambiente virtual e faça a instalação

```shell
$ cd meucandidato-dags
$ python3 -m venv .venv
$ source .venv/bin/activate
```

```shell
$ python setup.py install
```

3. Instale localmente o airflow e [siga as instruções de configuração básicas](https://airflow.incubator.apache.org/start.html) para sua execução.

4. Execute o workflow via `airflow backfill`. Abaixo um exemplo de importação dos dados do TSE:

```shell
$ airflow backfill import_tse_data -s 2017-10-18
```
