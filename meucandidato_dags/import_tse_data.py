# -*- coding: utf-8 -*-

import airflow

from datetime import timedelta, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator # noqa
from airflow.operators.bash_operator import BashOperator # noqa
from airflow.models import DAG, Variable

from meucandidato_dags.processors.tse_candidates_data import (
    TSECandidateDataProcessor, DivulgacaoCandContasTSEProcessor
)

URI_DOWNLOAD_TSE_DATA = "http://agencia.tse.jus.br/estatistica/sead/odsele/"

TSE_YEAR_DATA = Variable.get("TSE_YEAR_DATA", datetime.now().year)
MONGO_URI = Variable.get("MEUCANDIDATO_DATABASE_URI", "mongodb://localhost:27017")

URI_TYPE_TSE_DATA = {
    'candidate': 'consulta_cand/consulta_cand_{0}'.format(TSE_YEAR_DATA),
    'property': 'bem_candidato/bem_candidato_{0}'.format(TSE_YEAR_DATA),
    'legend': 'consulta_legendas/consulta_legendas_{0}'.format(TSE_YEAR_DATA),
    'revocation': 'motivo_cassacao/motivo_cassacao_{0}'.format(TSE_YEAR_DATA)
}

candidate_extraction = TSECandidateDataProcessor(
    db_uri=MONGO_URI,
    data_path='/tmp/tse_cand_{0}'.format(TSE_YEAR_DATA),
    filename='consulta_cand_{0}_BRASIL.csv'.format(TSE_YEAR_DATA)
)
api_tse_cand_contas_extraction = DivulgacaoCandContasTSEProcessor(
    db_uri=MONGO_URI,
    election_year=TSE_YEAR_DATA
)


def url_download_data(type):
    return URI_DOWNLOAD_TSE_DATA + URI_TYPE_TSE_DATA.get(type) + '.zip'


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(730),
    'email': ['me@gilsondev.in'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10)
}


dag = DAG(
    dag_id='import_tse_data',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)

start_import = DummyOperator(
    task_id='start_import',
    dag=dag
)

download_tse_candidate_data_zip = BashOperator(
    task_id='download_tse_candidate_data_zip',
    bash_command='''
    curl -o /tmp/consulta_cand_{{params.year}}.zip {{ params.download_url }}
    ''',
    params={
        'year': TSE_YEAR_DATA,
        'download_url': url_download_data('candidate')
    },
    dag=dag
)

uncompress_tse_candidate_data = BashOperator(
    task_id='uncompress_tse_candidate_data',
    bash_command='''
    unzip -o /tmp/consulta_cand_{{params.year}}.zip -d /tmp/tse_cand_{{params.year}}
    ''', # noqa
    params={'year': TSE_YEAR_DATA},
    dag=dag
)

remove_unused_tse_files = BashOperator(
    task_id='remove_unused_tse_files',
    bash_command='rm -rf /tmp/tse_cand_{{params.year}}/*.pdf',
    params={'year': TSE_YEAR_DATA},
    dag=dag
)

import_candidate_data = PythonOperator(
    task_id='import_candidate_data',
    python_callable=candidate_extraction.run,
    dag=dag
)

import_candidate_extra_data = PythonOperator(
    task_id='import_extra_data_candidate',
    python_callable=api_tse_cand_contas_extraction.run,
    dag=dag
)

start_import >> download_tse_candidate_data_zip >> uncompress_tse_candidate_data >> remove_unused_tse_files >> import_candidate_data >> import_candidate_extra_data

# download_tse_candidate_data_zip.set_upstream(start_import)
# uncompress_tse_candidate_data.set_upstream(download_tse_candidate_data_zip)
# remove_unused_tse_files.set_upstream(uncompress_tse_candidate_data)
# 
# import_candidate_data.set_upstream(remove_unused_tse_files)
# import_candidate_extra_data.set_upstream(import_candidate_data)

if __name__ == '__main__':
    dag.cli()
