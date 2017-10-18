# -*- coding: utf-8 -*-

import airflow

from datetime import timedelta, datetime

from pymongo import MongoClient

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator # noqa
from airflow.operators.bash_operator import BashOperator # noqa
from airflow.models import DAG, Variable

from meucandidato_dags.processors.tse_candidates_data import (
    TSECandidateDataProcessor
)

URI_DOWNLOAD_TSE_DATA = "http://agencia.tse.jus.br/estatistica/sead/odsele/"

TSE_YEAR_DATA = Variable.get("TSE_YEAR_DATA", datetime.now().year)

URI_TYPE_TSE_DATA = {
    'candidate': 'consulta_cand/consulta_cand_{0}'.format(TSE_YEAR_DATA),
    'property': 'bem_candidato/bem_candidato_{0}'.format(TSE_YEAR_DATA),
    'legend': 'consulta_legendas/consulta_legendas_{0}'.format(TSE_YEAR_DATA),
    'revocation': 'motivo_cassacao/motivo_cassacao_{0}'.format(TSE_YEAR_DATA)
}

DATABASE = MongoClient("mongodb://localhost:27017")['meucandidato']

candidate_extraction = TSECandidateDataProcessor(
    db_warehouse=DATABASE,
    data_path='/tmp/tse_cand_{0}'.format(TSE_YEAR_DATA)
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
    'retries': 1
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

download_tse_ownership_data_zip = DummyOperator(
    task_id='download_tse_ownership_data_zip',
    dag=dag
)

download_tse_legend_parties_data_zip = DummyOperator(
    task_id='download_tse_legend_parties_data_zip',
    dag=dag
)

uncompress_tse_candidate_data = BashOperator(
    task_id='uncompress_tse_candidate_data',
    bash_command='''
    unzip /tmp/consulta_cand_{{params.year}}.zip -d /tmp/tse_cand_{{params.year}}
    ''', # noqa
    params={'year': TSE_YEAR_DATA},
    dag=dag
)

uncompress_tse_ownership_data = DummyOperator(
    task_id='uncompress_tse_ownership_data',
    dag=dag
)

uncompress_tse_legend_parties_data = DummyOperator(
    task_id='uncompress_tse_legend_parties_data',
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

import_ownership_data = DummyOperator(
    task_id='import_ownership_data',
    dag=dag
)

import_legend_parties_data = DummyOperator(
    task_id='import_legend_parties_data',
    dag=dag
)

download_tse_candidate_data_zip.set_upstream(start_import)
download_tse_ownership_data_zip.set_upstream(start_import)
download_tse_legend_parties_data_zip.set_upstream(start_import)

uncompress_tse_candidate_data.set_upstream(download_tse_candidate_data_zip)
uncompress_tse_ownership_data.set_upstream(download_tse_ownership_data_zip)
uncompress_tse_legend_parties_data.set_upstream(download_tse_legend_parties_data_zip)
uncompress_tse_candidate_data.set_downstream(remove_unused_tse_files)
uncompress_tse_ownership_data.set_downstream(remove_unused_tse_files)
uncompress_tse_legend_parties_data.set_downstream(remove_unused_tse_files)

remove_unused_tse_files.set_downstream(import_candidate_data)
remove_unused_tse_files.set_downstream(import_ownership_data)
remove_unused_tse_files.set_downstream(import_legend_parties_data)

if __name__ == '__main__':
    dag.cli()
