# -*- coding: utf-8 -*-

import airflow

from datetime import timedelta, datetime

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator # noqa
from airflow.models import DAG, Variable

from meucandidato_dags.processors.tse_candidates_data import (
    DivulgacaoCandContasTSEProcessor
)

TSE_YEAR_DATA = Variable.get("TSE_YEAR_DATA", datetime.now().year)
MONGO_URI = Variable.get("MEUCANDIDATO_DATABASE_URI", "mongodb://localhost:27017")

api_tse_cand_contas_extraction = DivulgacaoCandContasTSEProcessor(
    db_uri=MONGO_URI,
    election_year=TSE_YEAR_DATA
)


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
    dag_id='import_candidate_extra_data',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60)
)

start_import = DummyOperator(
    task_id='start_import',
    dag=dag
)

import_candidate_extra_data = PythonOperator(
    task_id='import_extra_data_candidate',
    python_callable=api_tse_cand_contas_extraction.run,
    dag=dag
)

import_candidate_extra_data.set_upstream(start_import)

if __name__ == '__main__':
    dag.cli()
