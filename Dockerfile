FROM puckel/docker-airflow:1.10.2

ARG AIRFLOW_HOME=/usr/local/airflow

USER root

COPY requirements.txt /tmp
RUN apt-get update -yqq && apt-get install unzip
RUN pip install -r /tmp/requirements.txt
RUN pip install ipdb

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint