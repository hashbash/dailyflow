from datetime import datetime, timedelta
import datetime as dt
from airflow.exceptions import AirflowException

import logging
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.settings import Session
from dailyflow import SQLSensorOperator
from dailyflow_models import SQLSensor
from dailyflow_models import SQLDag
from dailyflow_models import SQLDagTask

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}


def make_subdag(subdag_id, parent_dag_id, start_date, schedule_interval):
    subdag = DAG('%s.%s' % (parent_dag_id, subdag_id),
                 start_date=start_date,
                 schedule_interval=schedule_interval,
                 )

    session = Session()
    sensors = session.query(SQLSensor) \
        .filter(SQLSensor.enabled == True) \
        .all()

    graphs = []
    for sensor in sensors:
        graph = SQLSensorOperator(
            task_id=sensor.label,
            sql_sensor_id=sensor.id,
            conn_id=sensor.connection.conn_id,
            pool=sensor.pool.pool if sensor.pool else None,
            poke_interval=sensor.poke_interval,
            timeout=sensor.timeout,
            ttl=sensor.ttl,
            main_argument=sensor.main_argument,
            sql=sensor.sql,
            dag=subdag,
        )
        graphs.append(graph)

    def get_parent_ids(sql_sensor_id):
        parent_labels = session.query(SQLSensor.parent_labels) \
            .filter(SQLSensor.id == sql_sensor_id) \
            .first()[0]
        parent_ids = []
        for label in parent_labels or []:
            result = session.query(SQLSensor.id) \
                .filter(SQLSensor.label == label) \
                .first()[0]
            if not result:
                raise AirflowException("Parent sensor with label '%s' doesn't exists." % label)
            parent_ids.append(result)
        return parent_ids

    for graph in graphs:
        parent_ids = get_parent_ids(graph.sql_sensor_id)
        for parent_id in parent_ids:
            for parent_graph in graphs:
                if parent_id == parent_graph.sql_sensor_id:
                    graph.set_upstream(parent_graph)

    return subdag


def execute_statement(**kw):
    hook = BaseHook.get_connection(kw['conn_id']).get_hook()
    logging.info('Executing: %s', kw['sql'])
    hook.run(kw['sql'])


session = Session()
dags = session.query(SQLDag) \
    .filter(SQLSensor.enabled == True) \
    .all()


for _dag in dags:
    default_args['start_date'] = datetime.combine(
        _dag.start_date,
        dt.time(),
    )
    dag = DAG(_dag.id,
              default_args=default_args,
              schedule_interval=_dag.schedule_interval,
              )
    subdag_name = 'subdag_with_sensors'
    sub_dag = SubDagOperator(
        subdag=make_subdag(subdag_id=subdag_name,
                           parent_dag_id=_dag.id,
                           start_date=default_args['start_date'],
                           schedule_interval=_dag.schedule_interval
                           ),
        task_id=subdag_name,
        dag=dag
    )

    tasks = session.query(SQLDagTask).all()
    # todo: implement enabled/disabled state of SQLDagTask

    graphs = []
    for task in tasks:
        graph = PythonOperator(
            task_id=task.label,
            python_callable=execute_statement,
            op_kwargs={'conn_id': task.connection.conn_id, 'sql': task.sql},
            pool=task.pool.pool if task.pool else None,
            dag=dag
        )
        graphs.append(graph)
        if not task.upstream_task_id:
            sub_dag >> graph

    for graph in graphs:
        for task in tasks:
            if task.label == graph.task_id and task.upstream_task_id:
                for upstream in graphs:
                    if upstream.task_id == task.parent.label:
                        upstream >> graph
