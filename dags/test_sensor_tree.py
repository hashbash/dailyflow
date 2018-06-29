from datetime import datetime, timedelta
from airflow.exceptions import AirflowException

from airflow import DAG
from airflow.settings import Session
from dailyflow import SQLSensorOperator
from dailyflow_models import SQLSensor


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('test_sesnor_tree',
          default_args=default_args,
          schedule_interval='@daily',
          )

session = Session()
sensors = session.query(SQLSensor)\
    .filter(SQLSensor.enabled == True)\
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
        dag=dag,
    )
    graphs.append(graph)

def get_parent_ids(sql_sensor_id):
    parent_labels = session.query(SQLSensor.parent_labels)\
                        .filter(SQLSensor.id == sql_sensor_id)\
                        .first()[0]
    parent_ids = []
    for label in parent_labels or []:
        result = session.query(SQLSensor.id)\
                .filter(SQLSensor.label == label)\
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

            
    
