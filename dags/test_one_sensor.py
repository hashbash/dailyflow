from datetime import datetime, timedelta


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

dag = DAG('test_one_sensor',
          default_args=default_args,
          schedule_interval='@daily',
          )

session = Session()
sensor = session.query(SQLSensor)\
    .filter(SQLSensor.id == 1)\
    .first()

graph = SQLSensorOperator(
    task_id='test_sensor',
    sql_sensor_id=1,
    conn_id=sensor.connection.conn_id,
    pool=sensor.pool.pool if sensor.pool else None,
    poke_interval=sensor.poke_interval,
    timeout=sensor.timeout,
    ttl=sensor.ttl,
    main_argument=sensor.main_argument,
    sql=sensor.sql,
    dag=dag,
)

