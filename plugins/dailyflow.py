import logging
from datetime import datetime, timedelta
from time import sleep

from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSensorTimeout

from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from sqlalchemy import func

from dailyflow_models import SQLSensorRun as SensorRunModel
from dailyflow_utils import State

from dailyflow_utils import Session


class SQLSensorOperator(BaseOperator):
    template_fields = (
        'sql',
        'rendered_argument',
        '_dag_id',
        '_dag_run_id',
        '_task_id',
        '_execution_date',
    )
    template_ext = ('.sql',)
    ui_color = '#c9fcf2'

    @apply_defaults
    def __init__(
            self,
            sql_sensor_id,
            conn_id,
            pool,
            poke_interval,
            timeout,
            ttl,
            main_argument,
            sql,
            **kwargs):
        self.sql_sensor_id = sql_sensor_id
        self.conn_id = conn_id
        self.pool = pool
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.ttl = ttl
        self.sql = sql
        self.rendered_argument = main_argument

        self._dag_id = '{{ dag.dag_id }}'
        self._dag_run_id = '{{ run_id }}'
        self._task_id = '{{ task_instance.task_id }}'
        self._execution_date = '{{ execution_date }}'
        # todo: forward _extra to parent class
        self.session = None
        self.run = None
        self.query_result = None
        self.bool_result = None
        super(SQLSensorOperator, self).__init__(**kwargs)

    def get_new_run(self):
        new_run = SensorRunModel(
            sql_sensor_id=self.sql_sensor_id,
            dag_id=self._dag_id,
            dag_run_id=self._dag_run_id,
            task_id=self._task_id,
            execution_date=self._execution_date,
            rendered_argument=self.rendered_argument,
            sql=self.sql,
            status=State.RUNNING,
        )
        return new_run

    def execute_statement(self):
        hook = BaseHook.get_connection(self.conn_id).get_hook()
        logging.info('Executing: %s', self.sql)
        self.query_result = hook.get_records(self.sql)
        logging.info('Execution result: %s' % self.query_result)

    def get_bool_result(self):
        if not self.query_result:
            return False
        else:
            if str(self.query_result[0][0]) in ('0', '',):
                return False
            elif not bool(self.query_result[0][0]):
                return False
            else:
                return True

    def is_expired_run(self):
        if self.run and (
                # not self.run.validation_date
                # or
                (
                self.run.status in State.ok()
                and
                self.run.validation_date + timedelta(seconds=self.ttl) < datetime.now()
                )
                or
                (
                self.run.status == State.RUNNING # pick up runs, probably was failed at runtime
                and self.run.created + timedelta(seconds=self.timeout) < datetime.now()
                )
        ):
            return True
        return False

    def set_state(self, state, set_result=False):
        if self.run:
            logging.info('Change status from %s to %s' % (self.run.status, state))
            self.run.sql_sensor_id = self.sql_sensor_id
            self.run.dag_id = self._dag_id
            self.run.dag_run_id = self._dag_run_id
            self.run.task_id = self._task_id
            self.run.execution_date = self._execution_date
            self.run.rendered_argument = self.rendered_argument
            self.run.sql = self.sql
            self.run.status = state
            if set_result:
                self.run.validation_date = func.now()
            if set_result and self.query_result:
                self.run.result = str(self.query_result[0][0])
            self.session.commit()
        else:
            raise AirflowException("Cannot set status. Run doesn't exists.")

    def poke(self, context):
        self.run = self.session.query(SensorRunModel) \
            .filter(SensorRunModel.sql_sensor_id == self.sql_sensor_id) \
            .filter(SensorRunModel.rendered_argument == self.rendered_argument) \
            .order_by(SensorRunModel.created.desc()) \
            .first()
            
        if not self.run or self.run.status == State.EXPIRED:
            self.run = self.get_new_run()
            self.session.add(self.run)
            self.session.commit()
            self.execute_statement()
            self.bool_result = self.get_bool_result()
            return self.bool_result
        elif self.is_expired_run():
            logging.info('SQLSensorRun %s is expired.' % self.run.id)
            self.set_state(State.EXPIRED)
            return False
        elif self.run.status == State.SUCCESS:
            return True
        elif self.run.status == State.RUNNING:
            return False
        elif self.run.status in State.for_restart():
            self.set_state(State.RUNNING)
            self.run.sql_sensor_id = self.sql_sensor_id
            self.run.dag_id = self._dag_id
            self.run.dag_run_id = self._dag_run_id
            self.run.task_id = self._task_id
            self.run.execution_date = self._execution_date
            self.run.rendered_argument = self.rendered_argument
            self.run.sql = self.sql
            self.session.commit()
            self.execute_statement()
            self.bool_result = self.get_bool_result()
            return self.bool_result
        return False

    def execute(self, context):
        self.session = Session().get()
        started_at = datetime.now()
        success = False
        counter = 0
        while not success:
            logging.info('Iteration sequence: %s' % counter)
            try:
                if (datetime.now() - started_at).total_seconds() > self.timeout:
                    raise AirflowSensorTimeout
                success = self.poke(context)
                logging.info('Bool result of SQLSensorRun is %s from %s' % (success, self.query_result))
                if success:
                    if self.run and self.run.status != State.SUCCESS:
                        self.set_state(State.SUCCESS, set_result=True)
                    logging.info("Success criteria met. Exiting. Result was: %s" % self.query_result)
                elif self.run.status == State.EXPIRED:
                    logging.info('Run is expired. Skip slipping.')
                else:
                    logging.info('Current status is %s, nap time %s.' % (self.run.status, self.poke_interval))
                    sleep(self.poke_interval)
            except AirflowSensorTimeout as e:
                self.set_state(State.FAILED)
                raise AirflowSensorTimeout('Snap. Time is OUT.')
            except Exception as e:
                self.set_state(State.FATAL)
                raise Exception('Fatal exception: %s' % e)
            finally:
                counter += 1
                self.session.commit()
        logging.info('Execution finished. Close session.')
        self.session.close()
    
    def on_kill(self):
        if self.run and self.run.dag_id == self._dag_id:
            self.set_state(State.FATAL)
        raise AirflowException('Task was killed by someting external')
