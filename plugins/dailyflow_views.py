from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.www import utils as wwwutils
from dailyflow_models import *
from flask import Blueprint
from flask_admin.contrib.sqla import ModelView
from airflow.settings import Session


# Creating a flask admin BaseView
class SQLSensorsView(wwwutils.SuperUserMixin, ModelView):
    list_template = 'airflow/model_list.html'
    edit_template = 'airflow/model_edit.html'
    create_template = 'airflow/model_create.html'
    column_display_actions = True
    page_size = 500
    form_columns = (
        'enabled',
        'label',
        'description',
        'owner',
        'parent_labels',
        'cross_check',
        'ttl',
        'connection',
        'pool',
        'poke_interval',
        'main_table',
        'main_argument',
        'sql',
    )
    verbose_name = "SQL Sensor"
    verbose_name_plural = "SQL Sensors"
    column_default_sort = ('label', False)
    column_list = (
        'enabled',
        'label',
        'description',
        'owner',
        'parent_labels',
        'cross_check',
        'connection',
        'main_table',
        'sql',
    )
    column_labels = {
        'ttl': "ttl",
        'sql': "SQL Statement",
    }
    column_filters = ('label', 'owner.username', 'connection.conn_id')
    column_searchable_list = ('owner.username', 'label', 'connection.conn_id', 'sql')

    # have no idea how to add choises within list
    # form_choices = {
    #     'parent_labels': [
    #         (i.label, i.label)
    #         for i in (
    #             Session().query(SQLSensor.label)
    #         )
    #     ]
    # }


class SQLSensorRunView(wwwutils.SuperUserMixin, ModelView):
    named_filter_urls = True
    can_create = False
    can_edit = False
    can_delete = True
    column_display_pk = True

    verbose_name_plural = "SQL Sensor runs"
    verbose_name = "SQL Sensor run"
    column_default_sort = ('created', True)

    column_list = (
        'id',
        'created',
        'status',
        'sql_sensor_id',
        'dag_id',
        'dag_run_id',
        'task_id',
        'execution_date',
        'result',
        'sql',
    )
    column_filters = (
        'id',
        'created',
        'status',
        'sql_sensor_id',
        'dag_id',
        'dag_run_id',
        'task_id',
        'execution_date',
        'result',
        'sql',
    )
    column_labels = {
        'sql': "SQL Statement",
    }


class SQLDagView(wwwutils.SuperUserMixin, ModelView):
    list_template = 'airflow/model_list.html'
    edit_template = 'airflow/model_edit.html'
    create_template = 'airflow/model_create.html'
    column_display_actions = True
    page_size = 500
    form_columns = (
        'enabled',
        'id',
        'description',
        'owner',
        'start_date',
        'end_date',
        'schedule_interval',
        'retries',
        'retry_delay',
        'pool',
        'sla',
        'drill_down_sql_sensors',
        'specific_sql_sensors',
        'extra',
    )
    verbose_name = "SQL DAG"
    verbose_name_plural = "SQL DAGs"
    column_default_sort = ('created', True)
    column_list = (
        'enabled',
        'id',
        'owner',
        'schedule_interval',
        'drill_down_sql_sensors',
        'specific_sql_sensors',
        'extra',
        'created'
    )
    column_filters = (
        'enabled',
        'id',
        'owner',
        'schedule_interval',
        'description',
    )
    column_searchable_list = (
        'id',
        'description',
        'drill_down_sql_sensors',
        'specific_sql_sensors',
        'extra',
    )
    column_labels = {
        'sla': "sla",
        'id': 'DAG id',
    }


class SQLDagTaskView(wwwutils.SuperUserMixin, ModelView):
    list_template = 'airflow/model_list.html'
    edit_template = 'airflow/model_edit.html'
    create_template = 'airflow/model_create.html'
    column_display_actions = True
    page_size = 500
    form_columns = (
        'parent_sql_dag',
        'label',
        'parent',
        'description',
        'connection',
        'pool',
        'sql',
    )
    verbose_name = "SQL DAG Task"
    verbose_name_plural = "SQL DAG Tasks"
    column_default_sort = ('created', True)
    column_list = (
        'id',
        'label',
        'parent',
        'sql_dag_id',
        'connection',
        'sql',
    )
    column_filters = (
        'id',
        'label',
        'connection.conn_id',
        'pool.pool'
    )
    column_searchable_list = (
        'id',
        'label',
        'upstream_task_id',
        'sql_dag_id',
        'conn_id',
        'sql',
    )
    column_labels = {
        'sql': "SQL Statement",
    }


sql_sensors = SQLSensorsView(SQLSensor, Session, category="Daily Flow", name="SQL Sensors")
sql_sensor_runs = SQLSensorRunView(SQLSensorRun, Session, category="Daily Flow", name="SQL Sensor Runs")
sql_dags = SQLDagView(SQLDag, Session, category="Daily Flow", name="SQL DAGs")
sql_dag_tasks = SQLDagTaskView(SQLDagTask, Session, category="Daily Flow", name="SQL DAG Tasks")

# Creating a flask blueprint to integrate the templates and static folder
dailyflow_bp = Blueprint(
    "dailyflow", __name__,
    template_folder='templates',  # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/dailyflow'
)


# Defining the plugin class
class DailyFlowPlugin(AirflowPlugin):
    name = "dailyflow"
    admin_views = [sql_sensors, sql_sensor_runs, sql_dags, sql_dag_tasks]
    flask_blueprints = [dailyflow_bp]
