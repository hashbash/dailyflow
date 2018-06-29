from airflow import models
from airflow.models import ID_LEN
from sqlalchemy.dialects.postgres import ARRAY
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref
from sqlalchemy.orm import relationship
from sqlalchemy import UniqueConstraint

from dailyflow_utils import State

DeclarativeBase = declarative_base()


class SQLSensor(DeclarativeBase):
    __tablename__ = 'sql_sensor'

    id = Column(Integer, primary_key=True)
    label = Column(String, unique=True, nullable=False)
    description = Column(Text)
    created = Column(DateTime, default=func.now())
    user_id = Column(Integer(), ForeignKey(models.User.id), nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    ttl = Column(Integer, default=3600 * 24 * 7, nullable=False)
    timeout = Column(Integer, default=3600 * 3, nullable=False)
    # parent_labels = Column(ARRAY(String()), ForeignKey('sql_sensor.label'))
    parent_labels = Column(ARRAY(String()))
    cross_check = Column(Boolean, default=False, nullable=False)
    conn_id = Column(Integer(), ForeignKey(models.Connection.id), nullable=False)
    pool_id = Column(Integer(), ForeignKey(models.Pool.id))
    main_table = Column(String)
    poke_interval = Column(Integer, default=60, nullable=False)
    main_argument = Column(String, nullable=False, default='')
    sql = Column(Text, nullable=False)
    _extra = Column('extra', String(5000))

    owner = relationship(
        models.User,
        foreign_keys=[user_id]
    )
    connection = relationship(
        models.Connection,
        foreign_keys=[conn_id]
    )
    pool = relationship(
        models.Pool,
        foreign_keys=[pool_id]
    )
    positive_int = CheckConstraint(
        and_(
            ttl > 0,
            timeout > 0,
        )
    )

    # todo: implement check or relationship for parent_labels
    # todo: implement getter and setter for _extra

    def __repr__(self):
        return self.label


class SQLSensorRun(DeclarativeBase):
    __tablename__ = 'sql_sensor_run'

    id = Column(Integer, primary_key=True)
    sql_sensor_id = Column(Integer, ForeignKey('sql_sensor.id'), index=True, unique=False)
    created = Column(DateTime, default=func.now())
    updated = Column(DateTime, default=func.now(), onupdate=func.now())
    # dag_id = Column(String(ID_LEN), ForeignKey(models.DagModel.dag_id))
    dag_id = Column(String(ID_LEN))
    dag_run_id = Column(String(100))
    task_id = Column(String(ID_LEN))
    execution_date = Column(DateTime)
    validation_date = Column(DateTime)
    status = Column(String)
    result = Column(Text)
    rendered_argument = Column(String)
    sql = Column(Text)

    sql_sensor = relationship(
        SQLSensor,
        foreign_keys=[sql_sensor_id]
    )

    source_dag = relationship(
        models.DagModel,
        foreign_keys=[dag_id],
        primaryjoin=models.DagModel.dag_id == dag_id,
    )
    source_dag_run = relationship(
        models.DagRun,
        foreign_keys=[
            models.DagRun.dag_id,
            models.DagRun.run_id,
            models.DagRun.execution_date,
        ],
        primaryjoin=and_(
            models.DagRun.dag_id == dag_id,
            models.DagRun.run_id == dag_run_id,
            models.DagRun.execution_date == execution_date,
        )
    )
    unique_cols = Index(
        'unique_for_non_expired',
        sql_sensor_id,
        execution_date,
        rendered_argument,
        unique=True,
        postgresql_where=(status != State.EXPIRED),
    )

    def __repr__(self):
        return '{0} ({1})'.format(self.sql_sensor_id, self.id)


class SQLDag(DeclarativeBase):
    __tablename__ = 'sql_dag'

    id = Column(String(ID_LEN), primary_key=True)
    description = Column(Text)
    created = Column(DateTime, default=func.now())
    user_id = Column(Integer(), ForeignKey(models.User.id), nullable=False)
    enabled = Column(Boolean, default=True, nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    schedule_interval = Column(String, nullable=False)
    retries = Column(Integer, default=2)
    retry_delay = Column(Integer, default=60 * 10)
    pool_id = Column(Integer(), ForeignKey(models.Pool.id))
    sla = Column(Integer, default=3600 * 5)
    drill_down_sql_sensors = Column(ARRAY(String(), ForeignKey('sql_sensor.label')))
    specific_sql_sensors = Column(ARRAY(String(), ForeignKey('sql_sensor.label')))
    extra = Column(String(5000))

    child_tasks = relationship(
        "SQLDagTask",
        back_populates="parent_sql_dag",
    )
    owner = relationship(
        models.User,
        foreign_keys=[user_id],
    )
    pool = relationship(
        models.Pool,
        foreign_keys=[pool_id],
    )

    def __repr__(self):
        return self.id


class SQLDagTask(DeclarativeBase):
    __tablename__ = 'sql_dag_task'

    id = Column(Integer, primary_key=True)
    label = Column(String(ID_LEN), nullable=False)
    description = Column(Text)
    sql_dag_id = Column(String(ID_LEN), ForeignKey('sql_dag.id'), nullable=False)
    created = Column(DateTime, default=func.now())
    upstream_task_id = Column(Integer, ForeignKey('sql_dag_task.id'))
    conn_id = Column(Integer(), ForeignKey(models.Connection.id), nullable=False)
    pool_id = Column(Integer(), ForeignKey(models.Pool.id))
    sql = Column(Text, nullable=False)

    parent = relationship(
        "SQLDagTask",
        remote_side=id,
        backref=backref('parent_values', lazy='joined'),
    )
    parent_sql_dag = relationship(
        "SQLDag",
        back_populates="child_tasks"
    )
    connection = relationship(
        models.Connection,
        foreign_keys=[conn_id]
    )
    pool = relationship(
        models.Pool,
        foreign_keys=[pool_id]
    )
    unique_cols = UniqueConstraint(
        label,
        sql_dag_id
    )

    def __repr__(self):
        return '{0} ({1})'.format(self.label, self.sql_dag_id)
