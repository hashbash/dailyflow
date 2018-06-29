from airflow import configuration as conf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class State(object):
    """Static class to avoid hardcoding"""
    NONE = None
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    DOWNSTREAM_FAILED = 'downstream_failed'
    EXPIRED = 'expired'
    FATAL = 'fatal'

    REMOVED = 'removed'

    @classmethod
    def ok(cls):
        """A list of states indicating that a run is done well."""
        return [cls.SUCCESS, ]

    @classmethod
    def unfinished(cls):
        """A list of states indicating that a run is unfinished
           and is required to do something."""
        return [
            cls.RUNNING,
            cls.FAILED,
            cls.DOWNSTREAM_FAILED,
            cls.EXPIRED,
            cls.FATAL,
            cls.NONE,
        ]

    @classmethod
    def for_restart(cls):
        return [
            cls.FAILED,
            cls.DOWNSTREAM_FAILED,
            cls.EXPIRED,
            cls.FATAL,
        ]


class Session:
    def __init__(self, autocommit=False, autoflush=True):
        self.autocommit = autocommit
        self.autoflush = autoflush
        self.session = None

    def create(self):
        SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
        engine_args = dict(pool_size=conf.getint('core', 'SQL_ALCHEMY_POOL_SIZE'),
                           pool_recycle=conf.getint('core', 'SQL_ALCHEMY_POOL_RECYCLE'))
        engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
        session = sessionmaker(autocommit=self.autocommit, autoflush=self.autoflush, bind=engine)
        self.session = session()
        return self.session

    def get(self):
        return self.session or self.create()
