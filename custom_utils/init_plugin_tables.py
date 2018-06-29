from airflow import settings
from dailyflow_models import *
from sys import argv
import logging


objects = [
    SQLSensor,
    SQLSensorRun,
    SQLDag,
    SQLDagTask,
]

def create_tables(session):
    for i in objects:
        try:
            i.__table__.create(bind=session.connection())
            session.commit()
            logging.info('Table %s created.' % i.__tablename__)
        except Exception as e:
            session.rollback()
            logging.error(e)


def drop_tables(session):
    for i in objects:
        try:
            i.__table__.drop(bind=session.connection())
            session.commit()
            logging.info('Table %s dropped.' % i.__tablename__)
        except Exception as e:
            session.rollback()
            logging.error(e)

if argv[1]:
    try:
        session = settings.Session()
        if argv[1] == '--create': # Run it only mannualy
            create_tables(session)
        elif argv[1] == '--drop':
            drop_tables(session)
        session.commit()
    except Exception as e:
        session.rollback()
        logging.info('ROLLBACK;')
    finally:
        session.close()
        logging.info('CLOSE CONNECTION.')
