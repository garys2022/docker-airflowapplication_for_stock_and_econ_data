import sys
from pathlib import Path
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.settings import Session

sys.path.append(r'/mypackage')

from stock_and_econ_data_ETL.extract.extract import load_stock_data, load_stock_action_data, extract_econ_data
from stock_and_econ_data_ETL.load.load import load_raw_stock_to_db_sqlorm, load_bronze_econ_data_to_db_orm
from stock_and_econ_data_ETL.transform.transform import stock_data_clean, \
    bronze_to_silver_single_econ_data \
    ,silver_to_gold_merge_and_clean
from stock_and_econ_data_ETL.model.db import Spy, Spy_silver\
    , Gold_stock_and_econ_data
from stock_and_econ_data_ETL.constant.constant import econ_datas,sma_period
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func

import signal
from contextlib import contextmanager
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutError(f"Timed out! this function should not run longer than {seconds} seconds")

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


default_args = {
    'owner': 'gary',
    'retries': 5,
    'retry_delay': timedelta(seconds=300),
    'catchup':True
}
#connecting to db https://stackoverflow.com/questions/67450086/cannot-get-custom-mysqloperator-to-work-in-airflow-extra-dejson-error-with-hook
# include __extra__ separately.

hook = BaseHook.get_hook(conn_id="mysql_azure")
conn = BaseHook.get_connection(conn_id="mysql_azure")
hostname = conn.host
login = conn.login
password = conn.password
port = conn.port
schema = conn.schema
ssl_path = conn.extra_dejson['ssl']['ca']

uri = f"mysql+pymysql://{login}:{password}@{hostname}:{port}/{schema}?ssl_ca={ssl_path}"
engine = create_engine(uri)

def raw_stock_data_etl(today):
    with time_limit(120):

        engine.connect()

        #record operation date
        today = datetime.strptime(today, '%Y-%m-%d').date()
        yesterday = today - timedelta(days=1)
        print(f"Task for {yesterday}")

        #extract data
        raw_stock_data = load_stock_data(start=yesterday, end=today)
        raw_stock_action_data = load_stock_action_data(start=yesterday, end=yesterday)
        print('data extracted')

        #merge stock and stock action data
        if raw_stock_data.size == 0:
            print('no data extracted from yf')
            return
        elif raw_stock_action_data.size == 0:
            merged_bronze_stock_data = raw_stock_data
            merged_bronze_stock_data['Dividends'] = None
            merged_bronze_stock_data['Stock Splits'] = None
        else:
            merged_bronze_stock_data = raw_stock_data.merge(raw_stock_action_data, how='left', on='Date')

        #load to db
        load_raw_stock_to_db_sqlorm(raw_stock_data=merged_bronze_stock_data, engine=engine)
        return


def raw_econ_data_etl(today):
    with time_limit(120):

        print('hooked to db')
        engine.connect()


        today = datetime.strptime(today, '%Y-%m-%d').date()
        plus_30_days = today + timedelta(days=30)
        yesterday = today - timedelta(days=1)
        print(f"Task for {today}")

        econ_data_bronze = extract_econ_data(start_date=yesterday.strftime("%Y-%m-%d"),
                                             end_date=plus_30_days.strftime("%Y-%m-%d"))

        for event in econ_data_bronze.keys():
            if econ_data_bronze[event].size == 0:
                print(f'No data extracted for {event} data')
        load_bronze_econ_data_to_db_orm(bronze_econ_data=econ_data_bronze, engine=engine)
        return


def stock_data_bronze_to_silver(today):
    with time_limit(600):

        print('hooked to db')
        engine.connect()

        today = datetime.strptime(today, '%Y-%m-%d').date()
        print(f"Task for {today}")

        with Session(engine) as session:

            print('testing')
            # get bronze data that not yet cleaned
            query = session.query(Spy).filter(~Spy.silver.any()).statement
            uncleaned_bronze_stock_data = pd.read_sql(query, session.bind)

            print(f"{uncleaned_bronze_stock_data.shape} data to clean")

            # clean data (bronze --> silver data)
            silver_stock_data = stock_data_clean(uncleaned_bronze_stock_data)

            # add to silver data db
            silver_stock_data_records = silver_stock_data.apply(lambda x: Spy_silver(**
                                                                                     {
                                                                                         'date': x['date'],
                                                                                         'open': x['open'],
                                                                                         'high': x['high'],
                                                                                         'low': x['low'],
                                                                                         'close': x['close'],
                                                                                         'adj_close': x['adj_close'],
                                                                                         'volume': x['volume'],
                                                                                         'dividends': x['dividends'],
                                                                                         'stock_splits': x[
                                                                                             'stock_splits'],
                                                                                         'is_dividends': x[
                                                                                             'is_dividends'],
                                                                                         'is_stock_splits': x[
                                                                                             'is_stock_splits'],
                                                                                         'bronze_id': x['bronze_id']
                                                                                     })
                                                                , axis=1).tolist()
            session.add_all(silver_stock_data_records)
            session.commit()
            print('data upload success')


def econ_data_bronze_to_silver(today):
    with time_limit(600):

        engine.connect()

        today = datetime.strptime(today, '%Y-%m-%d').date()
        print(f"Task for {today}")

        with Session(engine) as session:
            for econ_data in econ_datas.category.values():
                # got uncleaned object from bronze database
                query = session.query(econ_data.bronze_db).filter(~econ_data.bronze_db.silver.any()).statement
                uncleaned_bronze_econ_data = pd.read_sql(query, session.bind)

                print(f"{uncleaned_bronze_econ_data.shape} data to clean")

                if uncleaned_bronze_econ_data.size == 0:
                    print(f'no data to clean from {econ_data.name} bronze_db ')
                    continue

                # clean data
                single_silver_econ_data = bronze_to_silver_single_econ_data(uncleaned_bronze_econ_data)

                # load to db
                silver_econ_data_records = single_silver_econ_data.apply(
                    lambda x: econ_data.silver_db(**
                                                  {
                                                      'date': x['release_date'],
                                                      'actual': x['actual'],
                                                      'forecast': x['forecast'],
                                                      'previous': x['previous'],
                                                      'beat_forecast': x['beat_forecast'],
                                                      'beat_previous': x['beat_previous'],
                                                      'bronze_id': x['raw_id']
                                                  }
                                                  )
                    , axis=1).tolist()
                session.add_all(silver_econ_data_records)
                session.commit()


def stock_econ_data_silver_to_gold(today):
    pass

    engine.connect()

    today = datetime.strptime(today, '%Y-%m-%d').date()
    print(f"Task for {today}")

    with Session(engine) as session:

        # get the earliest date that stock data have not been processed
        earliest_stock_data_to_clean = session.query(func.min(Spy_silver.date)).filter(
            ~Spy_silver.gold.any()
        ).one()[0]

        if earliest_stock_data_to_clean == None:
            print('no uncleaned silver stock data find, return')
            return


        silver_econ_datas = {}
        # get the earliest data that econ data have not been processed
        #for econ_data in econ_datas.category.keys():
        #    econ_silver_db = econ_datas.category[econ_data].silver_db
        #    earliest_stock_data_to_clean = np.amin([earliest_stock_data_to_clean,
        #                                            session.query(func.min(econ_silver_db.date)).filter(
        #                                                ~econ_silver_db.gold.any()).one()[0]])

        # to extract enough data to process
        earliest_stock_data_to_clean = earliest_stock_data_to_clean - timedelta(days=600)
        # extract data to merge
        silver_stock_data = pd.read_sql(session.query(Spy_silver)
                                        .filter(Spy_silver.date >= earliest_stock_data_to_clean)
                                        .statement, session.bind)
        for econ_data in econ_datas.category.keys():
            econ_silver_db = econ_datas.category[econ_data].silver_db
            silver_econ_datas[econ_data] = pd.read_sql(session.query(econ_silver_db)
                                                       .filter(econ_silver_db.date >= earliest_stock_data_to_clean)
                                                       .statement, session.bind)

        gold_data = silver_to_gold_merge_and_clean(silver_stock_data, silver_econ_datas)

        # delete before insert
        session.query(Gold_stock_and_econ_data)\
            .filter(Gold_stock_and_econ_data.date.in_(gold_data.date.tolist()))\
            .delete(synchronize_session='evaluate')

        gold_data_to_add = gold_data.apply(
            lambda x: Gold_stock_and_econ_data(**
                                               {col: x[col] for col in gold_data.columns}), axis=1).tolist()

        session.add_all(gold_data_to_add)
        session.commit()

with DAG(
        default_args=default_args,
        dag_id='extract_raw_stock_data_azure',
        description='v13',
        start_date=datetime(2023, 1, 23),
        schedule_interval='0 0 * * 2-6'
) as dag:
    get_raw_stock_data = PythonOperator(
        task_id='bronze_stock_data_ETL',
        python_callable=raw_stock_data_etl,
        op_kwargs={
            'today': '{{ ds }}'
        }
    )

    get_bronze_econ_data = PythonOperator(
        task_id='bronze_econ_data',
        python_callable=raw_econ_data_etl,
        op_kwargs={
            'today': '{{ ds }}'
        }
    )

    bronze_to_silver_stock_data = PythonOperator(
        task_id='stock_data_bronze_to_silver',
        python_callable=stock_data_bronze_to_silver,
        op_kwargs={
            'today': '{{ ds }}'
        }
    )

    bronze_to_silver_econ_data = PythonOperator(
        task_id='econ_data_bronze_to_silver',
        python_callable=econ_data_bronze_to_silver,
        op_kwargs={
            'today': '{{ ds }}'
        }
    )

    silver_to_gold_data = PythonOperator(
        task_id='stock_and_econ_data_silver_to_gold',
        python_callable=stock_econ_data_silver_to_gold,
        op_kwargs={
            'today': '{{ ds }}'
        }

    )

    get_raw_stock_data >> bronze_to_silver_stock_data
    get_bronze_econ_data >> bronze_to_silver_econ_data
    [bronze_to_silver_stock_data,bronze_to_silver_econ_data] >> silver_to_gold_data
