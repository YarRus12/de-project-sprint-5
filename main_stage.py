from typing import List
from urllib.parse import quote_plus as quote
from pymongo.mongo_client import MongoClient
import pymongo
from airflow.models.variable import Variable
from datetime import datetime, timedelta
from airflow import DAG
import psycopg2
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
import datetime as dt
import logging
import re
import json
import logging
import Check
from decimal import Decimal
import Stage_upload
import requests


log = logging.getLogger(__name__)

"""Констатны с наименованиями таблиц по схемам"""
ALL_STG_TABLES = ['bonussystem_ranks', 'bonussystem_users', 'bonussystem_events', 'ordersystem_orders', 'ordersystem_restaurants', 'ordersystem_users', 'srv_etl_settings', 'delivery', 'couriers']
#ALL_DDS_TABLES = ['dm_users', 'dm_restaurants', 'dm_products', 'dm_timestamps', 'dm_orders', 'fct_product_sales', 'dm_couriers', 'dm_delivery']
#ALL_CDM_TABLES = ['dm_settlement_report', 'dm_courier_ledger']
host='https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/'
headers={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "russu",
    "X-Cohort": "5"}

class MongoConnect:
    def __init__(self,
                 cert_path: str,  # Путь до файла с сертификатом
                 user: str,  # Имя пользователя БД
                 pw: str,  # Пароль пользователя БД
                 host: List[str],  # Список хостов для подключения
                 rs: str,  # replica set.
                 auth_db: str,  # БД для аутентификации
                 main_db: str  # БД с данными
                 ) -> None:
        self.user = user
        self.pw = pw
        self.hosts = host
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=','.join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db)
    # Создаём клиент к БД
    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]


MONGO_DB_CERTIFICATE_PATH = "../de-project-4(v1)/PracticaumSp5MongoDb.crt"
MONGO_DB_DATABASE_NAME = "db-mongo"
MONGO_DB_HOST = "rc1a-ba83ae33hvt4pokq.mdb.yandexcloud.net:27018"
MONGO_DB_PASSWORD = "student1"
MONGO_DB_REPLICA_SET = "rs01"
MONGO_DB_USER = "student"
url = f'mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_HOST}/?replicaSet={MONGO_DB_REPLICA_SET}&authSource={MONGO_DB_DATABASE_NAME}'
mongo_client = pymongo.MongoClient(url, tlsCAFile=MONGO_DB_CERTIFICATE_PATH)[MONGO_DB_DATABASE_NAME]
    










def check_database():
    Check.check_and_create('stg', ALL_STG_TABLES)
#    Check.check_and_create('dds', ALL_DDS_TABLES)
#    Check.check_and_create('cdm', ALL_CDM_TABLES)

connect_to_scr = psycopg2.connect("host=rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net port=6432 sslmode=require dbname=de-public user=student password=student1")
connect_to_db = psycopg2.connect("host=localhost port=15432 dbname=de user=jovyan password=jovyan")
    


def download_from_postgresql(connect_to_scr, connect_to_db, out_schema, out_table, schema, table, id_column):
    Stage_upload.download_postgresdata_to_staging(connect_to_scr, connect_to_db, out_schema=out_schema, out_table=out_table, schema=schema, table=table, id_column=id_column)

def download_from_mongo(mongo_client, collection_name, schema, table):
    Stage_upload.download_mongo_to_staging(mongo_client=mongo_client, collection_name=collection_name, schema=schema, table_name=table)

def download_from_api(host, headers, schema, table):
    Stage_upload.download_api_to_staging(host=host, headers=headers, schema=schema, table=table)


"""ТЕСТОВЫЙ БЛОК ДО ПРИСОЕДИНЕНИЯ К AIRFLOW"""
check_database()
download_from_postgresql(connect_to_scr=connect_to_scr, connect_to_db=connect_to_db, out_schema='public', out_table='ranks', schema='stg', table='bonussystem_ranks',id_column='True')
download_from_postgresql(connect_to_scr=connect_to_scr, connect_to_db=connect_to_db, out_schema='public', out_table='users', schema='stg', table='bonussystem_users',id_column='False')
download_from_postgresql(connect_to_scr=connect_to_scr, connect_to_db=connect_to_db, out_schema='public', out_table='outbox', schema='stg', table='bonussystem_events',id_column='True')
download_from_mongo(mongo_client=mongo_client, collection_name='orders', schema='stg', table='ordersystem_orders')
download_from_mongo(mongo_client=mongo_client, collection_name='restaurants', schema='stg', table='ordersystem_restaurants')
download_from_mongo(mongo_client=mongo_client, collection_name='users', schema='stg', table='ordersystem_users')
download_from_api(host=host+'deliveries', headers=headers, schema='stg', table='delivery')
download_from_api(host=host+'couriers', headers=headers, schema='stg', table='couriers')
#restaurants = requests.get(host="https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants", headers=headers, schema='stg', table='restoraunts')


"""

dag = DAG(
    schedule_interval='* 1 * * *',
    dag_id='download data to stage',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['stage', 'mongo', 'postgresql'],
    is_paused_upon_creation=False
)

rank_load = PythonOperator(task_id='rank_load',
                                 python_callable=download_from_postgresql,
                                 op_kwargs={'connect': connect_to_scr, 'out_schema': 'public', 'out_table': 'ranks', 'schema': 'stg', 'table': 'bonussystem_ranks', 'id_column': 'True'},
                                 dag=dag)
users_load = PythonOperator(task_id='users_load',
                                 python_callable=download_from_postgresql,
                                 op_kwargs={'connect': connect_to_scr, 'out_schema': 'public', 'out_table': 'users', 'schema': 'stg', 'table': 'bonussystem_users', 'id_column': 'False'},
                                 dag=dag)
outbox_load = PythonOperator(task_id='outbox_load',
                                 python_callable=download_from_postgresql,
                                 op_kwargs={'connect': connect_to_scr, 'out_schema': 'public', 'out_table': 'outbox', 'pg_schema': 'stg', 'pg_table': 'bonussystem_events', 'id_column': 'True'},
                                 dag=dag)
orders_load = PythonOperator(task_id='orders',
                                 python_callable=download_from_mongo,
                                 op_kwargs={'collection_name': 'orders', 'schema': 'stg', 'table_name': 'ordersystem_orders'},
                                 dag=dag)
restaurants_load = PythonOperator(task_id='restaurants',
                                 python_callable=download_from_mongo,
                                 op_kwargs={'collection_name': 'restaurants', 'schema': 'stg', 'table_name': 'ordersystem_restaurants'},
                                 dag=dag)
users_load = PythonOperator(task_id='users',
                                 python_callable=download_from_mongo,
                                 op_kwargs={'collection_name': 'users', 'schema': 'stg', 'table_name': 'ordersystem_users'},
                                 dag=dag)                             
couriers_load = = PythonOperator(task_id='couriers_load',
                                 python_callable=download_from_api,
                                 op_kwargs={'host': 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers',
                                 'headers': headers, 'schema': 'stg', 'table': 'couriers'},
                                 dag=dag)
delivery_load = = PythonOperator(task_id='delivery_load',
                                 python_callable=download_from_api,
                                 op_kwargs={'host': 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries',
                                 'headers': headers, 'schema': 'stg', 'table': 'delivery'},
                                 dag=dag)

rank_load, users_load, outbox_load, orders_load, restaurants_load, users_load, couriers_load, delivery_load

"""