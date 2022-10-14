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
import requests

log = logging.getLogger(__name__)


def download_postgresdata_to_staging(connect_to_scr, connect_to_db, out_schema, out_table, table, schema, id_column = 'True'):
    cursor_to_stg = connect_to_db.cursor()
    cursor_to_stg.execute(f"""SELECT coalesce(max(id), -1) FROM {schema}.{table}""")
    max_id = cursor_to_stg.fetchone()[0]
    cur = connect_to_scr.cursor()
    sql = f"""SELECT * FROM {out_schema}.{out_table} WHERE id > {max_id};"""
    cur.execute(sql)
    src_data = cur.fetchall()
    log.info(f"Данные из внешнего источника: {out_schema}.{out_table} загружены в память")
    cursor_to_stg.execute(f"""SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' and table_name = '{table}';""")
    columns = cursor_to_stg.fetchall()
    log.info(f"Сведения о колонках таблицы {schema}.{table} получены")
    if id_column == 'False':
        src_data = [(x[1:]) for x in list(map(list, src_data))]
        columns = ', '.join([x[0] for x in columns[1:]])
    else:
        columns = ', '.join([x[0] for x in columns])
        src_data = list(map(list, src_data))
    log.info(f"Колонки таблицы {schema}.{table} и данные для нее обработаны")
    final_query = []
    for line in src_data:
        vars = []
        for x in line:
            if type(x) == Decimal:
                x = str(x).replace('Decimal(','').replace(')','')
                vars.append(f"'{x}'")
            else:
                vars.append(f"'{x}'")
        final_query.append(f"""({','.join(vars)})""")
    log.info(f"Данные подготовлены для загрузки в staing слой таблицы {table}")
    for row in final_query:
        try:
            query = f"""INSERT INTO {schema}.{table} ({columns}) VALUES {''.join(row)}"""
            cursor_to_stg.execute(query)
        except Exception as error:
            log.warning(f'{error}')    
        connect_to_db.commit()
    log.info(f"Данные загружены в  таблицу {schema}.{table}")




def download_mongo_to_staging(mongo_client, collection_name, schema, table_name):
    collection = mongo_client[collection_name]
    connect_to_db = psycopg2.connect("host=localhost port=15432 dbname=de user=jovyan password=jovyan")
    cursor_to_stg = connect_to_db.cursor()
    cursor_to_stg.execute(f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM {schema}.srv_etl_settings WHERE workflow_settings = '{table_name}';""")
    max_date = cursor_to_stg.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
    filter = {'update_ts': {'$gt': max_date}}
    sort = [('update_ts', -1)]
    docs = list(collection.find(filter=filter, sort=sort, limit=1000)) 
    log.info(f"Обнаружено {len(docs)} новых записей")
    dates = []
    if len(docs) > 0:
        key_values = {}
        for n in docs:
            value = {}
            for k,v in n.items():
                if k in ('_id', 'update_ts'):
                    key_values.update({k:str(v)})
                else:
                    value.update({k:str(v)})
            result = []
            text = f"""{value}"""
            for i in text:
                if i == "'":
                    i = '"'
                result.append(i)
            record_date = key_values.get('update_ts')
            dates.append(record_date)
            value = ''.join(result)
            sql = f"""INSERT INTO {schema}.{table_name} (object_id, update_ts, object_value) VALUES ({', '.join([("'"+str(x)+"'") for k,x in key_values.items()])}, '{value}') ON CONFLICT (object_id) DO NOTHING;"""
            cursor_to_stg.execute(sql)
            connect_to_db.commit()
        log.info('Данные записаны')
        meta_sql = f"""INSERT INTO {schema}.srv_etl_settings as tbl (workflow_key, workflow_settings) VALUES ('{max(dates)}','{table_name}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;"""
        cursor_to_stg.execute(meta_sql)
        log.info('Мета данные сохранены')
        connect_to_db.commit()
    else:
        log.info('НОВЫЕ ЗАПИСИ ОТСУТСТВУЮТ')



def delivery_data(data, max_date):
    values = []
    dates = []
    for row in data:
        order_id = row.get('order_id')
        order_ts = row.get('order_ts')
        delivery_id = row.get('delivery_id')
        courier_id = row.get('courier_id')
        address = row.get('address')
        rate = row.get('rate')
        delivery_ts = row.get('delivery_ts')
        sum_delivey = row.get('sum')
        tip_sum = row.get('tip_sum')
        try:
           a = datetime.strptime(order_ts, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            a = datetime.strptime(order_ts, '%Y-%m-%d %H:%M:%S')
        if a > max_date:
            values.append(f"('{order_id}', '{order_ts}', '{delivery_id}', '{courier_id}', '{address}', '{delivery_ts}', '{rate}', '{sum_delivey}', '{tip_sum}')")
            dates.append(order_ts)
        if len(dates) == 0:
            return None, None
    return values, max(dates)


def couriers_data(data):
    values = []
    for row in data:
        id = row.get('_id')
        name = row.get('name')
        values.append(f"('{id}', '{name}')")
    return values


def download_api_to_staging(host, headers, schema, table):
    data = requests.get(host, headers=headers)
    data = data.json()
    connect_to_db = psycopg2.connect("host=localhost port=15432 dbname=de user=jovyan password=jovyan")
    cursor_to_stg = connect_to_db.cursor()
    cursor_to_stg.execute(f"""SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' and table_name = '{table}';""")
    columns = cursor_to_stg.fetchall()
    columns = [x[0] for x in columns[1:]]
    columns_names = ', '.join(columns)
    if table == 'delivery':
        cursor_to_stg.execute(f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM {schema}.srv_etl_settings WHERE workflow_settings = '{table}';""")
        max_date = cursor_to_stg.fetchone()[0]
        max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
        values, max_date = delivery_data(data, max_date)
        if values is None:
            log.info(f"В источнике нет данных свежее, чем те что уже записаны в таблицу {table}")
            return 200
    elif table == 'couriers':
        values = couriers_data(data)
    else:
        log.warning(f'Неверное имя таблицы {table}')
        raise('Проверьте вводимые данные')
    if table == 'delivery':
        for row in values:
            sql = f"""INSERT INTO {schema}.{table} ({columns_names}) VALUES {row} ON CONFLICT DO NOTHING;"""
            cursor_to_stg.execute(sql)
        meta_sql = f"""INSERT INTO {schema}.srv_etl_settings as tbl (workflow_key, workflow_settings) VALUES ('{max_date}','{table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;"""
        cursor_to_stg.execute(meta_sql)
    if table == 'couriers':
        for row in values:
            sql = f"""INSERT INTO {schema}.{table} ({columns_names}) VALUES {row} ON CONFLICT DO NOTHING;"""
            cursor_to_stg.execute(sql)
    connect_to_db.commit()
    log.info(f'Загрузка данных в таблицу {table} завершена')


if __name__ == '__main__':
    download_postgresdata_to_staging()
    download_mongo_to_staging()
    download_api_to_staging()