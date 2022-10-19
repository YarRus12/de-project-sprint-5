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


log = logging.getLogger(__name__)


def check_and_create(connect_to_db, table_schema, tables):
    inside_cursor = connect_to_db.cursor()
    # Делаем выборку наименований таблиц их информационной схемы postgresql
    check_stg_select = f"""SELECT table_name  FROM information_schema.columns
    WHERE table_schema = '{table_schema}';"""
    inside_cursor.execute(check_stg_select)
    # Из кортежей выбираем первый элемент и преобразуем его в список
    check_result = list(x[0] for x in inside_cursor.fetchall())
    for i in tables:
        if i not in check_result:
            inside_cursor.execute(f"""create SCHEMA IF NOT EXISTS {table_schema}""")
            script_name = f'/lessons/dags/SQL_scripts/{table_schema}/ddl_{i}.sql'
            inside_cursor.execute(open(script_name, 'r').read())
            connect_to_db.commit()
            log.warning(f"В схеме {table_schema} нехватает таблиц {i}, выполнен скрипт {script_name}")
    log.info(f'Проверка схемы {table_schema} завершена успешно')


if __name__ == '__main__':
    check_and_create()