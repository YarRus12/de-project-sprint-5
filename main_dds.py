from runpy import run_path
from turtle import circle
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
import DDS_upload
import requests



ALL_DDS_TABLES = ['dm_users', 'dm_restaurants', 'dm_products', 'dm_timestamps', 'dm_orders', 'fct_product_sales', 'dm_couriers', 'dm_delivery']


log = logging.getLogger(__name__)

connect_to_db = psycopg2.connect("host=localhost port=15432 dbname=de user=jovyan password=jovyan")


def check_database():
    Check.check_and_create('dds', ALL_DDS_TABLES)

def upload_restaurant_dds(connect_to_db, dds_table, stg_table):
    DDS_upload.upload_restaurant(connect_to_db=connect_to_db,dds_table=dds_table,stg_table=stg_table)

def upload_timespamp_dss(connect_to_db, dds_table, stg_orders, stg_delivery):
    DDS_upload.upload_timespamp(connect_to_db=connect_to_db, dds_table=dds_table, stg_orders=stg_orders, stg_delivery=stg_delivery)

def upload_users_dds(connect_to_db, dds_table, stg_table):
    DDS_upload.upload_users(connect_to_db=connect_to_db, dds_table=dds_table, stg_table=stg_table)

def upload_dm_products_dds(connect_to_db, dds_table, restaurant_table, stg_table):
    DDS_upload.upload_dm_products(connect_to_db=connect_to_db, dds_table=dds_table, restaurant_table=restaurant_table, stg_table=stg_table)

def upload_orders_dds(connect_to_db, dds_table, stg_table, time_table, user_table, restaurant_table):
    DDS_upload.upload_orders(connect_to_db=connect_to_db, dds_table=dds_table, stg_table=stg_table, time_table=time_table, user_table=user_table, restaurant_table=restaurant_table)

def upload_couriers_dds(connect_to_db, dds_table, stg_table):
    DDS_upload.upload_couriers(connect_to_db=connect_to_db, dds_table=dds_table, stg_table=stg_table)

def upload_delivery_dds(connect_to_db, dds_table, stg_table, time_table, courier_table, order_table):
    DDS_upload.upload_delivery(connect_to_db=connect_to_db, dds_table=dds_table, stg_table=stg_table,
    time_table=time_table, courier_table=courier_table, order_table=order_table)

def events_load_dds(connect_to_db, dds_table, stg_table, product_table, order_table):
    DDS_upload.events_load(connect_to_db=connect_to_db, dds_table=dds_table,
    stg_table=stg_table, product_table=product_table, order_table=order_table)


#check_database()
#upload_restaurant_dds(connect_to_db=connect_to_db, dds_table='dm_restaurants', stg_table='ordersystem_restaurants')
#upload_timespamp_dss(connect_to_db=connect_to_db, dds_table='dm_timestamps', stg_orders='ordersystem_orders',  stg_delivery='delivery')
#upload_users_dds(connect_to_db=connect_to_db, dds_table='dm_users', stg_table='ordersystem_users')
#upload_dm_products_dds(connect_to_db=connect_to_db, dds_table='dm_products',restaurant_table='dm_restaurants',stg_table='ordersystem_restaurants')
#upload_orders_dds(connect_to_db=connect_to_db, dds_table='dm_orders', stg_table='ordersystem_orders', time_table='dm_timestamps', user_table='dm_users', restaurant_table='dm_restaurants')
upload_couriers_dds(connect_to_db=connect_to_db, dds_table='dm_couriers', stg_table='couriers')
events_load_dds(connect_to_db=connect_to_db, dds_table='fct_product_sales', stg_table='bonussystem_events', product_table='dm_products', order_table='dm_orders')
upload_delivery_dds(connect_to_db=connect_to_db, dds_table='dm_delivery', stg_table='delivery', time_table='dm_timestamps', courier_table='dm_couriers', order_table='dm_orders')






"""
"""