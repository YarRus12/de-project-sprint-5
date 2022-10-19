from urllib.parse import quote_plus as quote
from airflow import DAG
import psycopg2
from airflow.operators.python import PythonOperator
from airflow.models.xcom import XCom
from datetime import datetime
import logging
import Check
import DDS_upload



ALL_DDS_TABLES = ['dm_users', 'dm_restaurants', 'dm_products', 'dm_timestamps', 'dm_orders', 'fct_product_sales', 'dm_couriers', 'dm_delivery']


log = logging.getLogger(__name__)

connect_to_db = psycopg2.connect("host=localhost port=5432 dbname=de user=jovyan password=jovyan")


def check_database(connect_to_db=connect_to_db):
    Check.check_and_create(connect_to_db,'dds', ALL_DDS_TABLES)

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


dag = DAG(
    schedule_interval='* 1 * * *',
    dag_id='download_data_to_dds',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['dds', 'couriers', 'products'],
    is_paused_upon_creation=False
)

check = PythonOperator(task_id='check_database',
                                 python_callable=check_database,
                                 dag=dag)
restaurant_dds = PythonOperator(task_id='restaurant_dds',
                                 python_callable=upload_restaurant_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_restaurants', 'stg_table': 'ordersystem_restaurants'},
                                 dag=dag)
timespamp_dss = PythonOperator(task_id='timespamp_dss',
                                 python_callable=upload_timespamp_dss,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_timestamps', 'stg_orders': 'ordersystem_orders', 'stg_delivery':'delivery'},
                                 dag=dag)
users_dds = PythonOperator(task_id='users_dds',
                                 python_callable=upload_users_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_users', 'stg_table': 'ordersystem_users'},
                                 dag=dag)
products_dds = PythonOperator(task_id='products_dds',
                                 python_callable=upload_dm_products_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_products', 'restaurant_table':'dm_restaurants', 'stg_table': 'ordersystem_restaurants'},
                                 dag=dag)
orders_dds = PythonOperator(task_id='orders_dds',
                                 python_callable=upload_orders_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_orders', 'stg_table':'ordersystem_orders', 'time_table':'dm_timestamps', 'user_table':'dm_users', 'restaurant_table':'dm_restaurants'},
                                 dag=dag)
couriers_dds = PythonOperator(task_id='couriers_dds',
                                 python_callable=upload_couriers_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_couriers', 'stg_table': 'couriers'},
                                 dag=dag)
facts_dds = PythonOperator(task_id='load_dds',
                                 python_callable=events_load_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'fct_product_sales', 'stg_table': 'bonussystem_events', 'product_table':'dm_products', 'order_table':'dm_orders'},
                                 dag=dag)
delivery_dds = PythonOperator(task_id='delivery_dds',
                                 python_callable=upload_delivery_dds,
                                 op_kwargs={'connect_to_db': connect_to_db, 'dds_table': 'dm_delivery', 'stg_table':'delivery', 'time_table':'dm_timestamps', 'courier_table':'dm_couriers', 'order_table':'dm_orders'},
                                 dag=dag)


check >> restaurant_dds >> timespamp_dss >> users_dds >> couriers_dds >> delivery_dds >> products_dds>> orders_dds >> facts_dds