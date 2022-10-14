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
import Stage_upload
import requests

log = logging.getLogger(__name__)



def upload_restaurant(connect_to_db, dds_table, stg_table):
    """Функция заполняет таблицу dds.dm_restaurants данными из stg.ordersystem_restaurants"""
    cursor = connect_to_db.cursor()
    max_date_query = f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}';"""
    cursor.execute(max_date_query)
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
    row_select = f"""SELECT * FROM stg.ordersystem_restaurants WHERE update_ts > '{max_date}';"""
    cursor.execute(row_select)
    data = cursor.fetchall()
    if len(data) == 0:
        log.info(f'В таблице stg.{stg_table} отсутсвуют новые данные')
        return 200
    time_rows = []
    for row in data:
        id = row[1]
        name = row[2][:row[2].find('menu') - 3] + '}'
        time = row[3]
        time_rows.append(time)
        sql_restaunt_dds_query = f"""INSERT INTO dds.{dds_table} (restaurant_id, restaurant_name, active_from, active_to)
        SELECT '{id}', '{name}'::json->>'name', '{time}', '2099-12-31 00:00:00+03:00'
        ON CONFLICT (restaurant_id) DO UPDATE SET restaurant_name = EXCLUDED.restaurant_name, active_from = EXCLUDED.active_from;"""
        cursor.execute(sql_restaunt_dds_query)
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max(time_rows)}','{dds_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Данные загружены в таблицу {dds_table}')
    

def upload_timespamp(connect_to_db, dds_table, stg_orders, stg_delivery):
    """Функция заполняет таблицу dds.dm_timestamps данными из stg.ordersystem_orders"""
    cursor = connect_to_db.cursor()
    max_date_query = f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}_order';"""
    cursor.execute(max_date_query)
    max_date = cursor.fetchone()[0]
    max_delivery_date_query = f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}_delivery';"""
    cursor.execute(max_delivery_date_query)
    max_delivery_date = cursor.fetchone()[0]
    max_date, max_delivery_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f'), datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
    sql_timestamp_query = f"""INSERT INTO dds.{dds_table} (ts, year, month, day, time, date, special_mark)
                            SELECT
                                update_ts,
                                date_part('year', update_ts),
                                date_part('month', update_ts),
                                date_part('day', update_ts),
                                update_ts::time,
                                to_date(to_char(update_ts, 'YYYY/MM/DD'), 'YYYY/MM/DD'),
                                0
                                FROM stg.{stg_orders}
                                WHERE update_ts::time > '{max_date}'
                            UNION ALL
                            SELECT
                                delivery_ts,
                                date_part('year', delivery_ts),
                                date_part('month', delivery_ts),
                                date_part('day', delivery_ts),
                                delivery_ts::time,
                                to_date(to_char(delivery_ts, 'YYYY/MM/DD'), 'YYYY/MM/DD'),
                                1
                            FROM stg.{stg_delivery}
                            WHERE delivery_ts::time > '{max_delivery_date}';    
                                """
    cursor.execute(sql_timestamp_query)
    cursor.execute(f"""SELECT max(ts) FROM dds.{dds_table} WHERE special_mark=0;""")
    max_date = cursor.fetchone()[0]
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max_date}','{dds_table}_order') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    cursor.execute(f"""SELECT max(ts) FROM dds.{dds_table} WHERE special_mark=1;""")
    max_date = cursor.fetchone()[0]
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max_delivery_date}','{dds_table}_delivery') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Сведения о времени заказов обновлены')


def upload_users(connect_to_db, dds_table, stg_table):
    """Функция заполняет таблицу dds.dm_users данными из stg.ordersystem_users"""
    cursor = connect_to_db.cursor()
    users_query = f"""
        INSERT INTO dds.{dds_table} (user_id, user_name, user_login)
        SELECT user_id, user_name, user_login FROM (
        SELECT object_id as user_id,
            object_value::json->>'name' as user_name,
            object_value::json->>'login' as user_login
        FROM stg.{stg_table}) scr
        ON CONFLICT (user_login) DO UPDATE SET user_id = EXCLUDED.user_id, user_name = EXCLUDED.user_name;"""
    cursor.execute(users_query)
    connect_to_db.commit()
    log.info(f'Данные о клиентах записаны в табилцу {dds_table}')


def extract_data(row_value_row):
    """Функция преобразует полученные данные в формат json"""
    result_list = []
    start = row_value_row.find('menu') + 7
    row_value_row = row_value_row[start:]
    delete_symbols = [' ObjectId\(', '"\[', '\]', '\(', '\)']
    for object_n in delete_symbols:
        row_value_row = re.sub(object_n, '', row_value_row)
    clear_rows = row_value_row.split('}, {')
    for i in clear_rows:
        if i.startswith('{'):
            i = i[1:]
        if i.endswith('}"}'):
            i = i[:-3]
        i = '{' + i + '}'
        if len(i) > 5:
            result_list.append(i)
    return result_list


def upload_dm_products(connect_to_db, dds_table, restaurant_table, stg_table):
    """Функция заполняет таблицу dds.dm_products данными из stg.ordersystem_restaurants и
    вспомогательной таблицы stg.restmenu"""
    cursor = connect_to_db.cursor()
    max_date_query = f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}';"""
    cursor.execute(max_date_query)
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
    cursor.execute(f"""SELECT * FROM stg.{stg_table} WHERE update_ts > '{max_date}';""")
    restoraunt_data = cursor.fetchall()
    new_dates = []
    for row in restoraunt_data:
        restaurant_id = row[1]
        update_ts = row[3]
        new_dates.append(update_ts)
        values = extract_data(row[2])
        for record in values:
            product_id = eval(record).get('_id')
            product_name = eval(record).get('name')
            product_price = eval(record).get('price')
            insert_sql =f"""INSERT INTO dds.{dds_table} 
                        (
                            restaurant_id,
                            product_id,
                            product_name,
                            product_price,
                            active_from,
                            active_to
                        )
                        SELECT dds_rest.id, row.product_id, row.product_name, row.product_price::numeric, row.active_from::timestamp, row.active_to::timestamp
                        FROM (
                        SELECT '{restaurant_id}' as restaurant_id, '{product_id}' as product_id,
                                '{product_name}' as product_name, '{product_price}' as product_price,
                                '{update_ts}' as active_from, '2099-12-31 00:00:00+03:00' as active_to) as row
                        INNER JOIN dds.{restaurant_table} dds_rest
                        ON row.restaurant_id = dds_rest.restaurant_id
                        WHERE row.active_from > '{max_date}'
                        ;"""
            cursor.execute(insert_sql)
    if len(new_dates) == 0:
        log.info(f'В таблице stg.{stg_table} отсутствуют новые записи')
        return 200
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max(new_dates)}','{dds_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Сведения о продуктах добавлены в таблицу {dds_table}')


def extract_order_data(row_value_row):
    """Функция распрарсевает данные в строке object_value таблицы stg.ordersystem_orders"""
    start_restaurant = row_value_row.find('restaurant')  # Можем дополнительно достать дату заказа
    start_order_date = row_value_row.find('date')
    start_user = row_value_row.find('user')
    start_order_items = row_value_row.find('order_items')
    start_bonus_payment = row_value_row.find('bonus_payment')
    start_order_status = row_value_row.find('statuses')
    finall_order_status = row_value_row.find('final_status')
    delete_symbols = [' ObjectId\(', '\)']  # '{', '}', ' ', '"\[', '\]', '\(',
    """В этом участке кода, мы создаем данные для колонки restorant_id в формате json"""
    restaurant_id = '{' + row_value_row[start_restaurant - 1:start_order_date - 5] + '}'
    for object_n in delete_symbols:
        restaurant_id = re.sub(object_n, '', restaurant_id).replace('restaurant": "{"id', 'restaurant_id')
    """В этом участке кода, мы создаем данные для колонки order_date в формате json со сведениями о дате создания заказа"""
    order_date = '{' + row_value_row[start_order_date - 1:start_user - 3] + '}'
    """В этом участке кода, мы создаем данные для колонки user_id в формате json со сведениями о немере клиента"""
    user_id = '{' + row_value_row[start_user - 1:start_order_items - 5] + '}'
    for object_n in delete_symbols:
        user_id = re.sub(object_n, '', user_id).replace('user": "{"id', 'user_id')
    """В этом участке кода, мы создаем данные для колонки order_items в формате json для содержимого заказов (номер заказа, продукты, их цены)"""
    order_items = '{' + row_value_row[start_order_items - 2:start_bonus_payment - 3] + '}'
    for object_n in delete_symbols:
        order_items = re.sub(object_n, '', order_items).replace('"[', '[').replace(']"', ']')
    """В этом участке кода, мы создаем данные для колонки bonus_payment в формате json для начисленных бонусов"""
    bonus_payment = '{' + row_value_row[start_bonus_payment - 1:start_order_status - 3] + '}'
    """В этом участке кода, мы создаем данные для колонки order_status в формате json для учета статусов заказов и времени их обновления"""
    order_status = '{' + row_value_row[start_order_status - 1:finall_order_status - 3] + '}'
    for object_n in delete_symbols:
        order_status = re.sub(object_n, '', order_status).replace('"[', '[').replace(']"', ']')
    """В этом участке кода, мы создаем данные для колонки final_status в формате json для хранения финального статуса заказа"""
    final_status = '{' + row_value_row[finall_order_status - 1:-1] + '}'
    return restaurant_id, order_date, user_id, order_items, bonus_payment, order_status, final_status


def upload_orders(connect_to_db, dds_table, stg_table, time_table, user_table, restaurant_table):
    """Функция заполняет таблицу dds.dm_orders"""
    cursor = connect_to_db.cursor()
    max_date_query = f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}';"""
    cursor.execute(max_date_query)
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
    cursor.execute(f"""SELECT object_id, object_value, update_ts FROM stg.{stg_table};""")
    data = cursor.fetchall()
    new_dates = []
    for row in data:
        order_id = row[0]
        # time = row[2]
        restaurant_id, order_date, user_id, order_items, bonus_payment, order_status, final_status = extract_order_data(str(row[1]))
        new_dates.append(eval(order_date).get('date'))
        orders_sql = f"""INSERT INTO dds.{dds_table} (user_id, restaurant_id, timestamp_id, order_key, order_status)
                            SELECT du.id as user_id, dr.id as restaurant_id, dt.id as timestamp_id, order_info.order_id as order_key, order_info.order_status
                            FROM (
                                SELECT '{order_id}' as order_id, '{eval(user_id).get('user_id')}' as user_id, '{eval(restaurant_id).get('restaurant_id')}' as restaurant_id,
                                '{eval(order_date).get('date')}' as order_date, '{eval(order_status).get('order_status')}' as order_status   
                            ) as order_info
                            INNER JOIN dds.{time_table} dt
                            ON Date_trunc('second', dt.ts) = Date_trunc('second', order_info.order_date::timestamptz) AND dt.special_mark = 0
                            INNER JOIN dds.{user_table} du 
                            ON du.user_id  = order_info.user_id
                            INNER JOIN dds.{restaurant_table} dr
                            ON dr.restaurant_id = order_info.restaurant_id
                            WHERE order_info.order_date > '{max_date}';"""
        cursor.execute(orders_sql)
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max(new_dates)}','{dds_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Данные загружены в таблицу {dds_table}')


def events_load(connect_to_db, dds_table, stg_table, product_table, order_table):
    """Функция создает и заполняет вспомогательную таблицу с даннми о заказах в формате json"""
    cursor = connect_to_db.cursor()
    cursor.execute(f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}';""")
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
    cursor.execute(f"""
                    SELECT
                        id,
                        event_ts,
                        event_value::json->>'user_id' as user_id,
                        event_value::json->>'order_id' as order_id, 
                        event_value::json->>'order_date' as order_date,
                        event_value::json->>'product_payments' as product_payments
                    FROM stg.{stg_table}
                    WHERE event_type = 'bonus_transaction' AND event_value::json->>'order_date' > '{max_date}'
                    ORDER BY id ASC;""")
    data = cursor.fetchall()
    orders_dates = []
    for row in data: 
        order_id = row[3]
        order_date = row[4]
        orders_dates.append(order_date)
        product_payments = row[5].replace('[','').replace(']','')
        products_in_event = product_payments.split('}, {')
        for record in products_in_event:
            record = '{' + record.replace('{','').replace('}','') + '}'
            #print(record)
            product_id = eval(record).get('product_id')
            product_name = eval(record).get('product_name')
            price = eval(record).get('price')
            quantity = eval(record).get('quantity')
            product_cost = eval(record).get('product_cost')
            bonus_payment = eval(record).get('bonus_payment')
            bonus_grant = eval(record).get('bonus_grant')
            insert_sql = f"""INSERT INTO dds.{dds_table}
                    (order_id, product_id, count, price, total_sum, bonus_payment, bonus_grant)
                    SELECT
                        do1.id as order_id,
                        dp.id as product_id,
                        row.count::integer,
                        row.price::numeric(14,6),
                        row.total_sum::numeric(14,6),
                        row.bonus_payment::numeric(14,6),
                        row.bonus_grant::numeric(14,6) 
                    FROM (
                        SELECT '{order_id}' as order_id, '{product_id}' as product_id, '{quantity}' as count, '{price}' as price,
                        '{quantity*price}' as total_sum, '{bonus_payment}' as bonus_payment, '{bonus_grant}' as bonus_grant, '{datetime.strptime(order_date, '%Y-%m-%d %H:%M:%S')}' as order_date
                    ) as row
                    INNER JOIN dds.{product_table} dp
                    ON row.product_id = dp.product_id
                    INNER JOIN dds.{order_table} do1
                    ON row.order_id = do1.order_key
                    WHERE row.order_date > '{max_date}'
                    ORDER BY order_id, product_id;
                    """
            cursor.execute(insert_sql)
    if len(orders_dates) == 0:
        log.info(f'Новые записи для включения в таблицу {dds_table} отсутствуют')
        return 200
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max(orders_dates)}','{dds_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Данные загружены в таблицу {dds_table}')


def upload_couriers(connect_to_db, dds_table, stg_table):
    """Функция заполняет таблицу dds.dm_couriers данными из stg.couriers"""
    cursor = connect_to_db.cursor()
    users_query = f"""
        INSERT INTO dds.{dds_table} (courier_id, courier_name)
        SELECT courier_id, courier_name
        FROM stg.{stg_table}
        ON CONFLICT (courier_id) DO NOTHING;"""
    cursor.execute(users_query)
    connect_to_db.commit()
    log.info(f'Данные о курьерах записаны в табилцу {dds_table}')


def upload_delivery(connect_to_db, dds_table, stg_table, time_table, courier_table, order_table):
    """Функция заполняет таблицу dds.dm_delivery"""
    cursor = connect_to_db.cursor()
    max_date_query = f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00.0') FROM stg.srv_etl_settings WHERE workflow_settings = '{dds_table}';"""
    cursor.execute(max_date_query)
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S.%f')
    cursor.execute(f"""SELECT order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum FROM stg.{stg_table};""")
    data = cursor.fetchall()
    new_dates = []
    for row in data:
        order_id = row[0]
        order_ts = row[1]
        delivery_id = row[2]
        courier_id = row[3]
        address = row[4]
        delivery_ts = row[5]
        rate=row[6]
        delivery_sum = row[7]
        tip_sum = row[8]
        new_dates.append(delivery_ts)
        delivery_insert = f"""INSERT INTO dds.{dds_table} (order_id, delivery_id, courier_id, address, delivery_ts_id, rate, delivery_sum, tip_sum)
                            SELECT do1.id as order_id, delivery_info.delivery_id as delivery_id, dc.id as courier_id,
                            delivery_info.address as address, dt.id as delivery_ts_id, delivery_info.rate::integer as rate,
                            delivery_info.delivery_sum::numeric(14,6) as delivery_sum, delivery_info.tip_sum::numeric(14,6) as tip_sum
                            FROM (
                                SELECT '{order_id}' as order_id, '{order_ts}' as order_ts, '{delivery_id}' as delivery_id, '{courier_id}' as courier_id,
                                        '{address}' as address, '{delivery_ts}' as delivery_ts, '{rate}' as rate, '{delivery_sum}' as delivery_sum, '{tip_sum}' as tip_sum
                            ) as delivery_info
                            INNER JOIN dds.{time_table} dt
                            ON Date_trunc('second', dt.ts) = Date_trunc('second', delivery_info.delivery_ts::timestamptz) AND dt.special_mark = 1
                            INNER JOIN dds.{courier_table} dc
                            ON dc.courier_id = delivery_info.courier_id
                            INNER JOIN dds.{order_table} do1
                            ON do1.order_key = delivery_info.order_id
                            WHERE delivery_info.delivery_ts > '{max_date}';"""
        cursor.execute(delivery_insert)
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES ('{max(new_dates)}','{dds_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Данные загружены в таблицу {dds_table}')




