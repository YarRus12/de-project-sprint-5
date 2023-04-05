from airflow import DAG
import psycopg2
from airflow.operators.python import PythonOperator
import logging
from src.scripts import Check
from datetime import datetime


ALL_CDM_TABLES = ['dm_settlement_report', 'dm_courier_ledger']
log = logging.getLogger(__name__)
connect_to_db = psycopg2.connect("host=localhost port=5432 dbname=de user=jovyan password=jovyan")


def check_database(connect_to_db=connect_to_db):
    Check.check_and_create(connect_to_db, 'cdm', ALL_CDM_TABLES)

def courier_ledger(connect_to_db, cdm_table, delivery_table, timestamps_table, couriers_table):
    cursor = connect_to_db.cursor()
    cursor.execute(f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00') FROM stg.srv_etl_settings WHERE workflow_settings = '{cdm_table}';""")
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
    insert_sql = f"""INSERT INTO cdm.{cdm_table}
                    (courier_id, courier_name, settlement_year,
                    settlement_month, orders_count, orders_total_sum,
                    rate_avg, order_processing_fee, courier_order_sum,
                    courier_tips_sum, courier_reward_sum)
                    With cte AS (
                    SELECT
                    dd.courier_id,
                    dd.delivery_ts_id,
                    dd.rate,
                    dd.order_sum,
                    dt.year,
                    dt.month,
                    case
                    WHEN dd.rate < 4 and dd.order_sum * 0.05 > 100
                    then dd.order_sum * 0.05
                    WHEN dd.rate < 4 and dd.order_sum * 0.05 < 100
                    then 100
                    WHEN rate < 4.5 and dd.order_sum * 0.07 > 150
                    then dd.order_sum * 0.07
                    WHEN dd.rate < 4.5 and dd.order_sum * 0.07 < 150
                    then 150
                    WHEN dd.rate < 4.9 and dd.order_sum * 0.08 > 175
                    then dd.order_sum * 0.08
                    WHEN dd.rate < 4.9 and dd.order_sum * 0.08 < 175
                    then 175
                    WHEN dd.rate >= 4.9 and dd.order_sum * 0.1 > 200
                    then dd.order_sum * 0.1
                    else 200
                    END as reward,
                    tip_sum
                    FROM dds.{delivery_table} dd
                    INNER JOIN dds.{timestamps_table} dt
                    ON dd.delivery_ts_id = dt.id
                    where dt.month=(select date_part('month', (now()-interval '1 month')))
                    and dt.ts > '{max_date}'
                    )
                    SELECT
                    dc.id as courier_id,
                    max(dc.courier_name) as courier_name,
                    max(cte.year) as year,
                    max(cte.month) as month,
                    count(1) as orders_count,
                    sum(cte.order_sum) as orders_total_sum,
                    avg(cte.rate) as rate_avg,
                    sum(cte.order_sum) * 0.25 as order_processing_fee,
                    sum(cte.reward) as courier_order_sum,
                    sum(tip_sum) as courier_tips_sum,
                    (sum(cte.reward) + sum(tip_sum)) * 0.95 as courier_reward_sum
                    from cte
                    inner join dds.{couriers_table} dc
                    on cte.courier_id = dc.id
                    group by dc.id
                    ;"""
    cursor.execute(insert_sql)
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES (date_trunc('second', now()::timestamp),'{cdm_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Данные загружены в таблицу {cdm_table}')


def settlement_report(connect_to_db, cdm_table, restaurants_table, orders_table, timestamps_table, fct_table):
    cursor = connect_to_db.cursor()
    cursor.execute(f"""SELECT coalesce(max(workflow_key),'2022-01-01 00:00:00') FROM stg.srv_etl_settings WHERE workflow_settings = '{cdm_table}';""")
    max_date = cursor.fetchone()[0]
    max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
    cursor = connect_to_db.cursor()
    insert_sql = f"""INSERT INTO cdm.{cdm_table}
                    (restaurant_id, restaurant_name, settlement_date,
                    orders_count, orders_total_sum, orders_bonus_payment_sum,
                    orders_bonus_granted_sum, order_processing_fee,
                    restaurant_reward_sum)
                    select
                        dr.id as restaurant_id,
                        max(dr.restaurant_name) as restaurant_name,
                        now() as settlement_date,
                        count(do1.id) as orders_count,
                        sum(fct.total_sum) as orders_total_sum,
                        sum(fct.bonus_payment) as orders_bonus_payment_sum,
                        sum(fct.bonus_grant)orders_bonus_granted_sum,
                        sum(fct.total_sum)*0.25 as order_processing_fee,
                        sum(fct.total_sum)-sum(fct.bonus_payment)-sum(fct.total_sum)*0.25 as restaurant_reward_sum
                    from dds.{restaurants_table} dr
                    inner join dds.{orders_table} do1
                    on do1.restaurant_id = dr.id
                    inner join dds.{timestamps_table} dt
                    on do1.timestamp_id = dt.id
                    inner join dds.{fct_table} fct
                    on do1.id = fct.order_id
                    where do1.order_status = 'CLOSED'
                    and dt.ts > '{max_date}'
                    group by dr.id;"""
    cursor.execute(insert_sql)
    cursor.execute(f"""INSERT INTO stg.srv_etl_settings as tbl (workflow_key, workflow_settings)
    VALUES (date_trunc('second', now()::timestamp),'{cdm_table}') ON CONFLICT (workflow_settings) DO UPDATE SET workflow_key = EXCLUDED.workflow_key;""")
    connect_to_db.commit()
    log.info(f'Данные загружены в таблицу {cdm_table}')


dag = DAG(
    schedule_interval='30 8 10 * *',
    dag_id='download_cdm',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['cdm', 'mongo', 'postgresql'],
    is_paused_upon_creation=False
)

check = PythonOperator(task_id='check_database',
                                 python_callable=check_database,
                                 dag=dag)
courier_ledger = PythonOperator(task_id='courier_ledger',
                                 python_callable=courier_ledger,
                                 op_kwargs={'connect_to_db': connect_to_db, 'cdm_table': 'dm_courier_ledger', 'delivery_table':'dm_delivery', 'timestamps_table':'dm_timestamps', 'couriers_table':'dm_couriers'},
                                 dag=dag)
settlement_report = PythonOperator(task_id='settlement_report',
                                 python_callable=settlement_report,
                                 op_kwargs={'connect_to_db': connect_to_db, 'cdm_table':'dm_settlement_report', 'restaurants_table':'dm_restaurants', 'orders_table':'dm_orders', 'timestamps_table':'dm_timestamps', 'fct_table':'fct_product_sales'},
                                 dag=dag)
check >> [courier_ledger, settlement_report]