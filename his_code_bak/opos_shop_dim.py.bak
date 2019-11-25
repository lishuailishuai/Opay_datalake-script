# coding=utf-8

import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.utils.email import send_email
from airflow.models import Variable
from utils.connection_helper import get_hive_cursor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
import codecs
import csv
import logging

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 10, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'opos_shop_dim',
    schedule_interval="*/5 * * * *",
    default_args=args)

insert_shop_dim_data = BashOperator(
    task_id='insert_shop_dim_data',
    bash_command="""
        mysql -udml_insert -p6VaEyu -h10.52.149.112 opos_dw  -e "

            insert into opos_dw.opos_agent_shop_dim (agent_id,opay_id,shop_id,bd_id,city_id)
            select
            a.id,
            a.agent_id,
            s.id,
            s.bd_id,
            s.city_id
            from
            opos_dw.agents a
            join
            opos_dw.bd_shop s on a.agent_id = concat(s.opay_id,'')
            ON DUPLICATE KEY
            UPDATE
            opay_id=VALUES(opay_id),
            shop_id=VALUES(shop_id),
            bd_id=VALUES(bd_id),
            city_id=VALUES(city_id)

            ;

        "
    """,
    dag=dag,
)
