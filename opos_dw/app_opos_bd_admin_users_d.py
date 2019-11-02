# -*- coding: utf-8 -*-
"""
拉取opos bd_admin_users
"""
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from utils.connection_helper import get_db_conn
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.operators.bash_operator import BashOperator
from plugins.comwx import ComwxApi
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 11, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opos_bd_admin_users_d',
    schedule_interval="30 04 * * *",
    max_active_runs=1,
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 300',
    dag=dag
)

# 依赖
dependence_table_opos_metrcis_report_task = HivePartitionSensor(
    task_id="dependence_table_opos_metrcis_report_task",
    table="opos_metrcis_report",
    partition="dt='{{ ds }}'",
    schema="opos_temp",
    poke_interval=120,
    dag=dag
)

dependence_table_opos_active_user_daily_task = HivePartitionSensor(
    task_id="dependence_table_opos_active_user_daily_task",
    table="opos_active_user_daily",
    partition="dt='{{ ds }}'",
    schema="opos_temp",
    poke_interval=120,
    dag=dag
)


# sync_db = Variable.get("app_opos_bd_admin_users").split("\n")
# {"odb":"opay_crm", "otable":"bd_admin_users", "oconn":"opos_opay_crm", "ddb":""}


def sync_table_data_to_bi(**op_kwargs):
    odb = op_kwargs.get('odb', '')
    otable = op_kwargs.get('otable', '')
    oconn = op_kwargs.get('oconn', '')
    ddb = op_kwargs.get('ddb', '')
    ds = op_kwargs.get('ds')

    if odb == "" or otable == "" or oconn == "" or ddb == "":
        return

    dconn = get_db_conn('mysql_bi_utf8mb4')
    dcursor = dconn.cursor()
    isql = '''replace into {db}.{table} 
        (id, username, password, name, phone, avatar, opay_email, leader_id, opay_user, job_id, department_id, 
        remember_token, create_user, created_at, updated_at, is_old_admin) values 
    '''.format(db=ddb, table=otable)

    oconn = get_db_conn(oconn)
    ocursor = oconn.cursor()

    logging.info(ds)
    if ds == '2019-11-01':
        where = ""
    else:
        where = '''WHERE 
            (created_at>='{ds} 00:00:00' AND created_at<='{ds} 23:59:59') OR 
            (updated_at>='{ds} 00:00:00' AND updated_at<='{ds} 23:59:59')
        '''.format(ds=ds)

    try:
        osql = '''
            SELECT 
                id, username, password, name, phone, avatar, opay_email, leader_id, opay_user, job_id, department_id, 
                remember_token, create_user, created_at, updated_at, is_old_admin 
            FROM {db}.{table} 
            {where} 
        '''.format(db=odb, table=otable, where=where)
        logging.info(osql)
        ocursor.execute(osql)
        row = []
        res = ocursor.fetchall()
        for record in res:
        # while True:
        #    try:
        #        record = ocursor.next()
        #    except BaseException as e:
        #        logging.info(e)
        #        record = None
            # logging.info(record)
            if not record:
                break
            row.append("('{}')".format("','".join([str(x).replace("'", "\\'") for x in record])))
            if len(row) >= 2000:
                logging.info(len(row))
                dcursor.execute("{h} {v}".format(h=isql, v=",".join(row)))
                row = []

        if len(row) > 0:
            logging.info("last: {}".format(len(row)))
            dcursor.execute("{h} {v}".format(h=isql, v=",".join(row)))
    except BaseException as e:
        logging.info(e)
        wxapi = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')
        wxapi.postAppMessage(
            '重要重要重要：{}.{}数据写入mysql异常【{}】'.format(odb, otable, ds),
            '271'
        )


sync_table_schema = PythonOperator(
    task_id='sync_table_schema_{}_{}'.format('opay_crm', 'bd_admin_users'),
    python_callable=sync_table_data_to_bi,
    provide_context=True,
    op_kwargs={
        "odb": 'opay_crm',
        "otable": 'bd_admin_users',
        "oconn": 'opos_opay_crm',
        "ddb": 'opos_dw',
        "ds": '{{ ds }}'
    },
    dag=dag
)


dependence_table_opos_metrcis_report_task >> sleep_time
dependence_table_opos_active_user_daily_task >> sleep_time
sleep_time >> sync_table_schema
