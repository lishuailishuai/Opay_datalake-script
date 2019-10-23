# -*- coding: utf-8 -*-
"""
平台司机数据2019-08-31
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.connection_helper import get_hive_cursor, get_db_conn
from datetime import datetime, timedelta
from airflow.sensors import UFileSensor
from airflow.operators.impala_plugin import ImpalaOperator
import re,sys
import logging
from utils.validate_metrics_utils import *
import time

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 8, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_finance_payment_guarantors_d',
    schedule_interval="40 02 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 60',
    dag=dag
)

hive_db = 'oride_dw'
hive_table = 'dwd_oride_finance_driver_repayment_extend_df'

"""
##----------------------------------依赖数据源------------------------------##
"""

#依赖前一天数据是否存在
dwd_oride_finance_driver_repayment_extend_df_tesk = UFileSensor(
    task_id='dwd_oride_finance_driver_repayment_extend_df',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_finance_driver_repayment_extend_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天数据是否存在
dwm_oride_driver_base_di_tesk = UFileSensor(
    task_id='dwm_oride_driver_base_di_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


"""
##------------------------------------ end --------------------------------##
"""

mysql_table = 'oride_dw.app_oride_finance_payment_guarantors_d'


##------------------------------------ SQL --------------------------------##

# 从hive读取数据
def get_data_from_hive(ds,**op_kwargs):
    # ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    hql = '''
        SELECT ovlist.driver_id, --司机id
               signups.id,
               signups.name,
               signups.mobile,
               signups.driver_type,
               signups.city,
               signups.guarantors_mobile,
               ovlist.overdue_payment_cnt
        FROM (--取出所有7天以上的逾期司机的详细逾期天数
        
              SELECT driver_id,
                     IF(INSTR(concat_ws('',collect_list(false_id)),'0')=0, LENGTH(concat_ws('',collect_list(false_id))), INSTR(concat_ws('',collect_list(false_id)),'0')-1) AS      overdue_payment_cnt --违约期数
        
              FROM (--取出所有逾期时间超过7天的司机，重算真实的逾期天数
        
                    SELECT driver_id,
                           dt,
                           (CASE WHEN balance<0 THEN '1' ELSE '0' END) AS false_id
                    FROM oride_dw_ods.ods_sqoop_base_data_driver_balance_extend_df
                    WHERE driver_id IN
                        (SELECT driver_id
                         FROM oride_dw.dwd_oride_finance_driver_repayment_extend_df
                         WHERE dt='{pt}'
                           AND overdue_payment_cnt>7)
                    ORDER BY driver_id,
                             dt DESC) AS slack_det_b
              GROUP BY driver_id ) AS ovlist
        LEFT OUTER JOIN
          (SELECT old_signup.id,
                  old_signup.driver_id,
                  old_signup.name,
                  old_signup.mobile,
                  old_signup.driver_type,
                  old_signup.city,
                  IF(guarantors.mobile IS NULL, '', guarantors.mobile) AS guarantors_mobile
           FROM (select * from oride_dw_ods.ods_sqoop_mass_rider_signups_df WHERE dt='{pt}'
             AND driver_id<>0) AS old_signup
           LEFT JOIN
             (SELECT rider_id,
                     concat_ws('    ',collect_list(mobile)) AS mobile
              FROM oride_dw_ods.ods_sqoop_mass_rider_signups_guarantors_df
              WHERE dt='{pt}'
              GROUP BY rider_id) AS guarantors ON old_signup.id = guarantors.rider_id
           ) AS signups ON ovlist.driver_id = signups.driver_id
        ORDER BY signups.city ASC,
         ovlist.overdue_payment_cnt DESC;
    '''.format(
        hive_db=hive_db,
        hive_table=hive_table,
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        prev_3_day=airflow.macros.ds_add(ds, -2),
    )

    logging.info(hql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(hql)
    hive_data = hive_cursor.fetchall()

    mysql_conn = get_db_conn('mysql_bi')
    mcursor = mysql_conn.cursor()

    __data_to_mysql(
        mcursor,
        hive_data,
        [
            'day', 'city_id', 'city_name', 'driver_id', 'driver_name', 'driver_mobile', 'driver_type',
            'balance', 'repayment_total_amount', 'start_date', 'repayment_amount', 'total_numbers',
            'effective_days', 'lose_numbers', 'last_back_time', 'today_repayment', 'status','order_numbers','order_agv','fault'
        ],
        'day=VALUES(day)'
    )

    hive_cursor.close()
    mcursor.close()


# 数据集写入mysql
def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into {table} ({columns})'.format(
        table=mysql_table,
        columns=','.join(column)
    )
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for (day, city_id, city_name, driver_id, driver_name, driver_mobile, driver_type,
                balance, repayment_total_amount, start_date, repayment_amount, total_numbers,
                effective_days, lose_numbers, last_back_time, today_repayment, status,order_numbers,order_agv,fault) in data:

            row = [
                day, city_id, city_name, driver_id, driver_name.replace("'", "\\'"), driver_mobile, driver_type,
                balance, repayment_total_amount, start_date, repayment_amount, total_numbers,
                effective_days, lose_numbers, last_back_time, today_repayment, status,order_numbers,order_agv,fault
            ]
            if sval == '':
                sval = '(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            else:
                sval += ',(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            cnt += 1
            if cnt >= 1000:
                #logging.info(esql.format(isql, sval, update))
                conn.execute(esql.format(isql, sval, update))
                cnt = 0
                sval = ''

        if cnt > 0 and sval != '':
            #logging.info(esql.format(isql, sval, update))
            conn.execute(esql.format(isql, sval, update))
    except BaseException as e:
        logging.info(e)
        sys.exit(1)
        return


get_data_from_hive_task = PythonOperator(
    task_id='get_data_from_hive_task',
    python_callable=get_data_from_hive,
    provide_context=True,
    dag=dag
)


dwd_oride_finance_driver_repayment_extend_df_tesk>>dwm_oride_driver_base_di_tesk>>sleep_time >> get_data_from_hive_task
