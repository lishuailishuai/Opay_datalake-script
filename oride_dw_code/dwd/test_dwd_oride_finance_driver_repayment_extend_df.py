# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
 
args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 9, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'test_dwd_oride_finance_driver_repayment_extend_df', 
    schedule_interval="30 02 * * *", 
    default_args=args,
    catchup=False) 


##----------------------------------------- 变量 ---------------------------------------## 

table_name="test_dwd_oride_finance_driver_repayment_extend_df"
hdfs_path="hdfs://warehourse/user/hive/warehouse/test_db.db/"+table_name




##----------------------------------------- 脚本 ---------------------------------------## 

test_dwd_oride_finance_driver_repayment_extend_df_task = HiveOperator(

    task_id='test_dwd_oride_finance_driver_repayment_extend_df_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

SELECT dri.city_id,
       city_name,
       --城市名称

       dri.driver_id,
       --司机ID(非专车有手机的骑手)

       dri.driver_name,
       --司机名称

       dri.phone_number,
       --手机号

       dri.product_id,
       --司机类型

       (CASE
            WHEN dri.product_id=1 THEN ogreen.total_ * ogreen.amount_service
            ELSE ostreet.total_ * ostreet.amount_service
        END) AS repayment_all,
       --还款总额

       (CASE
            WHEN dri.product_id=1 THEN ogreen.total_
            ELSE ostreet.total_
        END) AS numbers,
       --还款次数(分期总数)

       (CASE
            WHEN dri.product_id=1 THEN ogreen.first_
            ELSE ostreet.first_
        END) AS start_date,
       --开始日期

       (CASE
            WHEN dri.product_id=1 THEN ogreen.last_
            ELSE ostreet.last_
        END) AS last_date,
       --最后还款

       (CASE
            WHEN dri.product_id=1 THEN ogreen.amount_service
            ELSE ostreet.amount_service
        END) AS repayment_amount,
       --还款金额

       (CASE
            WHEN bal.balance IS NULL THEN 0
            ELSE bal.balance
        END) AS balance,
       --余额

       (CASE
            WHEN dri.product_id=1 THEN nvl(ogreen.settled_,0)
            ELSE nvl(ostreet.settled_,0)
        END) AS settled_numbers,
       --还款期数

       nvl(oer.overdue_payment_cnt,0) AS overdue_payment_cnt,
       --违约期数

       nvl((CASE
            WHEN ((dri.product_id=1
                   AND ogreen.settled_=ogreen.total_)
                  OR (dri.product_id=2
                      AND ostreet.settled_=ostreet.total_)) THEN 0
            ELSE 1
        END),0) AS status,
       --0还完 1未还完

       dri.fault,
       --骑手状态

       dri.country_code,
       --国家码字段

       '{pt}' AS dt
FROM  

  (SELECT *
   FROM oride_dw.dim_oride_driver_base
   WHERE dt='{pt}'
     AND city_id<>'999001') dri
LEFT OUTER JOIN --所有骑手的余额表

  (SELECT driver_id,
          balance--余额

   FROM test_db.test_ods_sqoop_base_data_driver_balance_extend_df
   WHERE dt='{pt}') bal ON dri.driver_id=bal.driver_id
LEFT OUTER JOIN --专车司机扣款记录

  (SELECT driver_id,
          from_unixtime(min(DAY)) AS first_,
          from_unixtime(max(DAY)) AS last_,
          count(1) AS settled_,
          365 AS total_ ,
          max(amount_service) AS amount_service
   FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
   WHERE amount_service>0
     AND dt='{pt}'
   GROUP BY driver_id) ogreen ON dri.driver_id = ogreen.driver_id
LEFT OUTER JOIN --快车司机扣款记录
  (SELECT ta.driver_id,
          from_unixtime(min(ta.created_at)) AS first_,
          from_unixtime(max(ta.created_at)) AS last_,
          count(1) AS settled_,
          min(tb.numbers) AS total_ ,
          abs(min(ta.amount)) AS amount_service
   FROM (select *
         FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df
         WHERE dt = '{pt}'
           AND from_unixtime(updated_at,'yyyy-MM-dd HH')<='{now_day} 00'
           AND amount_reason=6
           AND amount<>0) ta
   LEFT OUTER JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_driver_repayment_df
      WHERE dt='{pt}'
        AND substring(updated_at,1,13)<='{now_day} 00'
        AND repayment_type=0) tb ON ta.driver_id = tb.driver_id
   GROUP BY ta.driver_id ) ostreet ON dri.driver_id = ostreet.driver_id
LEFT OUTER JOIN --所有骑手的违约期数

  (SELECT driver_id,
          IF(INSTR(concat_ws('',collect_list(false_id)),'0')=0, LENGTH(concat_ws('',collect_list(false_id))), INSTR(concat_ws('',collect_list(false_id)),'0')-1) AS overdue_payment_cnt --违约期数

   FROM
     (SELECT driver_id,
             dt,
             (CASE WHEN balance<0 THEN '1' ELSE '0' END) AS false_id
      FROM oride_dw_ods.ods_sqoop_base_data_driver_balance_extend_df
      WHERE dt BETWEEN DATE_ADD('{pt}', -15) AND DATE_ADD('{pt}',0)
      ORDER BY driver_id,
               dt DESC) AS slack_det
   GROUP BY driver_id) oer ON dri.driver_id=oer.driver_id

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        prev_6_day='{{macros.ds_add(ds, -6)}}',
        table=table_name
        ),
schema='oride_dw',
    dag=dag)


test_dwd_oride_finance_driver_repayment_extend_df_task