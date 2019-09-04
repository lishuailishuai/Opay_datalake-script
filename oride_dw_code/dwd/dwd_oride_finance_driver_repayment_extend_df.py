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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_oride_finance_driver_repayment_extend_df', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
ods_sqoop_base_data_driver_repayment_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_data_driver_repayment_df_prev_day_tesk",
      table="ods_sqoop_base_data_driver_df",
      partition="dt='{{ds}}'",
      schema="oride_dw",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )


#依赖前一天分区
ods_sqoop_base_data_driver_balance_extend_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_data_driver_balance_extend_df_prev_day_tesk",
      table="ods_sqoop_base_data_driver_extend_df",
      partition="dt='{{ds}}'",
      schema="oride_dw",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态 
      dag=dag
    )

# 依赖前一天分区
dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwd_oride_finance_driver_repayment_extend_df"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dwd_oride_finance_driver_repayment_extend_df_task = HiveOperator(

    task_id='dwd_oride_finance_driver_repayment_extend_df_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

SELECT city_id,
       city_name,
       --城市名称

       dri.driver_id,
       --司机ID(非专车有手机的骑手)

       dri.driver_name,
       --司机名称

       dri.phone_number,
       --手机号

       product_id,
       --司机类型

       red.amount_service,

       (case when dri.product_id=1 then red.amount_service*365 else rep.amount*rep.numbers end) AS repayment_all,
       --还款总额

       (case when dri.product_id=1 then null else rep.start_date end) as start_date,
       --开始日期

       (case when dri.product_id=1 then red.amount_service else rep.amount end) as repayment_amount,
       --还款金额

       (case when dri.product_id=1 then 365 else rep.numbers end) as numbers,
       --还款次数(分期总数)

       (case when bal.balance is null then 0 else bal.balance end) as balance,
       --余额

       nvl(oer.overdue_payment_cnt,0) as overdue_payment_cnt,
       --违约期数

       date_sub(dri.dt,nvl(oer.overdue_payment_cnt,0)) AS last_repayment_time,
       --最后还款时间

       nvl((CASE WHEN dri.product_id=1
           AND bal.balance>=0
           AND red.amount_service>0 THEN 1 
           WHEN dri.product_id<>1
            then rec.cnt ELSE 0 END),0) AS is_td_valid,
       --当天是否有效
       dri.country_code,
           --国家码字段
    
       '{pt}' AS dt
       
FROM

(SELECT *
   FROM oride_dw.dim_oride_driver_base
   WHERE dt='{pt}'
   and city_id<>'999001') dri
LEFT OUTER JOIN
  (SELECT *
   FROM oride_dw.ods_sqoop_base_data_driver_repayment_df
   WHERE dt='{pt}'
     AND substring(updated_at,1,13)<='{now_day} 00'
     and repayment_type=0) rep
  ON dri.driver_id=rep.driver_id
LEFT OUTER JOIN
  (SELECT driver_id,
          balance--余额
   FROM oride_dw.ods_sqoop_base_data_driver_balance_extend_df
   WHERE dt='{pt}') bal ON dri.driver_id=bal.driver_id
LEFT OUTER JOIN
 (SELECT driver_id,
          amount_service
   FROM oride_dw.ods_sqoop_base_data_driver_records_day_df
   WHERE from_unixtime(updated_at,'yyyy-MM-dd HH')<='{now_day} 00'
     AND dt = '{pt}'
     AND from_unixtime(DAY, 'yyyy-MM-dd') = dt) red
 ON dri.driver_id=red.driver_id
 LEFT OUTER JOIN
  (SELECT driver_id,
          count(distinct(case when amount<0 then driver_id else null end)) as cnt
   FROM oride_dw.ods_sqoop_base_data_driver_recharge_records_df
   WHERE from_unixtime(updated_at,'yyyy-MM-dd HH')<='{now_day} 00'
     AND dt = '{pt}'
     and amount_reason=6
     group by driver_id) rec ON dri.driver_id=rec.driver_id
  LEFT OUTER JOIN   
(SELECT driver_id,
       overdue_payment_cnt --违约期数
FROM
  (SELECT driver_id,
          false_id,
          dt,
          date_sub(dt,rank),
          row_number() OVER(PARTITION BY driver_id,false_id,date_sub(dt,rank)
                            ORDER BY dt) AS overdue_payment_cnt
   FROM
     (SELECT driver_id,
             false_id,
             dt,
             row_number() OVER(PARTITION BY driver_id,false_id
                               ORDER BY dt) AS rank
      FROM
        (SELECT driver_id,
                (CASE WHEN balance<0 THEN 1 ELSE 0 END) AS false_id,
                dt
         FROM oride_dw.ods_sqoop_base_data_driver_balance_extend_df
         WHERE dt BETWEEN '{prev_6_day}' AND '{pt}'
         ORDER BY dt) bal
      GROUP BY driver_id,
               false_id,
               dt) a1) x1
WHERE dt='{pt}'
  AND false_id=1) oer
ON dri.driver_id=oer.driver_id;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        prev_6_day='{{macros.ds_add(ds, -6)}}',
        table=table_name
        ),
schema='oride_dw',
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct driver_id) as cnt
      FROM oride_dw.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
        )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] >1:
        raise Exception ("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")
    
 
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag
)

#生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`
    
    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path+'/country_code=nal/dt={{ds}}'
        ),
    dag=dag)


ods_sqoop_base_data_driver_repayment_df_prev_day_tesk>>ods_sqoop_base_data_driver_balance_extend_df_prev_day_tesk>>dim_oride_driver_base_prev_day_task>>sleep_time>>dwd_oride_finance_driver_repayment_extend_df_task>>task_check_key_data>>touchz_data_success