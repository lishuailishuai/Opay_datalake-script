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

dag = airflow.DAG( 'dim_oride_passenger_base', 
    schedule_interval="20 01 * * *",
    default_args=args) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)


##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
ods_sqoop_base_data_user_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_data_user_df_prev_day_tesk",
      table="ods_sqoop_base_data_user_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )
#依赖前一天分区
ods_sqoop_base_data_user_extend_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_data_user_extend_df_prev_day_tesk",
      table="ods_sqoop_base_data_user_extend_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dim_oride_passenger_base"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 


dim_oride_passenger_base_task = HiveOperator(

    task_id='dim_oride_passenger_base_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT t1.passenger_id,
       --乘客ID

       t1.phone_number,
       --手机号

       t1.first_name,
       --名

       t1.last_name,
       --姓

       t1.promoter_code,
       --推广员代码

       t1.updated_at,
       --最后更新时间

       t2.avg_score,
       -- 平均评分

       t2.total_score,
       -- 总评分

       t2.score_times,
       -- 评分次数

       t2.bonus,
       -- 奖励金

       t2.balance,
       -- 余额

       from_unixtime(t2.register_time,'yyyy-MM-dd hh:mm:ss') AS register_time,
       -- 注册时间

       from_unixtime(t2.login_time,'yyyy-MM-dd hh:mm:ss') AS login_time,
       -- 最后登陆时间

       t2.inviter_role,
       --

       t2.inviter_id,
       --

       t2.invite_num,
       --

       t2.invite_complete_num,
       --

       t2.invite_award,
       --
--t2.updated_at, -- 最后更新时间,可以精确到时间

       t2.pay_type,
       -- user auto pay settings(-1: not set 0: manual payment 1: auto payment)

       t2.city_id,
       -- 注册城市

       t2.language,
       -- 客户端语言

       t1.country_code,
       '{pt}' AS dt
FROM
  (SELECT id AS passenger_id,
          --'乘客ID'

          phone_number,
          -- '手机号'

          first_name,
          -- '名'

          last_name,
          -- '姓'

          promoter_code,
          --'推广员代码'

          updated_at,
          --'最后更新时间'

          'nal' AS country_code --国家码字段

   FROM oride_dw_ods.ods_sqoop_base_data_user_df
   WHERE dt= '{pt}') t1
LEFT OUTER JOIN
  (SELECT id,
          -- 用户 ID

          avg_score,
          -- 平均评分

          total_score,
          -- 总评分

          score_times,
          -- 评分次数

          bonus,
          -- 奖励金

          balance,
          -- 余额

          register_time,
          -- 注册时间

          login_time,
          -- 最后登陆时间

          inviter_role,
          --

          inviter_id,
          --

          invite_num,
          --

          invite_complete_num,
          --

          invite_award,
          --

          updated_at,
          -- 最后更新时间

          pay_type,
          -- user auto pay settings(-1: not set 0: manual payment 1: auto payment)

          city_id,
          -- 注册城市

          LANGUAGE -- 客户端语言
FROM oride_dw_ods.ods_sqoop_base_data_user_extend_df
   WHERE dt= '{pt}') t2 ON t1.passenger_id=t2.id;
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct passenger_id) as cnt
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

ods_sqoop_base_data_user_df_prev_day_tesk>>ods_sqoop_base_data_user_extend_df_prev_day_tesk>>sleep_time>>dim_oride_passenger_base_task>>task_check_key_data>>touchz_data_success

