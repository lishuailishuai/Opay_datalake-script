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

dag = airflow.DAG( 'dwm_oride_driver_audit_third_extend_di', 
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
ods_sqoop_base_data_driver_extend_df_prev_day_task=UFileSensor(
    task_id='ods_sqoop_base_data_driver_extend_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_extend",
        pt='{{ds}}'
        ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
        )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwm_oride_driver_audit_third_extend_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dwm_oride_driver_audit_third_extend_di_task = HiveOperator(

    task_id='dwm_oride_driver_audit_third_extend_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select 
  id as driver_id,--司机 ID
  serv_mode,--服务模式 (0: no service, 1: in service)
  serv_status,--服务状态 (0: wait assign, 1: pick up, 2: send)
  order_rate,--接单率
  assign_order,--派单数量
  take_order,--接单数量
  avg_score,--平均评分
  total_score,--总评分
  score_times,--评分次数
  last_order_id,--最近一个订单的ID
  register_time,--注册时间
  login_time,--最后登陆时间
  is_bind ,--状态 0 未绑定 1 已绑定
  first_bind_time,--初次绑定时间
  total_pay,--总计-已打款收入
  inviter_role,--邀请者角色
  inviter_id,--邀请者ID
  block,--后台管理司机接单状态(0: 允许 1:不允许)
  serv_type  as product_id,--1 专车 2 快车
  serv_score,--司机服务分
  local_gov_ids,--行会ID,json
  updated_at,--最后更新时间
  fault ,--正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
  city_id,--所属城市ID
  language,--客户端语言
  end_service_time,--专车司机结束收份子钱时间
  last_online_time,--最后上线时间
  last_offline_time, --最后下线时间
  'nal' AS country_code,
             --国家码字段
  '{pt}' as dt
from oride_dw_ods.ods_sqoop_base_data_driver_extend_df
where dt='{pt}'
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
schema='oride_dw',
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct driver_id) as cnt
      FROM oride_dw.dwm_oride_driver_audit_third_extend_di

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

ods_sqoop_base_data_driver_extend_df_prev_day_task>>sleep_time>>dwm_oride_driver_audit_third_extend_di_task>>task_check_key_data>>touchz_data_success
