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
    'owner': 'chenlili',
    'start_date': datetime(2019, 9, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_passenger_base_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dim_oride_passenger_base_prev_day_task = UFileSensor(
    task_id='dim_oride_passenger_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_passenger_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##

table_name = "dm_oride_passenger_base_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dm_oride_passenger_base_cube_d_task = HiveOperator(

    task_id='dm_oride_passenger_base_cube_d_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    with order_base_data as (
          SELECT if(t2.passenger_id IS NULL,1,0) AS is_first_order_mark,--准确说历史没有完单的是否本日首次
             if(t3.passenger_id IS NOT NULL,1,0) AS new_reg_user_mark, --是否当日新注册乘客
             null as is_fraud, --是否疑似作弊订单
             t1.*
            FROM
              (SELECT *
               FROM oride_dw.dwd_oride_order_base_include_test_di
               WHERE dt='{pt}'
               and city_id<>'999001' --去除测试数据
               and driver_id<>1) t1
            LEFT JOIN
              (SELECT passenger_id
               FROM oride_dw.dwd_oride_order_base_include_test_df
               WHERE dt in('{pt}','his')
                 AND create_date<dt
                 AND status IN(4,
                               5)
               GROUP BY passenger_id) t2 ON t1.passenger_id=t2.passenger_id
            LEFT JOIN
              (SELECT *
               FROM oride_dw.dim_oride_passenger_base
               WHERE dt='{pt}'
                 AND substr(register_time,1,10)=dt) t3 ON t1.passenger_id=t3.passenger_id
                    )
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    select 
        nvl(t2.city_id,-10000) as city_id,
        nvl(t2.product_id,-10000) as product_id,
        nvl(t1.new_users,0) as new_users,  --当天注册乘客数
        nvl(t1.act_users,0) as act_users,  --当天活跃乘客数
        nvl(t2.ord_users,0) as ord_users,  --当日下单乘客数
        nvl(t2.finished_users,0) as finished_users,  --当日完单乘客数
        nvl(t2.first_finished_users,0) as first_finished_users,  --当日订单中首次完单乘客数
        nvl(t2.old_finished_users,0) as old_finished_users,   --当日订单中完单老客数
        nvl(t2.new_user_ord_cnt,0) as new_user_ord_cnt,  --当日新注册乘客下单量
        nvl(t2.new_user_finished_cnt,0) as new_user_finished_cnt,  --当日新注册乘客完单量
        nvl(t2.new_user_gmv,0.0) as new_user_gmv,  --当日注册乘客完单gmv
        nvl(t2.paid_users,0) as paid_users,  --当日所有支付乘客数
        nvl(t2.online_paid_users,0) as online_paid_users,--当日线上支付乘客数
        nvl(t2.fraud_user_cnt,0) as fraud_user_cnt, --疑似作弊订单乘客数
        nvl(t2.driver_serv_type) as driver_serv_type, --订单表中司机业务类型
        nvl(t2.country_code,'nal') as country_code,
        '{pt}' dt     
        from (SELECT 'nal' AS country_code,
               -10000 AS city_id,
               -10000 AS product_id,
               -10000 as driver_serv_type,
               count(if(substr(register_time,1,10)=dt,passenger_id,NULL)) AS new_users, --当天注册乘客数
               count(if(substr(login_time,1,10)=dt,passenger_id,NULL)) AS act_users --当天活跃乘客数
        FROM oride_dw.dim_oride_passenger_base
        WHERE dt='{pt}') t1
        
        right join
        
        (SELECT nvl(country_code,'-10000') as country_code,
               nvl(city_id,-10000) as city_id,
               nvl(product_id,-10000) as product_id, --招手停订单数限定具体业务线
               nvl(driver_serv_type,-10000) as driver_serv_type, --订单表中对应的司机业务类型
         count(DISTINCT passenger_id) AS ord_users, --当日下单乘客数
         count(DISTINCT (if(status IN(4,5),passenger_id,NULL))) AS finished_users, --当日完单乘客数
         count(DISTINCT (IF (status IN(4,5)
                             AND is_first_order_mark=1,passenger_id,NULL))) AS first_finished_users, --当日订单中首次完单乘客数
         count(DISTINCT (IF (status IN(4,5)
                             AND is_first_order_mark=0,passenger_id,NULL))) AS old_finished_users, --当日订单中完单老客数
         count(IF (new_reg_user_mark=1,order_id,NULL)) AS new_user_ord_cnt, --当日新注册乘客下单量
         count(IF (new_reg_user_mark=1
                   AND status IN(4,5),order_id,NULL)) AS new_user_finished_cnt, --当日新注册乘客完单量
         sum(IF (new_reg_user_mark=1
                   AND status in(4,5),price,0.0)) AS new_user_gmv, --当日注册乘客完单gmv
         count(distinct(IF (pay_status=1,passenger_id,NULL))) AS paid_users, --当日所有支付乘客数
         count(distinct(IF (pay_status=1
                            AND pay_mode IN(2,3),passenger_id,NULL))) AS online_paid_users, --当日线上支付乘客数
         null as fraud_user_cnt --疑似作弊订单乘客数
        FROM order_base_data
        group by nvl(country_code,'-10000'),
               nvl(city_id,-10000),
               nvl(product_id,-10000),
               nvl(driver_serv_type,-10000)
        with cube) t2
        on t1.country_code=t2.country_code and t1.city_id=nvl(t2.city_id,-10000) and t1.product_id=nvl(t2.product_id,-10000)
        where nvl(t2.country_code,'-10000')<>'-10000' and nvl(t2.city_id,-10000)<>999001;
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    dag=dag)

# 生成_SUCCESS
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
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwd_oride_order_base_include_test_df_prev_day_task >> \
dependence_dim_oride_passenger_base_prev_day_task >> \
sleep_time >> \
dm_oride_passenger_base_cube_d_task >> \
touchz_data_success
