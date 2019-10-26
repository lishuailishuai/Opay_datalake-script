# -*- coding: utf-8 -*-
import logging
from datetime import datetime, timedelta

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor

from utils.connection_helper import get_hive_cursor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess

args = {
    'owner': 'chenlili',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_finance_df',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

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
ods_sqoop_base_data_driver_recharge_records_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_recharge_records_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_recharge_records",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_driver_reward_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_reward_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_reward",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_driver_records_day_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_records_day_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_records_day",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_order_finance_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_order_finance_df_task = HiveOperator(
    task_id='dwd_oride_order_finance_df_task',

    hql='''
SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT ord.city_id,
ord.product_id,
 ord.order_id, --订单号
 ord.create_date,--订单日期
 ord.driver_id, --司机id
 sum(nvl(recharge.amount,0.0)) AS recharge_amount, --充值金额
 sum(nvl(reward.amount,0.0)) AS reward_amount, --奖励金额
 sum(nvl(records.amount_pay_online,0.0)) AS amount_pay_online, --当日总收入-线上支付金额
 sum(nvl(records.amount_pay_offline,0.0)) AS amount_pay_offline, --当日总收入-线下支付金额
 ord.driver_serv_type, --订单表中司机业务类型字段
 'nal' as country_code,
 '{pt}' as dt
FROM
  (SELECT *
   FROM oride_dw.dwd_oride_order_base_include_test_df
   WHERE dt IN('{pt}',
               'his')) ord
LEFT JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df
   WHERE dt='{pt}'
     AND amount>0) recharge ON ord.order_id=recharge.order_id
LEFT JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_driver_reward_df
   WHERE dt='{pt}') reward ON ord.order_id=reward.order_id
LEFT JOIN
  (SELECT driver_id,
          from_unixtime(DAY,'yyyy-MM-dd') AS DAY,
          amount_pay_online,  --线上支付金额
          amount_pay_offline   --线下支付金额
   FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
   WHERE dt='{pt}') records ON ord.driver_id=records.driver_id
AND substr(ord.finish_time,1,10)=records.day
where ord.city_id<>999001
and ord.driver_id<>1
GROUP BY ord.city_id,
ord.product_id,
ord.order_id, --订单号
 ord.create_date,--订单日期
 ord.driver_id,
 ord.driver_serv_type;
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag
)


def check_key_data(ds, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      GROUP BY order_id HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
    )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] > 1:
        raise Exception("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")


# 主键重复校验
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
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

dependence_dwd_oride_order_base_include_test_df_prev_day_task >> \
ods_sqoop_base_data_driver_recharge_records_df_prev_day_task >> \
ods_sqoop_base_data_driver_reward_df_prev_day_task >>\
ods_sqoop_base_data_driver_records_day_df_prev_day_task >>\
sleep_time >> \
dwd_oride_order_finance_df_task >> \
task_check_key_data >> \
touchz_data_success
