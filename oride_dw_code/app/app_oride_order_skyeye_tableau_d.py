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

args = {
    'owner': 'lijialong',
    'start_date': datetime(2019, 9, 21),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_skyeye_tableau_d',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

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

dependence_dwd_oride_order_skyeye_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_skyeye_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_skyeye_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_order_skyeye_tableau_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

app_oride_order_skyeye_tableau_d_task = HiveOperator(
    task_id='app_oride_order_skyeye_tableau_d_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite TABLE oride_dw.{table} partition(country_code,dt)

    --天眼系统tableau监控
    select
        bit.city_id,
        count(if(os.is_fraud_order = true,os.order_id,null)) as fraud_order_cnt,--疑似作弊订单量
        count(distinct if(os.is_fraud_order = true,bit.driver_id,null)) as fraud_driver_cnt,--涉及司机人数（一个司机可以接多单）
        count(distinct if(os.is_fraud_order = true,bit.passenger_id,null)) as fraud_passenger_cnt,--涉及乘客人数
        count(if(os.is_fraud_order = true and bit.pay_mode in(2,3),os.order_id,null )) as fraud_online_pay_order_cnt,--线上支付订单量
        count(if(os.is_fraud_order = true and bit.pay_mode in(2,3),os.order_id,null )) / count(if(os.is_fraud_order = true,os.order_id,null)) as fraud_online_pay_order_rio,--线上支付占比
        count(if(bit.status in(4,5),bit.order_id,null)) as completed_num,--大盘完单量 (完单条件，status = 4，5)
        count(if(os.is_fraud_order = true,os.order_id,null)) / count(if(bit.status in(4,5),bit.order_id,null)) as fraud_order_rio,--疑似作弊订单占比
        nvl(bit.country_code,-10000)  as country_code,
        '{pt}'  as dt
    from(   
        select 
            city_id,
            order_id,
            driver_id,
            passenger_id,
            status,
            country_code,
            pay_mode
        from oride_dw.dwd_oride_order_base_include_test_di
        where dt = '{pt}' and city_id != 999001 and  driver_id != 1  
    )bit
    left join 
    (
        select 
            order_id,
            is_fraud_order
        from oride_dw.dwd_oride_order_skyeye_di
        where dt = '{pt}' and order_id is not null
    )os on bit.order_id = os.order_id
    group by bit.city_id,bit.country_code;    
'''.format(
        pt='{{ds}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)

# 生成_SUCCESS
touchz_data_success = BashOperator(
    task_id='touchz_data_sucess',
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
    dag=dag
)

# 执行依赖顺序

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwd_oride_order_skyeye_di_prev_day_task >> \
sleep_time >> \
app_oride_order_skyeye_tableau_d_task >> \
touchz_data_success