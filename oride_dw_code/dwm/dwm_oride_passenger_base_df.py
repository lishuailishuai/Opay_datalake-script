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
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 12, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_passenger_base_df',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dim_oride_passenger_base_prev_day_task = UFileSensor(
    task_id='dim_oride_passenger_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_passenger_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_order_base_include_test_di_prev_day_task = S3KeySensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-bi',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwm_oride_passenger_base_df_prev_day_task = UFileSensor(
    task_id='dwm_oride_passenger_base_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_passenger_base_df/country_code=nal",
        pt='{{macros.ds_add(ds, -1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_passenger_base_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwm_oride_passenger_base_df_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
with data_order as(
select if(create_time=first_ord_create_time,1,0) as first_ord_mark,  --首次下单标志
       if(create_time=first_finish_create_time,1,0) as first_finish_ord_mark, --首次完单标志
       if(create_time=first_finished_create_time,1,0) as first_finished_ord_mark, --首次完成支付标志
       if(create_time=recent_ord_create_time,1,0) as recent_ord_mark,  --最近一次下单标志
       if(create_time=recent_finish_create_time,1,0) as recent_finish_ord_mark, --最近一次完单标志
       if(create_time=recent_finished_create_time,1,0) as recent_finished_ord_mark, --最近一次完成支付标志
       *
from(select *,
       min(create_time) over(partition by passenger_id) as first_ord_create_time, --首次下单订单时间
       min(if(is_td_finish=1,create_time,null)) over(partition by passenger_id) as first_finish_create_time, --首次完单订单时间
       min(if(is_td_finish_pay=1,create_time,null)) over(partition by passenger_id) as first_finished_create_time, --首次完成支付订单时间
       max(create_time) over(partition by passenger_id) as recent_ord_create_time, --最近一次下单订单时间
       max(if(is_td_finish=1,create_time,null)) over(partition by passenger_id) as recent_finish_create_time, --最近一次完单订单时间
       max(if(is_td_finish_pay=1,create_time,null)) over(partition by passenger_id) as recent_finished_create_time  --最近一次完成支付订单时间
from oride_dw.dwd_oride_order_base_include_test_di   --首次跑数需要用订单表所有数据
where dt='{pt}' 
and city_id<>'999001' --去除测试数据
and driver_id<>1
) t
)
INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT dim_user.passenger_id,
       dim_user.city_id,
       dim_user.register_time, --乘客注册时间
       if(substr(dim_user.register_time,1,10)=dim_user.dt,1,0) as if_td_register,  --是否当天注册
       if(substr(dim_user.login_time,1,10)=dim_user.dt,1,0) as if_td_act,  --是否当天活跃
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_ord_id,first_order.order_id) as first_ord_id,  --乘客首次下单order_id 
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_create_date,first_order.create_date) as first_create_date,  --乘客首次下单时间   
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_city_id,first_order.city_id) as first_city_id,  --乘客首次下单对应的city_id
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_product_id,first_order.product_id) as first_product_id,  --乘客首次下单对应的product_id 
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_country_code,first_order.country_code) as first_country_code,  --乘客首次下单对应的国家码
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_ord_price,first_order.price) as first_ord_price,  --首次下单订单对应的gmv
       if(yes_dwm_user.first_ord_id is not null,yes_dwm_user.first_ord_distance,first_order.distance) as first_ord_distance,  --首次下单订单对应的里程   

       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_ord_id,first_finish_order.order_id) as first_finish_ord_id,  --乘客首次完单order_id 
       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_create_date,first_finish_order.create_date) as first_finish_create_date,  --乘客首次完单时间   
       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_city_id,first_finish_order.city_id) as first_finish_city_id,  --乘客首次完单对应的city_id
       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_product_id,first_finish_order.driver_serv_type) as first_finish_product_id,  --乘客首次完单对应的product_id
       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_country_code,first_finish_order.country_code) as first_finish_country_code,  --乘客首次完单对应的国家码 
       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_ord_price,first_finish_order.price) as first_finish_ord_price,  --首次完单订单对应的gmv
       if(yes_dwm_user.first_finish_ord_id is not null,yes_dwm_user.first_finish_ord_distance,first_finish_order.distance) as first_finish_ord_distance,  --首次完单订单对应的里程 
       
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_ord_id,recent_order.order_id) as recent_ord_id,  --乘客最近一次下单order_id 
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_create_date,recent_order.create_date) as recent_create_date,  --乘客最近一次下单时间   
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_city_id,recent_order.city_id) as recent_city_id,  --乘客最近一次下单对应的city_id
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_product_id,recent_order.product_id) as recent_product_id,  --乘客最近一次下单对应的product_id 
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_country_code,recent_order.country_code) as recent_country_code,  --乘客最近一次下单对应的国家码
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_ord_price,recent_order.price) as recent_ord_price,  --最近一次下单订单对应的gmv
       if(yes_dwm_user.recent_ord_id is not null,yes_dwm_user.recent_ord_distance,recent_order.distance) as recent_ord_distance,  --最近一次下单订单对应的里程   

       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_ord_id,recent_finish_order.order_id) as recent_finish_ord_id,  --乘客最近一次完单order_id 
       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_create_date,recent_finish_order.create_date) as recent_finish_create_date,  --乘客最近一次完单时间   
       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_city_id,recent_finish_order.city_id) as recent_finish_city_id,  --乘客最近一次完单对应的city_id
       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_product_id,recent_finish_order.driver_serv_type) as recent_finish_product_id,  --乘客最近一次完单对应的product_id
       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_country_code,recent_finish_order.country_code) as recent_finish_country_code,  --乘客最近一次完单对应的国家码 
       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_ord_price,recent_finish_order.price) as recent_finish_ord_price,  --最近一次完单订单对应的gmv
       if(yes_dwm_user.recent_finish_ord_id is not null,yes_dwm_user.recent_finish_ord_distance,recent_finish_order.distance) as recent_finish_ord_distance,
       'nal' as country_code,
       '{pt}' as dt

       from (select * 
from oride_dw.dim_oride_passenger_base
WHERE dt='{pt}') dim_user
left join
(select * 
from oride_dw.dwm_oride_passenger_base_df
where dt='{bef_yes_day}') yes_dwm_user
on dim_user.passenger_id=yes_dwm_user.passenger_id
left join
(select * from data_order where first_ord_mark=1) first_order
on dim_user.passenger_id=first_order.passenger_id
left join
(select * from data_order where first_finish_ord_mark=1) first_finish_order
on dim_user.passenger_id=first_finish_order.passenger_id
left join
(select * from data_order where recent_ord_mark=1) recent_order
on dim_user.passenger_id=recent_order.passenger_id
left join
(select * from data_order where recent_finish_ord_mark=1) recent_finish_order
on dim_user.passenger_id=recent_finish_order.passenger_id;
    '''.format(
        pt=ds,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct passenger_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()
    """
            #功能函数
            alter语句: alter_partition
            删除分区: delete_partition
            生产success: touchz_success

            #参数
            第一个参数true: 所有国家是否上线。false 没有
            第二个参数true: 数据目录是有country_code分区。false 没有
            第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

            #读取sql
            %_sql(ds,v_hour)

            第一个参数ds: 天级任务
            第二个参数v_hour: 小时级任务，需要使用

        """
    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_oride_passenger_base_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 熔断数据
    check_key_data_task(ds)

    # 生产success
    cf.touchz_success()


dwm_oride_passenger_base_df_task = PythonOperator(
    task_id='dwm_oride_passenger_base_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dim_oride_passenger_base_prev_day_task >> dwm_oride_passenger_base_df_task
dwd_oride_order_base_include_test_di_prev_day_task >> dwm_oride_passenger_base_df_task
dwm_oride_passenger_base_df_prev_day_task >> dwm_oride_passenger_base_df_task
