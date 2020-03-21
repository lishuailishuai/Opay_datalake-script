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
from airflow.sensors import OssSensor
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lishuai',
    'start_date': datetime(2020, 3, 19),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_oride_driver_base_hf',
                  schedule_interval="40 00 * * *",
                  default_args=args,

                  )

##----------------------------------------- 依赖 ---------------------------------------##
# 依赖前天分区
dwd_oride_driver_hf_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_hf_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_hf/country_code=nal",
            pt='{{ds}}',
            now_day='{{macros.ds_add(ds, +1)}}',
            hour='{{ execution_date.strftime("%H") }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

dwd_oride_driver_extend_hf_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_extend_hf_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_extend_hf/country_code=nal",
            pt='{{ds}}',
            now_day='{{macros.ds_add(ds, +1)}}',
            hour='{{ execution_date.strftime("%H") }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)


dim_oride_city_prev_day_tesk = OssSensor(
    task_id='dim_oride_city_prev_day_tesk',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "dim_oride_driver_base_hf"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=table_name),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##
def dim_oride_driver_base_hf_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    SELECT dri.id as driver_id,
        ext.city_id,--所属城市ID
        
        dri.phone_number as phone_number,--手机号
        
        dri.password as password,--密码
        
        dri.opay_account as opay_account,--opay 账号
        
        dri.plate_number as plate_number,--车牌号
        
        dri.real_name as driver_name,--真实姓名
        
        dri.birthday as birthday,--生日
        
        dri.gender as gender,--性别
        
        dri.government as government,--Local Government
        
        dri.country as country,--国家
        
        cit.city_name,--城市名称
        
        dri.black as black,--黑名单0正常1删除
        
        dri.group_id as group_id,--所属组id
        
        ext.serv_mode,--服务模式 (0: no service, 1: in service)
        
        ext.serv_status,--服务状态 (0: wait assign, 1: pick up, 2: send)
        
        from_unixtime(ext.local_register_time,'yyyy-MM-dd HH:mm:ss') as register_time,--注册时间
        
        from_unixtime(ext.local_login_time,'yyyy-MM-dd HH:mm:ss') as login_time,--最后登陆时间
        
        ext.is_bind,--状态 0 未绑定 1 已绑定
        
        from_unixtime(ext.local_first_bind_time,'yyyy-MM-dd HH:mm:ss') as first_bind_time,--初次绑定时间
        
        ext.block,--后台管理司机接单状态(0: 允许 1:不允许)
        
        ext.product_id,--1 专车 2 快车 3 Otrike
        
        ext.local_gov_ids,--行会ID,json
        
        dri.updated_at as updated_at,--最后更新时间
        
        ext.fault,--正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
        
        ext.LANGUAGE, --客户端语言
        
        from_unixtime(ext.local_end_service_time,'yyyy-MM-dd HH:mm:ss') as end_service_time,--专车司机结束收份子钱时间
        
        nvl(cit.country_code,'nal') AS country_code,--国家码字段
        
        '{pt}' as dt
    FROM
    (
        
        SELECT *
            FROM oride_dw.dwd_oride_driver_hf
        where dt='{pt}' and hour=22 
        
    ) dri 
    
    LEFT OUTER JOIN
    (
        SELECT id AS driver_id,--司机 ID
        
        serv_mode,--服务模式 (0: no service, 1: in service)
        
        serv_status,--服务状态 (0: wait assign, 1: pick up, 2: send)
        
        register_time,--注册时间
        
        login_time,--最后登陆时间
        
        is_bind,--状态 0 未绑定 1 已绑定
        
        first_bind_time,--初次绑定时间
        
        block,--后台管理司机接单状态(0: 允许 1:不允许)
        
        serv_type AS product_id,--1 专车 2 快车
        
        local_gov_ids,--行会ID,json
        
        fault,--正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
        
        city_id,--所属城市ID
        
        language, --客户端语言
        
        country_id,  --所属国家

        register_time as local_register_time,

        login_time as local_login_time,

        first_bind_time as local_first_bind_time,

        end_service_time as local_end_service_time

        from(
        
        SELECT *
            FROM oride_dw.dwd_oride_driver_extend_hf
        where dt='{pt}' and hour=22 
        
        ) ee 
        
    ) ext

    ON dri.id=ext.driver_id

    LEFT OUTER JOIN
    (
        select * 
        from oride_dw.dim_oride_city 
        where dt = '{pt}' 
    ) cit
    ON cit.city_id=ext.city_id;

    '''.format(
        pt=ds,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct driver_id) as cnt
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
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_oride_driver_base_hf_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_oride_driver_base_hf_task = PythonOperator(
    task_id='dim_oride_driver_base_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_driver_hf_prev_day_task >> dim_oride_driver_base_hf_task
dwd_oride_driver_extend_hf_prev_day_task >> dim_oride_driver_base_hf_task
dim_oride_city_prev_day_tesk >> dim_oride_driver_base_hf_task