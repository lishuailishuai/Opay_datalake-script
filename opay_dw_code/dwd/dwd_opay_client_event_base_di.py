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
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.CountriesAppFrame import CountriesAppFrame
args = {
    'owner': 'lishuai',
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = airflow.DAG('dwd_opay_client_event_base_di',
                 schedule_interval="00 02 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

opay_ep_logv0_prev_hour_task = HivePartitionSensor(
    task_id="opay_ep_logv0_prev_hour_task",
    table="opay_ep_logv0",
    partition="dt='{{ ds }}' and hour='22'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

opay_ep_logv1_prev_hour_task = HivePartitionSensor(
    task_id="opay_ep_logv1_prev_hour_task",
    table="opay_ep_logv1",
    partition="dt='{{ ds }}' and hour='22'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name="opay_dw"
table_name = "dwd_opay_client_event_base_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name



def dwd_opay_client_event_base_di_sql_task(ds):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';
        CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
        with opay_ep_logv0_data as (
            SELECT
                 `@timestamp` as time,get_json_object(message, '$.msg') as msg
            FROM
                oride_source.opay_ep_logv0
            WHERE
                dt='{pt}'
                AND get_json_object(message, '$.type') in ('opay')
        )
        insert overwrite table {db}.{table} partition(country_code,dt)
        SELECT
            null as client_ip,
            null as server_ip,
            concat_ws(' ',substr(time,1,10),substr(time,12,8)) as server_time,
            unix_timestamp(concat_ws(' ',substr(time,1,10),substr(time,12,8))) as server_timestamp,
            get_json_object(msg, '$.common.user_id'),
            get_json_object(msg, '$.common.user_number'),
            null as city_id,
            from_unixtime(cast(get_json_object(msg, '$.common.client_timestamp') as bigint),'yyyy-MM-dd HH:mm:ss'),
            get_json_object(msg, '$.common.client_timestamp'),
            get_json_object(msg, '$.common.platform'),
            get_json_object(msg, '$.common.os_version'),
            get_json_object(msg, '$.common.app_name'),
            get_json_object(msg, '$.common.app_version'),
            get_json_object(msg, '$.common.locale'),
            get_json_object(msg, '$.common.device_id'),
            get_json_object(msg, '$.common.device_screen'),
            get_json_object(msg, '$.common.device_model'),
            get_json_object(msg, '$.common.device_manufacturer'),
            get_json_object(msg, '$.common.is_root'),
            get_json_object(msg, '$.common.channel'),
            get_json_object(msg, '$.common.subchannel'),
            get_json_object(msg, '$.common.gaid'),
            get_json_object(msg, '$.common.appsflyer_id'),
            from_unixtime(cast(cast(e.event_time as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss'),
            cast(cast(e.event_time as bigint) / 1000 as bigint),
            e.event_name,
            e.page,
            e.source,
            e.event_value,
            null,
            null,
            null,
            null,
            'nal' as country_code,
            '{pt}' as dt
        FROM
            opay_ep_logv0_data LATERAL VIEW EXPLODE(from_json(get_json_object(msg, '$.events'), array(named_struct("event_name", "", "event_time","", "event_value","", "page","", "source","")))) es AS e;

         with opay_ep_logv1_data as (
            SELECT
                `@timestamp` as time,message as msg
            FROM
                oride_source.opay_ep_logv1
            WHERE
                dt='{pt}'
                
        )
        INSERT INTO TABLE {db}.{table} partition(country_code,dt)
        SELECT
            get_json_object(e, '$.cip') as ip,
            null as server_ip,
            concat_ws(' ',substr(time,1,10),substr(time,12,8)) as server_time,
            unix_timestamp(concat_ws(' ',substr(time,1,10),substr(time,12,8))) as server_timestamp,
            get_json_object(msg, '$.uid'),
            get_json_object(msg, '$.uno'),
            get_json_object(e, '$.cid') as city_id,
            from_unixtime(cast(get_json_object(msg, '$.t') as bigint),'yyyy-MM-dd HH:mm:ss'),
            get_json_object(msg, '$.t'),
            get_json_object(msg, '$.p'),
            get_json_object(msg, '$.ov'),
            get_json_object(msg, '$.an'),
            get_json_object(msg, '$.av'),
            get_json_object(msg, '$.l'),
            get_json_object(msg, '$.did'),
            get_json_object(msg, '$.dsc'),
            get_json_object(msg, '$.dmo'),
            get_json_object(msg, '$.dma'),
            get_json_object(msg, '$.isr'),
            get_json_object(msg, '$.ch'),
            get_json_object(msg, '$.sch'),
            get_json_object(msg, '$.gaid'),
            get_json_object(msg, '$.aid'),
            from_unixtime(cast(cast(get_json_object(e, '$.et') as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss'),
            cast(cast(get_json_object(e, '$.et') as bigint) / 1000 as bigint) ,
            get_json_object(e, '$.en'),
            null as page,
            null as source,
            get_json_object(e, '$.ev'),
            get_json_object(e, '$.lat'),
            get_json_object(e, '$.lng'),
            get_json_object(msg, '$.tid'),
            get_json_object(e, '$.bzp'),
            'nal' as country_code,
            '{pt}' as dt
        FROM
            opay_ep_logv1_data LATERAL VIEW explode(split(regexp_replace(regexp_replace(get_json_object(msg, '$.es'), '\\\\]|\\\\[',''),'\\\\}}\\,\\\\{{','\\\\}}\\;\\\\{{'),'\\\\;')) es AS e
        WHERE
            get_json_object(e, '$.bzp') in ('OPAY','HOME_MORE','CASHIN','CASHOUT','MYQR','QRSCAN','QR_COUPON','AJIRA','ADD_MONEY','BETTING','EASYCASH','ELECTRICITY','NEARBY_AGENT','PAYBILLPAYBILL_BANKING','PAYBILL_TV','PAYBILL_V2','TRANSFER','TV','VOUCHER','WATER','APPLICATION_MARKET','TRANSFER_MONEY','COMMON','SCAN_TO_PAY','MY_CODE','MERCHANT_AUTH','GROUPBY','EDUCATION','AIRTIME_DATA','SECURITY','REFER_FRIEND','REPORT_SCAM','ADD_CARD'
) OR get_json_object(e, '$.bzp') is null 
              group by
              get_json_object(e, '$.cip'),
              concat_ws(' ',substr(time,1,10),substr(time,12,8)),
              unix_timestamp(concat_ws(' ',substr(time,1,10),substr(time,12,8))),
              get_json_object(msg, '$.uid'),
              get_json_object(msg, '$.uno'),
              get_json_object(e, '$.cid'),
              from_unixtime(cast(get_json_object(msg, '$.t') as bigint),'yyyy-MM-dd HH:mm:ss'),
              get_json_object(msg, '$.t'),
              get_json_object(msg, '$.p'),
              get_json_object(msg, '$.ov'),
              get_json_object(msg, '$.an'),
              get_json_object(msg, '$.av'),
              get_json_object(msg, '$.l'),
              get_json_object(msg, '$.did'),
              get_json_object(msg, '$.dsc'),
              get_json_object(msg, '$.dmo'),
              get_json_object(msg, '$.dma'),
              get_json_object(msg, '$.isr'),
              get_json_object(msg, '$.ch'),
              get_json_object(msg, '$.sch'),
              get_json_object(msg, '$.gaid'),
              get_json_object(msg, '$.aid'),
              from_unixtime(cast(cast(get_json_object(e, '$.et') as bigint) / 1000 as bigint),'yyyy-MM-dd HH:mm:ss'),
              cast(cast(get_json_object(e, '$.et') as bigint) / 1000 as bigint) ,
              get_json_object(e, '$.en'),
              get_json_object(e, '$.ev'),
              get_json_object(e, '$.lat'),
              get_json_object(e, '$.lng'),
              get_json_object(msg, '$.tid'),
              get_json_object(e, '$.bzp')
            
            
            
        ;
    
    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_execution_time = kwargs.get('v_execution_time')
    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "false",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_client_event_base_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_client_event_base_di_task = PythonOperator(
    task_id='dwd_opay_client_event_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

opay_ep_logv0_prev_hour_task >> dwd_opay_client_event_base_di_task
opay_ep_logv1_prev_hour_task >> dwd_opay_client_event_base_di_task