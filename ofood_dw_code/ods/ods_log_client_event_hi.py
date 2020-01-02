import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.models import Variable
args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 10, 10),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'ods_ofood_log_client_event_hi',
    schedule_interval="15 * * * *",
    concurrency=15,
    default_args=args)

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    path='ufile://opay-datalake/ofood/client'

else:

    path='oss://opay-datalake/ofood/client'

create_ods_log_client_event_hi = HiveOperator(
    task_id='create_ods_log_client_event_hi',
    hql='''
        CREATE EXTERNAL TABLE if not exists ods_log_client_event_hi (
            user_id bigint comment'用户ID',
            user_number string comment '用户no',
            client_timestamp int comment '客户端时间戳',
            platform string comment '平台ios/android',
            os_version string comment '系统版本',
            app_name string comment'应用名',
            app_version string comment '应用版本',
            locale string comment '本地语言',
            device_id string comment '设备号',
            device_screen string comment '设备分辨率',
            device_model string comment '设备类型',
            device_manufacturer string comment '设备品牌',
            is_root string comment '是否root',
            event_name string comment '事件名',
            page string comment 'page',
            event_values string comment '事件内容'
        )
        PARTITIONED BY (`dt` string, `hour` string) 
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' with SERDEPROPERTIES("ignore.malformed.json"="true")
        LOCATION '{location_path}'
    '''.format(location_path=path),
    schema='ofood_dw_ods',
    dag=dag)

ods_log_client_event_hi_partition = HiveOperator(
    task_id='ods_log_client_event_hi_partition',
    hql="""
            ALTER TABLE ods_log_client_event_hi ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        """,
    schema='ofood_dw_ods',
    dag=dag)


create_ods_log_client_event_hi >> ods_log_client_event_hi_partition

