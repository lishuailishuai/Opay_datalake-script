import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors import UFileSensor
from airflow.models import Variable
from airflow.sensors import OssSensor

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 6, 16),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_cheating_detection',
    schedule_interval="30 * * * *",
    default_args=args)



#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    # 依赖前一天分区
    dwd_oride_driver_cheating_detection_hi_prev_hour_task = UFileSensor(
        task_id='dwd_oride_driver_cheating_detection_hi_prev_hour_task',
        filepath='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_cheating_detection_hi/country_code=nal",
            pt='{{ds}}',
            hour='{{ execution_date.strftime("%H") }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

else:
    dwd_oride_driver_cheating_detection_hi_prev_hour_task = OssSensor(
        task_id='dwd_oride_driver_cheating_detection_hi_prev_hour_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_driver_cheating_detection_hi/country_code=nal",
            pt='{{ds}}',
            hour='{{ execution_date.strftime("%H") }}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )


clear_promoter_mysql_data = MySqlOperator(
    task_id='clear_promoter_mysql_data',
    sql="""
        DELETE FROM promoter_users_device WHERE dt='{{ ds_nodash }}' AND hour='{{ execution_date.hour }}';
        DELETE FROM promoter_data_hour WHERE day='{{ ds_nodash }}' and hour='{{ execution_date.hour}}';
    """,
    mysql_conn_id='opay_spread_mysql',
    dag=dag)

promoter_detail_to_msyql = HiveToMySqlTransfer(
    task_id='promoter_detail_to_msyql',
    sql="""
            SELECT
                null as id,
                t.code as code,
                t.bind_number as phone,
                t.bind_device as device_id,
                0 as register_time,
                t.bind_time as bind_time,
                {{ ds_nodash }},
                {{ execution_date.hour}},
                ip
            FROM oride_dw.dwd_oride_driver_cheating_detection_hi 
            LATERAL VIEW json_tuple(event_value, 'bind_refferal_code', 'bind_number', 'bind_device_id', 'bind_time') t AS 
                code, bind_number, bind_device, bind_time  
            WHERE
                  dt='{{ ds }}'
                  AND hour='{{ execution_date.strftime("%H") }}'
        """,
    mysql_conn_id='opay_spread_mysql',
    mysql_table='promoter_users_device',
    dag=dag
)

promoter_hour_to_msyql = HiveToMySqlTransfer(
    task_id='promoter_hour_to_msyql',
    sql="""
            SELECT
                null as id,
                t.code as code,
                from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'), 'yyyyMMdd'),
                cast(hour as int),
                COUNT(DISTINCT t.bind_number) as users_count,
                COUNT(DISTINCT if (length(t.bind_device)>0, t.bind_device, NULL)) as device_count,
                unix_timestamp()
            FROM oride_dw.dwd_oride_driver_cheating_detection_hi 
            LATERAL VIEW json_tuple(event_value, 'bind_refferal_code', 'bind_number', 'bind_device_id') t AS 
                code, bind_number, bind_device 
            WHERE
                dt='{{ ds }}'
                AND hour='{{ execution_date.strftime("%H") }}'
            GROUP BY
                t.code,
                dt,
                hour
        """,
    mysql_conn_id='opay_spread_mysql',
    mysql_table='promoter_data_hour',
    dag=dag)


dwd_oride_driver_cheating_detection_hi_prev_hour_task >> promoter_detail_to_msyql
dwd_oride_driver_cheating_detection_hi_prev_hour_task >> promoter_hour_to_msyql
clear_promoter_mysql_data >> promoter_detail_to_msyql
clear_promoter_mysql_data >> promoter_hour_to_msyql
