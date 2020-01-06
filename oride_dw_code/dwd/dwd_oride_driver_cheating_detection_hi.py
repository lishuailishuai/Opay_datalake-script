import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 10, 15),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'dwd_oride_driver_cheating_detection_hi',
    schedule_interval="30 * * * *",
    default_args=args)
##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_oride_driver_cheating_detection_hi"

##----------------------------------------- 依赖 ---------------------------------------##

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    server_event_task = HivePartitionSensor(
        task_id="server_event_task",
        table="server_event",
        partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name
else:
    print("成功")

    server_event_task = HivePartitionSensor(
        task_id="server_event_task",
        table="server_event",
        partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_driver_cheating_detection_hi_task = HiveOperator(
    task_id='dwd_oride_driver_cheating_detection_hi_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt,hour)
        SELECT
            ip,
            server_ip,
            `timestamp`,
            common.user_id,
            common.user_number,
            common.client_timestamp,
            common.platform,
            common.os_version,
            common.app_name,
            common.app_version,
            common.locale,
            common.device_id,
            common.device_screen,
            common.device_model,
            common.device_manufacturer,
            common.is_root,
            common.channel,
            common.subchannel,
            common.gaid,
            common.appsflyer_id,
            e.event_time,
            e.event_name,
            e.page,
            e.source,
            e.event_value,
            'nal' as country_code,
            dt,
            hour
        FROM
            oride_source.server_event LATERAL VIEW EXPLODE(events) es AS e
        WHERE
            dt='{pt}'
            AND hour='{now_hour}'
            AND e.event_name='invitation_code_click_confirm'

        ;


'''.format(
        pt='{{ds}}',
        now_day='{{ds}}',
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name
    ),
    dag=dag
)

# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""

    #增加单独处理
    $HADOOP_HOME/bin/hadoop fs -mkdir -p {hdfs_data_dir}
    $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS

    # line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`
    # 
    # if [ $line_num -eq 0 ]
    # then
    #     echo "FATAL {hdfs_data_dir} is empty"
    #     exit 1
    # else
    #     echo "DATA EXPORT Successed ......"
    #     $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    # fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}/hour={{ execution_date.strftime("%H") }}'
    ),
    dag=dag)

server_event_task >> dwd_oride_driver_cheating_detection_hi_task >> touchz_data_success

