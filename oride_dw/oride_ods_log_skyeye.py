import airflow
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 29),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_ods_log_skyeye',
    schedule_interval="00 07 * * *",
    default_args=args)


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, db_name, table_name, **op_kwargs):
    tb = [
        {"db": db_name, "table":table_name, "partition": "dt={pt}".format(pt=ds), "timeout": "7200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

'''
源数据表
'''
#

table_list = [
    "driver",
    "order",
    "passenge"
]
HDFS_PATH = "ufile://opay-datalake/oride-research/tags/{table}_tags/dt={dt}"
for table in table_list:
    check_ufile=UFileSensor(
        task_id='check_ufile_{}'.format(table),
        filepath='oride-research/tags/{table}_tags/dt={{{{ ds }}}}/upload_success.txt'.format(table=table),
        bucket_name='opay-datalake',
        timeout=86400,
        dag=dag)

    # add partitions
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table),
        hql='''
                ALTER TABLE oride_dw_ods.`ods_log_oride_{table}_skyeye_di` ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
            '''.format(table=table),
        schema='oride_dw_ods',
        dag=dag)

    touchz_data_success = BashOperator(
        task_id='touchz_data_success_{}'.format(table),
        bash_command="""
                line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

                if [ $line_num -eq 0 ]
                then
                    echo "FATAL {hdfs_data_dir} is empty"
                else
                    echo "DATA EXPORT Successed ......"
                    $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
                fi
            """.format(
            hdfs_data_dir=HDFS_PATH.format(table=table, dt='{{ds}}')
        ),
        dag=dag)

    # 超时监控
    task_timeout_monitor= PythonOperator(
        task_id='task_timeout_monitor_{}'.format(table),
        python_callable=fun_task_timeout_monitor,
        provide_context=True,
        op_kwargs={
            'db_name': 'oride_dw_ods',
            'table_name': 'ods_log_oride_%s_skyeye_di' % table,
        },
        dag=dag
    )

    check_ufile >> add_partitions >> touchz_data_success
