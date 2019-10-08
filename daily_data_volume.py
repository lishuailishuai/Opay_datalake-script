# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import logging
import os

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 10, 6),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'daily_data_volume',
    schedule_interval="30 08 * * *",
    default_args=args)

def getSizeInNiceString(sizeInBytes):
    """
    Convert the given byteCount into a string like: 9.9bytes/KB/MB/GB
    """
    for (cutoff, label) in [(1024*1024*1024, "GB"),
                            (1024*1024, "MB"),
                            (1024, "KB"),
                            ]:
        if sizeInBytes >= cutoff:
            return "%.1f %s" % (sizeInBytes * 1.0 / cutoff, label)

    if sizeInBytes == 1:
        return "1 byte"
    else:
        bytes = "%.1f" % (sizeInBytes or 0,)
        return (bytes[:-2] if bytes.endswith('.0') else bytes) + ' bytes'

def get_dir_size(path, dt):
    total_size = 0
    get_dir_cmd = """
        hdfs dfs -ls -R {path} | grep drwx | grep dt={dt}$ | awk '{{print $8}}'
    """.format(path=path, dt=dt)
    for dir_path in os.popen(get_dir_cmd).readlines():
        dir_path = dir_path.strip()
        get_size_cmd = """
            hdfs dfs -du -s {dir_path} | awk '{{print $1}}'
        """.format(dir_path=dir_path)
        dir_size = os.popen(get_size_cmd).readline()
        dir_size = int(dir_size.strip())
        total_size += dir_size
        logging.info("dir_path:%s, size:%s", dir_path, getSizeInNiceString(dir_size))
    logging.info("path:%s, total size:%s", path, getSizeInNiceString(total_size))
    return total_size

def get_s3_dir_size(path, dt):
    total_size = 0
    get_dir_cmd = """
        hdfs dfs -ls {path} | grep drwx | grep -v flink | awk '{{print $8}}'
    """.format(path=path, dt=dt)
    for dir_path in os.popen(get_dir_cmd).readlines():
        dir_path = dir_path.strip()
        get_size_cmd = """
            hdfs dfs -du -s {dir_path}/dt={dt} | awk '{{print $1}}'
        """.format(dir_path=dir_path,dt=dt)
        try:
            dir_size = os.popen(get_size_cmd).readline()
            dir_size = int(dir_size.strip())
        except:
            dir_size = 0
        total_size += dir_size
        logging.info("dir_path:%s/dt=%s, size:%s", dir_path, dt, getSizeInNiceString(dir_size))
    logging.info("path:%s, total size:%s", path, getSizeInNiceString(total_size))
    return total_size

MODEL_PATH = "ufile://opay-datalake/oride/oride_dw"
BURIED_PATH = "s3a://opay-bi/oride_buried"
BINLOG_PATH = "s3a://opay-bi/oride_binlog"
SQOOP_PATH = "ufile://opay-datalake/oride_dw_sqoop"

def send_report_email(ds, **kwargs):
    email_to=['bigdata_dw@opay-inc.com']
    email_subject="每日数据量统计%s" % ds
    html="""
        日期：{dt}<br>
        业务：oride<br>
        模型日数据量:{model_size}<br>
        采集日数据量:{collect_size}<br>
        埋点日数据量:{buried_size}<br>
    """.format(
        dt=ds,
        model_size=getSizeInNiceString(get_dir_size(MODEL_PATH, ds)),
        collect_size=getSizeInNiceString(get_s3_dir_size(BINLOG_PATH, ds)+get_dir_size(SQOOP_PATH, ds)),
        buried_size=getSizeInNiceString(get_s3_dir_size(BURIED_PATH, ds))
    )
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')

send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)
