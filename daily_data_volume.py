# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import logging
import os

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2020, 1, 16),
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

ORIDE_MODEL_PATH = "oss://opay-datalake/oride/oride_dw"
ORIDE_BURIED_PATH = "s3a://opay-bi/oride_buried"
ORIDE_BINLOG_PATH = "oss://opay-datalake/oride_binlog"
ORIDE_SQOOP_PATH = "oss://opay-datalake/oride_dw_sqoop"

OFOOD_MODEL_PATH = "oss://opay-datalake/oride/ofood_dw"
OFOOD_BURIED_PATH = "oss://opay-datalake/ofood/client"
OFOOD_SQOOP_PATH = "oss://opay-datalake/ofood_dw_sqoop"

OPAY_DW_SQOOP_DF_PATH = "oss://opay-datalake/opay_dw_ods"
OPAY_DW_SQOOP_DI_PATH = "oss://opay-datalake/opay_dw_sqoop_di"

OPAY_OWEALTH_SQOOP_DF_PATH="oss://opay-datalake/opay_owealth_ods/"

OPOY_DW_SQOOP_DF_PATH="oss://opay-datalake/opos_dw_sqoop"

OBUS_SQOOP_PATH = "oss://opay-datalake/obus_dw_sqoop"

def send_report_email(ds, **kwargs):
    email_to=['bigdata_dw@opay-inc.com']
    email_subject="每日数据量统计%s" % ds
    html="""
        日期：{dt}<br>
        业务：oride<br>
        模型日数据量:{oride_model_size}<br>
        采集日数据量:{oride_collect_size}<br>
        埋点日数据量:{oride_buried_size}<br><br>

        业务：ofood<br>
        模型日数据量:{ofood_model_size}<br>
        采集日数据量:{ofood_collect_size}<br>
        埋点日数据量:{ofood_buried_size}<br><br>

        业务：opay<br>
        采集日数据量:{opay_di_collect_size}<br>
        采集日数据量:{opay_collect_size}<br><br>

        业务：opay_owealth<br>
        采集日数据量:{opay_owealth_df_collect_size}<br><br>

        业务：opoy<br>
        采集日数据量:{opoy_df_collect_size}<br><br>

        业务：obus<br>
        采集日数据量:{obus_collect_size}<br><br>

    """.format(
        dt=ds,
        oride_model_size=getSizeInNiceString(get_dir_size(ORIDE_MODEL_PATH, ds)),
        oride_collect_size=getSizeInNiceString(get_dir_size(ORIDE_BINLOG_PATH, ds)+get_dir_size(ORIDE_SQOOP_PATH, ds)),
        oride_buried_size=getSizeInNiceString(get_s3_dir_size(ORIDE_BURIED_PATH, ds)),
        ofood_model_size=getSizeInNiceString(get_dir_size(OFOOD_MODEL_PATH, ds)),
        ofood_collect_size=getSizeInNiceString(get_dir_size(OFOOD_SQOOP_PATH, ds)),
        ofood_buried_size=getSizeInNiceString(get_dir_size(OFOOD_BURIED_PATH, ds)),
        opay_collect_size=getSizeInNiceString(get_dir_size(OPAY_DW_SQOOP_DF_PATH, ds)),
        obus_collect_size=getSizeInNiceString(get_dir_size(OBUS_SQOOP_PATH, ds)),

        opay_di_collect_size=getSizeInNiceString(get_dir_size(OPAY_DW_SQOOP_DI_PATH, ds)),
        opay_owealth_df_collect_size=getSizeInNiceString(get_dir_size(OPAY_OWEALTH_SQOOP_DF_PATH, ds)),
        opoy_df_collect_size=getSizeInNiceString(get_dir_size(OPOY_DW_SQOOP_DF_PATH, ds))

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
