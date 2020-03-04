# coding=utf-8
import airflow
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator  import BashOperator
from airflow.operators.hive_operator import HiveOperator
from utils.connection_helper import get_db_conn, get_redis_connection
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.models import Variable
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from multiprocessing import Process, Queue, Manager
from os import getpid

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 10, 10),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
}

dag = airflow.DAG( 'ods_oride_log_driver_timerange',
    schedule_interval="10 00 * * *",
    default_args=args,
    )

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw_ods"
table_name = "ods_log_oride_driver_timerange"
hdfs_path = Variable.get("OBJECT_STORAGE_PROTOCOL") + "opay-datalake/oride/oride_dw_ods/" + table_name


##----------------------------------------- 脚本 ---------------------------------------##


KeyDriverOrderTime = "driver:ort:%d:%s"

get_driver_id = '''
select id from oride_data.data_driver order by id desc limit 1
'''
insert_timerange = '''
replace into bi.driver_timerange (`Daily`,`driver_id`,`driver_onlinerange`,`driver_freerange`) values (%s,%s,%s,%s)
'''

def get_driver_timerange(curr_list, dt, rows):
    #键 algo:driver:online:%d:%s 司机id 时间yyyymmdd
    KeyDriverOnlineTime = "algo:driver:online:%d:%s"
    redis = get_redis_connection('pika_85')
    num = 0
    for id in curr_list:
        online_time_dict = redis.hgetall(KeyDriverOnlineTime % (id, dt))
        if online_time_dict:
            #print('dirver id %d, pika result %s' % (id, online_time_dict))
            online_time=0
            order_time=0
            for value in  online_time_dict.values():
                tmp_list = str(value, 'utf-8').split('|')
                online_time += int(tmp_list[0])*60
                order_time += int(tmp_list[1])*60
            free_time = online_time - order_time
            #print('driver_id:%d, online_time_total:%d' % (id, online_time))
            row = '(' + str(id) + ',' + str(online_time) + ',' + str(free_time) + ')'
            num += 1
            rows.append(row)
    logging.info('driver id %d - %d, Pid[%d], rows num %d', curr_list[0] , curr_list[-1], getpid(), num)

def get_driver_online_time(ds, **op_kwargs):
    dt = op_kwargs["ds_nodash"]
    conn = get_db_conn('timerange_conn_db')
    mcursor = conn.cursor()
    mcursor.execute(get_driver_id)
    result = mcursor.fetchone()
    conn.commit()
    mcursor.close()
    conn.close()
    processes = []
    max_driver_id = result[0]

    logging.info('max driver id %d', max_driver_id)
    id_list = [x for x in range(1, max_driver_id+1)]
    part_size = 1000
    index = 0
    manager = Manager()
    rows = manager.list([])
    while index < max_driver_id:
        p = Process(target=get_driver_timerange,
                    args=(id_list[index:index + part_size], dt, rows))
        index += part_size
        processes.append(p)
        p.start()
    for p in processes:
        p.join()
    if rows:
        query = """
            INSERT OVERWRITE TABLE oride_dw_ods.{tab_name} PARTITION (dt='{dt}')
            VALUES {value}
        """.format(dt=ds, value=','.join(rows),tab_name=table_name)
        logging.info('import_driver_online_time run sql:%s' % query)
        hive_hook = HiveCliHook()
        hive_hook.run_cli(query)

import_ods_log_oride_driver_timerange = PythonOperator(
    task_id='import_ods_log_oride_driver_timerange',
    python_callable=get_driver_online_time,
    provide_context=True,
    dag=dag
)

create_ods_log_oride_driver_timerange = HiveOperator(
    task_id='create_ods_log_oride_driver_timerange',
    hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS {tab_name} (
          driver_id int,
          driver_onlinerange int,
          driver_freerange int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
        LOCATION
        '{hdfs_path}'

    """.format(tab_name=table_name, hdfs_path=hdfs_path),
    schema='oride_dw_ods',
    dag=dag)


#主流程
def execution_data_task_id(ds,**kargs):

    #生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS

    """
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"false","true")

TaskTouchzSuccess_task= PythonOperator(
    task_id='TaskTouchzSuccess_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

create_ods_log_oride_driver_timerange >> import_ods_log_oride_driver_timerange>>TaskTouchzSuccess_task