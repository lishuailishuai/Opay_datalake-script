# coding=utf-8

from impala.dbapi import connect
from airflow.hooks.base_hook import BaseHook
from redis import StrictRedis
from ufile import config, filemanager
import MySQLdb
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
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from plugins.comwx import ComwxApi
import json
import logging
from airflow.models import Variable
import requests
import os,sys,time
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors import UFileSensor
from plugins.ModelPublicFrame import ModelPublicFrame
from utils.util import on_success_callback

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 10, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['mingze.yang@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'trigger_rule':'all_success',
}

dag = airflow.DAG(
    dag_id='odssyncdwdtemp',
    schedule_interval="20 03 * * *",
    #schedule_interval="*/2 * * * *",
    default_args=args
)


##------------------------------- 手工修改 主参数部分 -------------------

owner_name='yangmingze'
ods_db_name='oride_dw_ods'
ods_table_name='ods_sqoop_base_data_operation_driver_log_df'
dwd_db_name='oride_dw'
dwd_table_name='dwd_oride_operation_driver_log_df'

##-------------------------------------------------------------------


def get_location(hive_db, hive_table):

    """
        读取hive 表location
    """

    location = None

    hive_cursor = get_hive_cursor()
    hql = '''
        DESCRIBE FORMATTED {db}.{table} 
    '''.format(
        db=hive_db,
        table=hive_table
    )
    #logging.info(hql)
    hive_cursor.execute(hql)
    res = hive_cursor.fetchall()

    for (col_name, col_type, col_comment) in res:
        col_name = col_name.lower().strip()
        if col_name == 'location:':
            location = col_type
            break

    if location is None:
        return None

    return location

def get_table_schema(hive_db, hive_table):

    """
        读取hive 表结构
    """

    hive_cursor = get_hive_cursor()
    hql = '''
        DESCRIBE FORMATTED {db}.{table} 
    '''.format(
        db=hive_db,
        table=hive_table
    )
    logging.info(hql)
    hive_cursor.execute(hql)
    res = hive_cursor.fetchall()

    hive_schema = []

    hive_schema_exp = []

    location=None

    for (column_name, column_type, column_comment) in res:

        col_name = column_name.lower().strip()

        column_type=str(column_type).strip()

        if col_name == 'location:':

            location = column_type
            break

        #将空字符串替换给未知
        if column_comment=="" or column_comment == "from deserializer":
            column_comment="未知"

        if col_name == '# col_name' or col_name == '':

            continue

        if col_name == '# partition information':

            if column_comment is None:
                column_comment="未知"

            break

        _schema=col_name+" "+column_type+" "+"COMMENT"+" "+column_comment.replace("\\n","")+"\n"

        hive_schema_exp.append(_schema)

        hive_schema.append(col_name+",--"+column_comment)

    return hive_schema


def get_format_schema(ods_db_name,ods_table_name,dwd_db_name,dwd_table_name):

    """
        格式化表结构信息
    """

    col_list=get_table_schema(ods_db_name,ods_table_name)

    nm=len(col_list)

    a=0

    b=""

    for i in col_list:

        a=a+1

        #将最后一条数据去除','
        if a==nm:
            i=i+"'nal' as country_code,\n'{{pt}}' as dt"

        b=b+"\n"+i


    schema_str="""
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE {db}.{table} partition(country_code,dt)

    select

        {schema_sql}
        
    from {ods_db}.{ods_tab}
    where dt='{{pt}}'

    """.format(
        db=dwd_db_name,
        table=dwd_table_name,
        ods_db=ods_db_name,
        ods_tab=ods_table_name,
        schema_sql=b
        )

    return schema_str



def fun_create_table(**kwargs):

    """
        #执行建表函数
    """

    in_ods_db_name=kwargs.get('v_ods_db_name')
    in_ods_table_name=kwargs.get('v_ods_table_name')
    in_dwd_db_name=kwargs.get('v_dwd_db_name')
    in_dwd_table_name=kwargs.get('v_dwd_table_name')

    create_table_info(in_ods_db_name,in_ods_table_name,in_dwd_db_name,in_dwd_table_name)

create_table_task= PythonOperator(
    task_id='create_table_task',
    python_callable=fun_create_table,
    provide_context=True,
    op_kwargs={
        'v_ods_db_name': ods_db_name,
        'v_ods_table_name':ods_table_name,
        'v_dwd_db_name': dwd_db_name,
        'v_dwd_table_name':dwd_table_name
    },
    dag=dag
)




def create_table_info(hive_db, hive_table,dwd_db_name,dwd_table_name):

    """
        建表信息
    """
    hive_hook = HiveCliHook()

    hive_cursor = get_hive_cursor()


    hql = '''
        DESCRIBE FORMATTED {db}.{table} 
    '''.format(
        db=hive_db,
        table=hive_table
    )
    #logging.info(hql)
    hive_cursor.execute(hql)
    res = hive_cursor.fetchall()

    hive_schema = []

    hive_schema_exp = []

    location=None

    for (column_name, column_type, column_comment) in res:

        col_name = column_name.lower().strip()

        column_type=str(column_type).strip()

        if col_name == 'location:':

            location = column_type
            break

        #将空字符串替换给未知
        if column_comment=="" or column_comment == "from deserializer":
            column_comment="未知"

        if col_name == '# col_name' or col_name == '':

            continue

        if col_name == '# partition information':

            if column_comment is None:
                column_comment="未知"

            break

        _schema=col_name+" "+column_type+" "+"COMMENT"+" '"+column_comment.replace("\\n","")+"',"

        hive_schema_exp.append(_schema)

    nm=len(hive_schema_exp)

    a=0

    b=""

    for i in hive_schema_exp:

        a=a+1

        #将最后一条数据去除','
        if a==nm:
            i=i.replace(",","")

        b=b+"\n"+i

    create_str="""
    use {dwd_hive_db};
    CREATE EXTERNAL TABLE {dwd_hive_db}.{dwd_hive_table}(
    {schema_sql}
    )
    comment ""
    partitioned by (country_code string comment '二位国家码',dt string comment '分区时间')
        STORED AS orc
        LOCATION
        "ufile://opay-datalake/oride/{dwd_hive_db}/{dwd_hive_table}";
    """.format(
        dwd_hive_db=dwd_db_name,
        dwd_hive_table=dwd_table_name,
        schema_sql=b
        )

    hive_hook.run_cli(create_str)


def fun_create_template(**kwargs):

    in_owner_name=kwargs.get('v_owner_name')
    in_ods_db_name=kwargs.get('v_ods_db_name')
    in_ods_table_name=kwargs.get('v_ods_table_name')
    in_dwd_db_name=kwargs.get('v_dwd_db_name')
    in_dwd_table_name=kwargs.get('v_dwd_table_name')

    #获取ods 元数据location
    in_ods_data_path=get_location(in_ods_db_name,in_ods_table_name).replace("ufile://opay-datalake/","")

    #获取dwd 元数据location
    in_dwd_data_path=get_location(in_dwd_db_name,in_dwd_table_name)

    #获取ods表结构
    _schema=get_format_schema(in_ods_db_name,in_ods_table_name,in_dwd_db_name,in_dwd_table_name)

    template_str="""
# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': '"""+in_owner_name+"""',
        'start_date': datetime(2019, 11, 9),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( '"""+in_dwd_table_name+"""', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 


##----------------------------------------- 依赖 ---------------------------------------## 


"""+in_ods_table_name+"""_tesk = UFileSensor(
    task_id='"""+in_ods_table_name+"""_tesk',
    filepath="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str=\""""+in_ods_data_path+"""\",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------## 

db_name=\""""+in_dwd_db_name+"""\"
table_name=\""""+in_dwd_table_name+"""\"
hdfs_path=\""""+in_dwd_data_path+"""\"


##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 

def """+in_dwd_table_name+"""_sql_task(ds):

    HQL='''

    """+_schema+"""
    
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL


#熔断数据，如果数据重复，报错
# def check_key_data_task(ds):

#     cursor = get_hive_cursor()

#     #主键重复校验
#     check_sql='''
#     SELECT count(1)-count(distinct city_id) as cnt
#       FROM {db}.{table}
#       WHERE dt='{pt}'
#       and country_code in ('NG')
#     '''.format(
#         pt=ds,
#         now_day=airflow.macros.ds_add(ds, +1),
#         table=table_name,
#         db=db_name
#         )

#     logging.info('Executing 主键重复校验: %s', check_sql)

#     cursor.execute(check_sql)

#     res = cursor.fetchone()
 
#     if res[0] >1:
#         flag=1
#         raise Exception ("Error The primary key repeat !", res)
#         sys.exit(1)
#     else:
#         flag=0
#         print("-----> Notice Data Export Success ......")

#     return flag



#主流程
def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql="""+in_dwd_table_name+"""_sql_task(ds)

    logging.info('Executing: %s', _sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据
    #check_key_data_task(ds)

    #生成_SUCCESS
    \"""
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    \"""
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"true","true")
    
"""+in_dwd_table_name+"""_task= PythonOperator(
    task_id='"""+in_dwd_table_name+"""_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)


"""+in_ods_table_name+"""_tesk>>"""+in_dwd_table_name+"""_task"""

    #将模板写文件
    with open('./'+in_dwd_table_name+'.py','w') as f:
        f.write(template_str)

    #print(template_str)





create_template_task= PythonOperator(
    task_id='create_template_task',
    python_callable=fun_create_template,
    provide_context=True,
    op_kwargs={
        'v_owner_name':owner_name,
        'v_ods_db_name': ods_db_name,
        'v_ods_table_name':ods_table_name,
        'v_dwd_db_name': dwd_db_name,
        'v_dwd_table_name':dwd_table_name
    },
    dag=dag
)



create_table_task>>create_template_task