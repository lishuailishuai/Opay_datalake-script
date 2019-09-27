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
from airflow.utils.trigger_rule import TriggerRule

"""
tb = [
        {"db": "oride_dw", "table": "app_oride_driver_base_d", "partition": "aaaaa", "timeout": "60"},
        {"db": "oride_dw", "table": "app_oride_order_base_d", "partition": "type=all/country_code=nal/dt=2019-09-20", "timeout": "120"}
    ]
"""


class TaskTouchzSuccess(object):

    def __init__(self):
        self.comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')


    def touchz_data_success(self,tables):

        for item in tables:

            table_name = item.get('table', None)
            hdfs_data_dir_str = item.get('hdfs_path', None)
    
    
        #判断数据文件是否为0
        line_str="$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk \'{{print $1}}\'".format(hdfs_data_dir=hdfs_data_dir_str)
    
        with os.popen(line_str) as p:
            line_num=p.read()
    
        #数据为0，发微信报警通知
        if line_num[0] == str(0):
            
            #comwx.postAppMessage('测试：DW调度系统任务 {jobname} 数据产出异常，对应时间:{pt}'.format(jobname=table_name,pt=ds), '271')
    
            logging.info("Error : {hdfs_data_dir} is empty".format(hdfs_data_dir=hdfs_data_dir_str))
            sys.exit(1)
    
        else:  
            succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=hdfs_data_dir_str)
    
            os.popen(succ_str)
    
            logging.info("DATA EXPORT Successed ......")
