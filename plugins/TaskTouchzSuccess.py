import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
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
        {"table":"test_airflow_test".format(dag_name=dag_ids),"hdfs_path": "{hdfs_path}/country_code=lan/dt={pt}".format(pt=ds,hdfs_path=hdfspath)}
    ]

"""


class TaskTouchzSuccess(object):

    def __init__(self):
        self.comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')
        self.table_name=""
        self.hdfs_data_dir_str=""


    def set_touchz_success(self,tables):

        try:

            for item in tables:
    
                self.table_name = item.get('table', None)
                self.hdfs_data_dir_str = item.get('hdfs_path', None)
        
            #判断数据文件是否为0
            line_str="$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk \'{{print $1}}\'".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
            logging.info(line_str)
        
            with os.popen(line_str) as p:
                line_num=p.read()
        
            #数据为0，发微信报警通知
            if line_num[0] == str(0):
                
                self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常，对应时间:{pt}'.format(jobname=self.table_name,pt=ds), '271')
        
                logging.info("Error : {hdfs_data_dir} is empty".format(hdfs_data_dir=self.hdfs_data_dir_str))
                sys.exit(1)
        
            else:  
                succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
                logging.info(succ_str)
        
                os.popen(succ_str)

                time.sleep(10)
        
                logging.info("DATA EXPORT Successed ......")

    
        except Exception as e:

            self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常，对应时间:{pt}'.format(jobname=self.table_name,pt=ds),'271')

            logging.info(e)

            sys.exit(1)

            