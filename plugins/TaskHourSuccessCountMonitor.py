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
from plugins.DingdingAlert import DingdingAlert

"""
in_text="2:>"

str_list="00/_SUCCESS,01/_SUCCESS,02/_SUCCESS,03/_SUCCESS,04/_SUCCESS,05/_SUCCESS,06/_SUCCESS,07/_SUCCESS,08/_SUCCESS,09/_SUCCESS,10/_SUCCESS,11/_SUCCESS,12/_SUCCESS,13/_SUCCESS,14/_SUCCESS,15/_SUCCESS,16/_SUCCESS,17/_SUCCESS,18/_SUCCESS,19/_SUCCESS,20/_SUCCESS,21/_SUCCESS,22/_SUCCESS,23/_SUCCESS"

hadoop dfs -ls hdfs://warehourse/user/hive/warehouse/oride_dw_ods.db/ods_binlog_data_order_hi/dt=2019-12-12/hour=*/_SUCCESS|awk -F"hour=" '{print $2}'|tr "\n" ","|sed -e 's/,$/\n/'

"""

class TaskHourSuccessCountMonitor(object):

    def __init__(self,ds,in_text,in_data_dir):

        self.dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')

        self.nm=in_text.split(":")[0]
        self.symbol=in_text.split(":")[1]
        self.v_data_dir=in_data_dir
        
        self.syl=self.number_rebuild(self.nm)


    def get_string_list(self):

        command="hadoop dfs -ls {data_dir}/hour=*/_SUCCESS|awk -F\"hour=\" '\{print $2\}'|tr \"\n\" \",\"|sed -e 's/,$/\n/'".format(data_dir=self.v_data_dir)

        logging.info(command)

        out = os.popen(command, 'r')
        res = out.readlines()
        
        res = 0 if res is None else res[0].lower().strip()
        out.close()

        #判断 _SUCCESS 文件是否生成
        if res== '' or res == 'None' or res[0] == '0':
            logging.info("_SUCCESS list 获取失败")

            sys.exit(1)
        
        else:
        
            logging.info(res)

            return res


    def number_rebuild(self,s):
    
        n=str(s)
    
        if len(n)<2:
            s_nm="0"+n
        else:
            s_nm=n
    
        return s_nm
    
    
    def nm_less_diff(self,s):

        res=[]
    
        sylstr=str(s)+"/_SUCCESS"
    
        #每个数字前增加 1(01,101)
        v_in_number="1"+syl
    
        if int(s)<=int(v_in_number):
    
            if sylstr not in res:
                res.append(sylstr)

        return res
    
    def nm_greater_diff(self,s):

        res=[]
    
        sylstr=str(s)+"/_SUCCESS"
    
        #每个数字前增加 1(01,101)
        v_in_number="1"+syl
    
        if int(s)>=int(v_in_number):
    
            if sylstr not in res:
         
                res.append(sylstr)

        return res
    
    def HourSuccessCountMonitor(self):

        res_list=[]

        str_list=self.get_string_list()

        for i in str_list.split(","):
        
            source_nm=int("1"+i.split("/")[0])
        
            if symbol=="<":
        
                res_list=nm_less_diff(source_nm)
        
            if symbol==">":
        
                res_list=nm_greater_diff(source_nm)
        
        return len(res_list)

