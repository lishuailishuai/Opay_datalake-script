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

tb = [
        {"start_timeThour": "2019-12-12T07", "end_dateThour": "2019-12-13T07", "partition": "/country_code=nal/dt=2019-09-20/"}
    ]


str_list="00/_SUCCESS,01/_SUCCESS,02/_SUCCESS,03/_SUCCESS,04/_SUCCESS,05/_SUCCESS,06/_SUCCESS,07/_SUCCESS,08/_SUCCESS,09/_SUCCESS,10/_SUCCESS,11/_SUCCESS,12/_SUCCESS,13/_SUCCESS,14/_SUCCESS,15/_SUCCESS,16/_SUCCESS,17/_SUCCESS,18/_SUCCESS,19/_SUCCESS,20/_SUCCESS,21/_SUCCESS,22/_SUCCESS,23/_SUCCESS"

hadoop dfs -ls hdfs://warehourse/user/hive/warehouse/oride_dw_ods.db/ods_binlog_data_order_hi/dt=2019-12-12/hour=*/_SUCCESS|awk -F"hour=" '{print $2}'|tr "\n" ","|sed -e 's/,$/\n/'

"""

class TaskHourSuccessCountMonitor(object):

    def __init__(self,ds,v_info):

        self.dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')

        self.v_info=v_info

        self.nm=""
        self.v_data_dir=""

        self.start_timeThour=""
        self.end_dateThour=""
        self.partition=""

        self.less_res=[]
        self.greater_res=[]
        
    def get_partition_list(self):

        """
            获取小时级分区所有_SUCCESS文件
        """

        command="hadoop dfs -ls {data_dir}/hour=*/_SUCCESS|awk -F\"hour=\" \'{{print $2}}\'|tr \"\\n\" \",\"|sed -e 's/,$/\\n/'".format(data_dir=self.v_data_dir)

        logging.info(command)

        out = os.popen(command, 'r')
        res = out.readlines()
        
        res[0] = 0 if res[0] is None else res[0].lower().strip()
        out.close()

        #判断 _SUCCESS 文件是否生成
        if res[0]== '' or res[0] == 'None' or res[0] == '0':
            logging.info("_SUCCESS list 获取失败")

            sys.exit(1)
        
        else:

            return res[0]

    def number_rebuild(self,s):

        """
            将基准小时格式进行格式化(1->01)
        """
    
        n=str(s)
    
        if len(n)<2:
            s_nm="0"+n
        else:
            s_nm=n
       
        return s_nm
    
    def nm_less_diff(self,s):

        """
            小于时间范围的判断
        """
    
        sylstr=str(s)+"/_SUCCESS"
    
        #每个数字前增加 1(01->101)
        v_in_number="1"+self.syl
    
        if int(s)<=int(v_in_number):
    
            if sylstr not in self.less_res:
                self.less_res.append(sylstr)

    
    def nm_greater_diff(self,s):

        """
            大于时间范围的判断
        """    

        sylstr=str(s)+"/_SUCCESS"
    
        #每个数字前增加 1(01->101)
        v_in_number="1"+self.syl
    
        if int(s)>=int(v_in_number):
    
            if sylstr not in self.greater_res:
         
                self.greater_res.append(sylstr)


    def summary_results(self,depend_data_dir,symbol,start_hour):

        """
            执行函数
        """

        #对比符号("<" and ">")
        symbol=symbol.strip()

        #数据目录分区地址
        self.v_data_dir=depend_data_dir.strip()

        self.syl=self.number_rebuild(start_hour)

        res_list=[]

        #获取分区列表
        partition_list=self.get_partition_list()

        for i in partition_list.split(","):
        
            #将原有小时分区，前面加1，进行数据对比
            source_nm=int("1"+i.split("/")[0])
        
            if symbol=="<":
        
                self.nm_less_diff(source_nm)
        
            if symbol==">":
        
                self.nm_greater_diff(source_nm)


        if symbol=="<":
            print("less_res")
            res_list=self.less_res
        
        if symbol==">":
            print("greater_res")
            res_list=self.greater_res

        print(res_list)

        return len(res_list)

    
    def HourSuccessCountMonitor(self):

        for item in self.v_info:

            #Json 变量信息
            start_timeThour = item.get('start_timeThour', None)
            end_dateThour = item.get('end_dateThour', None)
            depend_dir= item.get('depend_dir', None)

            #开始日期和小时
            start_time=start_timeThour.split("T")[0]
            start_time_hour=start_timeThour.split("T")[1]

            #开始依赖小时路径
            depend_start_dir=depend_dir+"/dt="+start_time

            #结束日期和小时
            end_time=end_dateThour.split("T")[0]
            end_time_hour=end_dateThour.split("T")[1]

            #结束依赖小时路径
            depend_end_dir=depend_dir+"/dt="+end_time


        res=self.summary_results(depend_start_dir,">",start_time_hour)+self.summary_results(depend_end_dir,"<",end_time_hour)

        print(res)

        
