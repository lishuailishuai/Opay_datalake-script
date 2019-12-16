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

tb = [
        {"start_timeThour": "2019-12-12T07", "end_dateThour": "2019-12-13T07", "depend_dir": "hdfs://warehourse/user/hive/warehouse/oride_dw_ods.db/ods_binlog_data_order_hi"}
    ]

"""

class TaskHourSuccessCountMonitor(object):

    def __init__(self,ds,v_info):

        self.dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')

        self.v_info=v_info

        self.v_data_dir=""

        self.start_timeThour=""
        self.end_dateThour=""
        self.partition=""

        self.less_res=[]
        self.greater_res=[]

        self.log_unite_dist={}

        self.start_time=""
        self.end_time=""
        
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
            分支sub函数
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

            res_list=self.less_res

            #输入日志
            self.log_unite_dist[self.end_time]=res_list
        
        if symbol==">":

            res_list=self.greater_res

            #输入日志
            self.log_unite_dist[self.start_time]=res_list

        return len(res_list)

    
    def HourSuccessCountMonitor(self):

        """
            主函数
        """

        for item in self.v_info:

            #Json 变量信息
            start_timeThour = item.get('start_timeThour', None)
            end_dateThour = item.get('end_dateThour', None)
            depend_dir= item.get('depend_dir', None)

            #开始日期和小时
            self.start_time=start_timeThour.split("T")[0]
            start_time_hour=start_timeThour.split("T")[1]

            #开始依赖小时路径
            depend_start_dir=depend_dir+"/dt="+self.start_time

            #结束日期和小时
            self.end_time=end_dateThour.split("T")[0]
            end_time_hour=end_dateThour.split("T")[1]

            #结束依赖小时路径
            depend_end_dir=depend_dir+"/dt="+self.end_time

        #统计依赖小时级分区个数
        hour_res_nm=self.summary_results(depend_start_dir,">",start_time_hour)+self.summary_results(depend_end_dir,"<",end_time_hour)

        logging.info(self.log_unite_dist)

        self.log_unite_dist={}

        #不等于24，属于依赖不成立
        if hour_res_nm!=24:

            logging.info("小时级分区文件SUCCESS 个数 {hour_res_nm} 不完整，异常退出.....".format(hour_res_nm=hour_res_nm))

            self.dingding_alert.send("小时级分区文件SUCCESS 个数 {hour_res_nm} 不完整，异常退出.....".format(hour_res_nm=hour_res_nm))

            sys.exit(1)
        else:
            pass

        

        
