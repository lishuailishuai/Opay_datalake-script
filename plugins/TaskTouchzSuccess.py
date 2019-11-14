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
        self.db_name=""
        self.ds=""


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
                
                self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name), '271')

                logging.info("Error : {hdfs_data_dir} is empty".format(hdfs_data_dir=self.hdfs_data_dir_str))
                sys.exit(1)
        
            else:  
                succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
                logging.info(succ_str)
        
                os.popen(succ_str)

                time.sleep(10)
        
                logging.info("DATA EXPORT Successed ......")

    
        except Exception as e:

            self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name),'271')

            logging.info(e)

            sys.exit(1)


    def get_country_code(self):

        cursor = get_hive_cursor()
    
        #获取二位国家码
        get_sql='''
    
        select concat_ws(',',collect_set(country_code)) as country_code from {db}.{table} WHERE dt='{pt}'
    
        '''.format(
            pt=self.ds,
            table=self.table_name,
            db=self.db_name
            )
    
        cursor.execute(get_sql)
    
        res = cursor.fetchone()

        if len(res[0]) >1:
            country_code_list=res[0]
    
            logging.info('Executing 二位国家码: %s', country_code_list)
    
        else:
    
            country_code_list="nal"
    
            logging.info('Executing 二位国家码为空，赋予默认值 %s', country_code_list)
    
        return country_code_list

   
    def data_not_file_type_touchz(self):

        """
            非空文件 touchz _SUCCESS
        """

        try:
        
            succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
            logging.info(succ_str)
    
            os.popen(succ_str)
    
            logging.info("DATA EXPORT Successed ......")

    
        except Exception as e:

            self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name),'271')

            logging.info(e)

            sys.exit(1)


    def data_file_type_touchz(self):

        """
            空文件 touchz _SUCCESS
        """

        try:
        
            #判断数据文件是否为0
            line_str="$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk \'{{print $1}}\'".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
            logging.info(line_str)
        
            with os.popen(line_str) as p:
                line_num=p.read()
        
            #数据为0，发微信报警通知
            if line_num[0] == str(0):
                
                self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name), '271')

                logging.info("Error : {hdfs_data_dir} is empty".format(hdfs_data_dir=self.hdfs_data_dir_str))
                sys.exit(1)
        
            else:  
                succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
                logging.info(succ_str)
        
                os.popen(succ_str)

                #time.sleep(10)
        
                logging.info("DATA EXPORT Successed ......")

    
        except Exception as e:

            self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name),'271')

            logging.info(e)

            sys.exit(1)



    def countries_touchz_success(self,ds,db_name,table_name,data_hdfs_path,country_partition="true",file_type="true",hour=None):

        """
        country_partition:是否有国家分区
        file_type:是否空文件也生成 success
            
        """

        try:

            self.db_name=db_name
            self.ds=ds
            self.table_name=table_name

            #获取国家列表
            country_code_list=self.get_country_code()


            # 没有国家分区并且每个目录必须有数据才能生成 Success
            if country_partition.lower()=="false" and file_type.lower()=="true":

                if hour is None:
                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=data_hdfs_path+"/dt="+self.ds
                else:
                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=data_hdfs_path+"/dt="+self.ds+"/hour="+hour

                print(self.hdfs_data_dir_str)

                sys.exit(1)

                self.data_not_file_type_touchz()

            # 没有国家分区并且数据为空也生成 Success
            if country_partition.lower()=="false" and file_type.lower()=="false":

                #输出不同国家的数据路径
                self.hdfs_data_dir_str=data_hdfs_path+"/dt="+self.ds

                self.data_file_type_touchz()



            for country_code_word in country_code_list.split(","):


                #有国家分区并且每个目录必须有数据才能生成 Success
                if country_partition.lower()=="true" and file_type.lower()=="true":

                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=data_hdfs_path+"/country_code="+country_code_word+"/dt="+self.ds

                    self.data_not_file_type_touchz()

                
                #有国家分区并且数据为空也生成 Success
                if country_partition.lower()=="true" and file_type.lower()=="false":

                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=data_hdfs_path+"/country_code="+country_code_word+"/dt="+self.ds

                    self.data_not_file_type_touchz()

            
        except Exception as e:

            self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=table_name),'271')

            logging.info(e)

            sys.exit(1)

            