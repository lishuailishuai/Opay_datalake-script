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



class CountriesPublicFrame(object):

    def __init__(self,v_ds,v_db_name,v_table_name,v_data_hdfs_path,v_country_partition="true",v_file_type="true",v_hour=None):

        self.comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

        self.table_name=v_table_name
        self.hdfs_data_dir_str=""
        self.data_hdfs_path=v_data_hdfs_path
        self.db_name=v_db_name
        self.ds=v_ds
        self.country_partition=v_country_partition
        self.file_type=v_file_type
        self.hour=v_hour

    def get_country_code(self):

        """
            获取当前表中所有二位国家码
        """

        cursor = get_hive_cursor()
        
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


    def check_success_exist(self):

        """
            验证_SUCCESS是否执行成功
        """

        time.sleep(15)

        print("debug-> check_success_exist")

        command="hadoop fs -ls {hdfs_data_dir}/_SUCCESS>/dev/null 2>/dev/null && echo 1 || echo 0".format(hdfs_data_dir=self.hdfs_data_dir_str)

        #logging.info(command)

        out = os.popen(command, 'r')
        res = out.readlines()
        
        res = 0 if res is None else res[0].lower().strip()
        out.close()

        #判断 _SUCCESS 文件是否生成
        if res== '' or res == 'None' or res[0] == '0':
            logging.info("_SUCCESS 验证失败")

            sys.exit(1)
        
        else:
        
            logging.info("_SUCCESS 验证成功")


    def delete_exist_partition(self):

        """
            删除已有分区，保证数据唯一性
        """

        time.sleep(10)

        print("debug-> delete_exist_partition")

        #删除语句
        del_command="hadoop fs -rm -r {hdfs_data_dir}".format(hdfs_data_dir=self.hdfs_data_dir_str)

        logging.info(del_command)

        os.popen(del_command, 'r')


        time.sleep(10)

        #验证删除分区是否存在
        check_command="hadoop fs -ls {hdfs_data_dir}>/dev/null 2>/dev/null && echo 1 || echo 0".format(hdfs_data_dir=self.hdfs_data_dir_str)

        out = os.popen(check_command, 'r')

        res = out.readlines()
        
        res = 0 if res is None else res[0].lower().strip()
        out.close()

        print(res)

        #判断 删除分区是否存在
        if res== '' or res == 'None' or res[0] == '0':

            logging.info("目录删除成功")
        
        else:

            #目录存在
            logging.info("目录删除失败:"+" "+"{hdfs_data_dir}".format(hdfs_data_dir=self.hdfs_data_dir_str))

            sys.exit(1)

        
   
    def data_not_file_type_touchz(self):

        """
            非空文件 touchz _SUCCESS
        """

        try:

            print("debug-> data_not_file_type_touchz")

            mkdir_str="$HADOOP_HOME/bin/hadoop fs -mkdir -p {hdfs_data_dir}".format(hdfs_data_dir=self.hdfs_data_dir_str)

            logging.info(mkdir_str)

            os.popen(mkdir_str)

            time.sleep(10)

            succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
            logging.info(succ_str)
    
            os.popen(succ_str)
    
            logging.info("DATA EXPORT Successed ......")

            self.check_success_exist()

    
        except Exception as e:

            #self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name),'271')

            logging.info(e)

            sys.exit(1)


    def data_file_type_touchz(self):

        """
            空文件 touchz _SUCCESS
        """

        try:

            print("debug-> data_file_type_touchz") 
        
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

                time.sleep(5)

                succ_str="hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
                logging.info(succ_str)
        
                os.popen(succ_str)
        
                logging.info("DATA EXPORT Successed ......")

            self.check_success_exist()

    
        except Exception as e:

            #self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.table_name),'271')

            logging.info(e)

            sys.exit(1)


    def delete_partition(self):

        """
            删除分区调用函数
        """

        self.countries_data_dir(self.delete_exist_partition)

    def touchz_success(self):

        """
            生成 Success 调用函数
        """

        # 没有国家分区并且每个目录必须有数据才能生成 Success
        if self.country_partition.lower()=="false" and self.file_type.lower()=="true":

            self.countries_data_dir(self.data_file_type_touchz)

        # 没有国家分区并且数据为空也生成 Success
        if self.country_partition.lower()=="false" and self.file_type.lower()=="false":

            self.countries_data_dir(self.data_not_file_type_touchz)
            

        #有国家分区并且每个目录必须有数据才能生成 Success
        if self.country_partition.lower()=="true" and self.file_type.lower()=="true":

            self.countries_data_dir(self.data_file_type_touchz)
            
        
        #有国家分区并且数据为空也生成 Success
        if self.country_partition.lower()=="true" and self.file_type.lower()=="false":

            self.countries_data_dir(self.data_not_file_type_touchz)
            


    def countries_data_dir(self,object_task):

        """
        country_partition:是否有国家分区
        file_type:是否空文件也生成 success
            
        """

        try:
        
            # 没有国家分区并且每个目录必须有数据才能生成 Success
            if self.country_partition.lower()=="false" and self.file_type.lower()=="true":

                if self.hour is None:
                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=self.data_hdfs_path+"/dt="+self.ds
                else:
                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=self.data_hdfs_path+"/dt="+self.ds+"/hour="+self.hour

                object_task()

                return

            # 没有国家分区并且数据为空也生成 Success
            if self.country_partition.lower()=="false" and self.file_type.lower()=="false":

                if self.hour is None:
                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=self.data_hdfs_path+"/dt="+self.ds
                else:
                    #输出不同国家的数据路径
                    self.hdfs_data_dir_str=self.data_hdfs_path+"/dt="+self.ds+"/hour="+self.hour

                object_task()

                return


            #获取国家列表
            country_code_list=self.get_country_code()


            for country_code_word in country_code_list.split(","):


                #有国家分区并且每个目录必须有数据才能生成 Success
                if self.country_partition.lower()=="true" and self.file_type.lower()=="true":

                    if self.hour is None:

                        #输出不同国家的数据路径
                        self.hdfs_data_dir_str=self.data_hdfs_path+"/country_code="+country_code_word+"/dt="+self.ds
                    else:

                        #输出不同国家的数据路径
                        self.hdfs_data_dir_str=self.data_hdfs_path+"/country_code="+country_code_word+"/dt="+self.ds+"/hour="+self.hour


                    object_task()

                
                #有国家分区并且数据为空也生成 Success
                if self.country_partition.lower()=="true" and self.file_type.lower()=="false":


                    if self.hour is None:

                        #输出不同国家的数据路径
                        self.hdfs_data_dir_str=self.data_hdfs_path+"/country_code="+country_code_word+"/dt="+self.ds
                    else:

                        #输出不同国家的数据路径
                        self.hdfs_data_dir_str=self.data_hdfs_path+"/country_code="+country_code_word+"/dt="+self.ds+"/hour="+self.hour

                    object_task()
            
        except Exception as e:

            #self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=table_name),'271')

            logging.info(e)

            sys.exit(1)

    #
    def alter_partition(self):   

        alter_str=""

        # 没有国家分区 && 小时参数为None
        if self.country_partition.lower()=="false" and self.hour is None:

            v_par_str="dt='{ds}'".format(self.ds)

            alter_str="""
            alter table table_name drop partition({v_par});\nalter table table_name add partition({v_par});
            """.format(v_par=v_par_str)

            return alter_str
            
        # 有国家分区 && 小时参数不为None
        if self.country_partition.lower()=="false" and self.hour is not None:
            v_par_str="dt='{ds}',hour='{hour}'".format(ds=self.ds,hour=self.hour)

            alter_str="alter table table_name drop partition({v_par});\nalter table table_name add partition({v_par});".format(v_par=v_par_str)

            return alter_str

        country_code_list=self.get_country_code()

        for country_code_word in country_code_list.split(","):

            # 有国家分区 && 小时参数为None
            if self.country_partition.lower()=="true" and self.hour is None:

                v_par_str="country_code='{country_code}',dt='{ds}'".format(ds=self.ds,country_code=country_code_word)

                alter_str=alter_str+"\n"+"alter table table_name drop partition({v_par});\nalter table table_name add partition({v_par});".format(v_par=v_par_str)

            # 有国家分区 && 小时参数不为None
            if self.country_partition.lower()=="true" and self.hour is not None:

                v_par_str="country_code='{country_code}',dt='{ds}',hour='{hour}'".format(ds=self.ds,hour=self.hour,country_code=country_code_word)

                alter_str=alter_str+"\n"+"alter table table_name drop partition({v_par});\nalter table table_name add partition({v_par});".format(v_par=v_par_str)
            

        return alter_str




