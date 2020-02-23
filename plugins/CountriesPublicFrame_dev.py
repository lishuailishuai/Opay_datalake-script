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
from utils.get_local_time import GetLocalTime



class CountriesPublicFrame_dev(object):

    def __init__(self,args):

        #正式环境
        #self.dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')

        #测试环境
        self.dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=c08440c8e569bb38ec358833f9d577b7638af5aaefbd55e3fd748b798fecc4d4')

        self.alert_url="http://8.208.14.165:8080/admin/airflow/tree?dag_id="

        self.items=args

        self.dag=None

        self.dag_id=None

        self.owner_name=None

        self.v_table_name=None
        self.hdfs_data_dir_str=None
        self.v_data_oss_path=None
        self.v_db_name=None
        self.v_is_country_partition=None
        self.v_is_result_force_exist=None
        self.utc_ds=None
        self.utc_hour=None
        self.v_is_countries_online=None
        self.v_del_flag=0
        self.v_frame_type=None

        self.v_country_code_map=None

        self.country_code_list=None

        self.time_offset=0

        self.get_mian_argument()

        self.get_country_code()

    def get_mian_argument(self):

        """
            获取主类参数
        """

        for item in self.items:

            #airflow dag
            self.dag=item.get('dag', None)
           
            #是否开通多国家业务(默认true)
            self.v_is_countries_online=item.get('is_countries_online', "true")

            #数据库名称
            self.v_db_name=item.get('db_name', None)

            #表名称
            self.v_table_name=item.get('table_name', None)

            #oss 路径
            self.v_data_oss_path=item.get('data_oss_path', None)

            #是否有国家分区(默认true)
            self.v_is_country_partition=item.get('is_country_partition', "true")

            #数据文件是否强制存在(默认true)
            self.v_is_result_force_exist=item.get('is_result_force_exist', "true")

            #脚本执行时间(%Y-%m-%d %H:%M:%S)
            self.v_execute_time=item.get('execute_time', None)

            #脚本执行UTC日期
            self.utc_ds=(datetime.strptime(self.v_execute_time,'%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d').strip()

            #脚本执行UTC小时
            self.utc_hour=(datetime.strptime(self.v_execute_time,'%Y-%m-%d %H:%M:%S')).strftime('%H').strip()

            #是否开启小时级任务(默认false)
            self.v_is_hour_task=item.get('is_hour_task', "false")

            #框架类型(utc[默认],local[使用本地时间产出])
            self.v_frame_type=item.get('frame_type', "utc")


        if self.dag:

            self.dag_id=self.dag.dag_id

            self.owner_name=self.dag.default_args.get("owner")

        else:

            self.owner_name="Null"

            self.dag_id = self.v_table_name


    def get_country_code(self):

        """
            获取当前表中所有二位国家码
        """

        if self.v_is_countries_online.lower()=="false":

            self.country_code_list="nal"

        if self.v_is_countries_online.lower()=="true":

            self.v_country_code_map = eval(Variable.get("country_code_dim"))

            s=list(self.v_country_code_map.keys())

            self.country_code_list=",".join(s)

            
    def check_success_exist(self):

        """
            验证_SUCCESS是否执行成功
        """

        time.sleep(15)

        logging.info("Check_Success_Exist")

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

        logging.info("Delete_Exist_Partition")

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
        
   
    def data_not_file_type_touchz(self):

        """
            非空文件 touchz _SUCCESS
        """

        try:

            logging.info("Data_Not_File_Type_Touchz")

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

            #self.dingding_alert.send('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.v_table_name))

            logging.info(e)

            sys.exit(1)


    def data_file_type_touchz(self):

        """
            创建 _SUCCESS
        """

        try:

            logging.info("Data_File_Type_Touchz") 
        
            #判断数据文件是否为0
            line_str="$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk \'{{print $1}}\'".format(hdfs_data_dir=self.hdfs_data_dir_str)
    
            logging.info(line_str)
        
            with os.popen(line_str) as p:
                line_num=p.read()
        
            #数据为0，发微信报警通知
            if line_num[0] == str(0):
                
                #self.dingding_alert.send('Test 调度系统任务 {jobname} 数据产出异常'.format(jobname=self.v_table_name))

                self.dingding_monitor()

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

            #self.dingding_alert.send('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=self.v_table_name))

            logging.info(e)

            sys.exit(1)

    def delete_partition(self):

        """
            删除分区调用函数
        """

        self.v_del_flag=1

        #没有国家分区
        if self.v_is_country_partition.lower()=="false":

            self.not_exist_country_code_data_dir(self.delete_exist_partition)


        #有国家分区
        if self.v_is_country_partition.lower()=="true":

            self.exist_country_code_data_dir(self.delete_exist_partition)

        self.v_del_flag=0


    def touchz_success(self):

        """
            生成 Success 函数
        """

         # 没有国家分区并且每个目录必须有数据才能生成 Success
        if self.v_is_country_partition.lower()=="false" and self.v_is_result_force_exist.lower()=="true":

            self.not_exist_country_code_data_dir(self.data_file_type_touchz)

        # 没有国家分区并且数据为空也生成 Success
        if self.v_is_country_partition.lower()=="false" and self.v_is_result_force_exist.lower()=="false":

            self.not_exist_country_code_data_dir(self.data_not_file_type_touchz)


        #有国家分区并且每个目录必须有数据才能生成 Success
        if self.v_is_country_partition.lower()=="true" and self.v_is_result_force_exist.lower()=="true":

            self.exist_country_code_data_dir(self.data_file_type_touchz)
        
        
        #有国家分区并且数据为空也生成 Success
        if self.v_is_country_partition.lower()=="true" and self.v_is_result_force_exist.lower()=="false":

            self.exist_country_code_data_dir(self.data_not_file_type_touchz)


    #没有国家码分区
    def not_exist_country_code_data_dir(self,object_task):

        """
        country_partition:是否有国家分区
        file_type:是否空文件也生成 success
            
        """

        try:

            #没有小时级分区
            if self.v_is_hour_task.lower()=="false":
                #输出不同国家的数据路径(没有小时级分区)
                self.hdfs_data_dir_str=self.v_data_oss_path+"/dt="+self.utc_ds
            else:
                #输出不同国家的数据路径(有小时级分区)
                self.hdfs_data_dir_str=self.v_data_oss_path+"/dt="+self.utc_ds+"/hour="+self.utc_hour
        
            # 没有国家分区并且每个目录必须有数据才能生成 Success
            if self.v_is_country_partition.lower()=="false" and self.v_is_result_force_exist.lower()=="true":

                object_task()

                return

            # 没有国家分区并且数据为空也生成 Success
            if self.v_is_country_partition.lower()=="false" and self.v_is_result_force_exist.lower()=="false":

                object_task()

                return

        except Exception as e:

            #self.dingding_alert.send('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=table_name))

            logging.info(e)

            sys.exit(1)

    def exist_country_code_data_dir(self,object_task):

        """
        country_partition:是否有国家分区
        file_type:是否空文件也生成 success
            
        """

        try:

            #获取国家列表

            for country_code_word in self.country_code_list.split(","):

                if country_code_word.lower()=='nal':
                    country_code_word=country_code_word.lower()

                else:
                    country_code_word=country_code_word.upper()


                #没有小时级分区
                if self.v_is_hour_task.lower()=="false":

                    #输出不同国家的数据路径(没有小时级分区)
                    self.hdfs_data_dir_str=self.v_data_oss_path+"/country_code="+country_code_word+"/dt="+self.utc_ds

                #(多国家)UTC 小时分区
                if self.v_is_hour_task.lower()=="true" and self.v_frame_type.lower()=="utc":

                    #输出不同国家(UTC时间)的数据路径(UTC 小时级分区)
                    self.hdfs_data_dir_str=self.v_data_oss_path+"/country_code="+country_code_word+"/dt="+self.utc_ds+"/hour="+self.utc_hour

                #(多国家)Local 小时分区
                if self.v_is_hour_task.lower()=="true" and self.v_frame_type.lower()=="local":

                    v_utc_time='{v_sys_utc}'.format(v_sys_utc=self.utc_ds+" "+self.utc_hour)
        
                    #国家本地日期
                    v_local_date=GetLocalTime('{v_utc_time}'.format(v_utc_time=v_utc_time),country_code_word,self.time_offset)["date"]
        
                    #国家本地小时
                    v_local_hour=GetLocalTime('{v_utc_time}'.format(v_utc_time=v_utc_time),country_code_word,self.time_offset)["hour"]

                    #输出不同国家(本地时间)的数据路径(Local 小时级分区)
                    self.hdfs_data_dir_str=self.v_data_oss_path+"/country_code="+country_code_word+"/dt="+v_local_date+"/hour="+v_local_hour

                #没有开通多国家业务(国家码默认nal)
                if self.v_is_country_partition.lower()=="true" and self.v_is_countries_online.lower()=="false":

                    #必须有数据才可以生成Success 文件
                    if self.v_is_result_force_exist.lower()=="true":

                        object_task()

                    #数据为空也生成 Success 文件
                    if self.v_is_result_force_exist.lower()=="false":
                        
                        object_task()

                
                #开通多国家业务
                if self.v_is_country_partition.lower()=="true" and self.v_is_countries_online.lower()=="true":

                    #刚刚开国的国家(按照false处理)
                    if self.v_country_code_map[country_code_word].lower()=="new":

                        #删除多国家分区使用
                        if self.v_del_flag==1:

                            object_task()
                            continue 

                        else:
                            self.data_not_file_type_touchz()

                            continue

                    #必须有数据才可以生成Success 文件
                    if self.v_is_result_force_exist.lower()=="true":

                        #删除多国家分区使用
                        if self.v_del_flag==1:

                            object_task()
                            continue

                        #在必须有数据条件下：国家是nal时，数据可以为空 
                        if country_code_word=="nal":
                            self.data_not_file_type_touchz()

                        else:
                            
                            object_task()


                    #数据为空也生成 Success 文件
                    if self.v_is_result_force_exist.lower()=="false":
                        
                        object_task()

                   
        except Exception as e:

            #self.dingding_alert.send('DW调度系统任务 {jobname} 数据产出异常'.format(jobname=table_name))

            logging.info(e)

            sys.exit(1)

    # alter 语句(包含单国家、多国家)
    def alter_partition(self):   

        alter_str=""

        # 没有国家分区 && 小时参数为None
        if self.v_is_country_partition.lower()=="false" and self.v_is_hour_task.lower()=="false":

            v_par_str="dt='{ds}'".format(ds=self.utc_ds)

            alter_str="alter table {db}.{table_name} drop partition({v_par});\n alter table {db}.{table_name} add partition({v_par});".format(v_par=v_par_str,table_name=self.v_table_name,db=self.v_db_name)

            return alter_str
            
        # 没有国家分区 && 小时参数不为None
        if self.v_is_country_partition.lower()=="false" and self.v_is_hour_task.lower()=="true":

            v_par_str="dt='{ds}',hour='{hour}'".format(ds=self.utc_ds,hour=self.utc_hour)

            alter_str="alter table {db}.{table_name} drop partition({v_par});\n alter table {db}.{table_name} add partition({v_par});".format(v_par=v_par_str,table_name=self.v_table_name,db=self.v_db_name)

            return alter_str

        #有国家分区
        for country_code_word in self.country_code_list.split(","):

            # 有国家分区 && 小时参数为None
            if self.v_is_country_partition.lower()=="true" and self.v_is_hour_task.lower()=="false":

                v_par_str="country_code='{country_code}',dt='{ds}'".format(ds=self.utc_ds,country_code=country_code_word)

                alter_str=alter_str+"\n"+"alter table {db}.{table_name} drop partition({v_par});\n alter table {db}.{table_name} add partition({v_par});".format(v_par=v_par_str,table_name=self.v_table_name,db=self.v_db_name)

            # 多国家(utc)分区 && 小时参数不为None && utc分区
            if self.v_is_country_partition.lower()=="true" and self.v_is_hour_task.lower()=="true" and self.v_frame_type.lower()=="utc":

                v_par_str="country_code='{country_code}',dt='{ds}',hour='{hour}'".format(ds=self.utc_ds,hour=self.utc_hour,country_code=country_code_word)

                alter_str=alter_str+"\n"+"alter table {db}.{table_name} drop partition({v_par});\n alter table {db}.{table_name} add partition({v_par});".format(v_par=v_par_str,table_name=self.v_table_name,db=self.v_db_name)
            
            # 多国家(本地时间)分区 && 小时参数不为None && 本地时间
            if self.v_is_country_partition.lower()=="true" and self.v_is_hour_task.lower()=="true" and self.v_frame_type.lower()=="local":

                #yyyy-MM-dd hh
                v_utc_time='{v_sys_utc}'.format(v_sys_utc=self.utc_ds+" "+self.utc_hour)
        
                #国家本地日期
                v_local_date=GetLocalTime('{v_utc_time}'.format(v_utc_time=v_utc_time),country_code_word,self.time_offset)["date"]
        
                #国家本地小时
                v_local_hour=GetLocalTime('{v_utc_time}'.format(v_utc_time=v_utc_time),country_code_word,self.time_offset)["hour"]

                #表分区，时间是本地时间
                v_par_str="country_code='{country_code}',dt='{ds}',hour='{hour}'".format(ds=v_local_date,hour=v_local_hour,country_code=country_code_word)

                alter_str=alter_str+"\n"+"alter table {db}.{table_name} drop partition({v_par});\n alter table {db}.{table_name} add partition({v_par});".format(v_par=v_par_str,table_name=self.v_table_name,db=self.v_db_name)

        return alter_str

    def dingding_monitor(self):

        url="""
                {alter_url}{dag_id}
            """.format(alter_url=self.alert_url,dag_id=self.dag_id)

        #换算分钟
        #format_date=int(int(timeout)/60)

        print(self.dag_id)

        self.dingding_alert.markdown_send("【及时性预警】",

            "Test <font color=#000000 size=3 face=\"微软雅黑\">【监控】</font><font color=#FF0000 size=3 face=\"微软雅黑\">及时性预警 </font>\n\n"+

            "**超时任务:** \n\n &nbsp;&nbsp;[{dag_id}]({url}) \n\n".format(
                dag_id=self.dag_id,
                url=url)+

            "**负 责 人 :** &nbsp;&nbsp;{owner_name} \n\n".format(
                owner_name=self.owner_name)+

            "**等待路径:** &nbsp;&nbsp;{hdfs_dir_name} \n\n".format(
                hdfs_dir_name=self.hdfs_data_dir_str)
        )
        
        logging.info("任务超时... ...")



