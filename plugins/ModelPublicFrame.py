# -*- coding: utf-8 -*-
"""
模型脚本开发框架，包括：
调度依赖
监控任务执行是否超时
生成_SUCCESS
"""

import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from utils.connection_helper import get_hive_cursor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
import json
import logging
from airflow.models import Variable
import requests
import os,sys,time
from airflow.sensors.UFileSensor import UFileSensor
from plugins.comwx import ComwxApi


class ModelPublicFrame(execution_date):

    hive_cursor = None
    comwx = None

    def __init__(self):
        self.hive_cursor = get_hive_cursor()
        self.comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')
        self.ds_date=execution_date.strftime("%Y-%m-%d") #日期(%Y-%m-%d)
        self.ds_date_hour=execution_date.strftime("%Y-%m-%d %H") #日期(%Y-%m-%d %H)
        self.ds_date_minute=execution_date.strftime("%Y-%m-%d %H:%M") #日期(%Y-%m-%d %H:%M)
        self.ds_date_second=execution_date.strftime("%Y-%m-%d %H:%M:%S") #日期(%Y-%m-%d %H:%M:%S)

    def __del__(self):
        self.hive_cursor.close()
        self.hive_cursor = None


    #读取hive location地址
    def get_hive_location(self,db,table):

        location = None

        try:

            hql = '''
                DESCRIBE FORMATTED {db}.{table}
            '''.format(table=table, db=db)

            logging.info(hql)
            self.hive_cursor.execute(hql)
            res = self.hive_cursor.fetchall()
            for (col_name, col_type, col_comment) in res:
                col_name = col_name.lower().strip()
                if col_name == 'location:':
                    location = col_type
                    break
    
            if location is None:
                return None

            else:
                return location
    

        except Exception as e:

            self.comwx.postAppMessage('Error: '+db.table+'数据开发模板--读取hive location地址 异常','271')

            logging.info(e)

            sys.exit(1)



    """
    检查文件，协程多个调用并发执行
    """

    @asyncio.coroutine
    def task_trigger(self,command,dag_id_name, timeout):

        # timeout --时间偏移量
        # 时间偏移量= 任务正常执行结束时间(秒)+允许任务延迟的最大时间(秒)
        # 正常执行结束时间300秒+ 允许任务延迟的最大120秒=时间偏移量420 秒

        try:

            sum_timeout = 0 
            timeout_step = 120 #任务监控间隔时间(秒)
            command = command.strip()

            while sum_timeout <= int(timeout):
    
                logging.info("sum_timeout："+str(sum_timeout))
                logging.info("timeout："+str(timeout))
                logging.info(command)
    
                yield from asyncio.sleep(int(timeout_step))
    
                sum_timeout += timeout_step
                out = os.popen(command, 'r')
                res = out.readlines()
    
                #res 获取返回值_SUCCESS是否存在(1 存在)
                res = 0 if res is None else res[0].lower().strip()
                out.close()
    
                logging.info("数据标识的返回值："+str(res))
    
                #判断数据文件是否生成
                if res == '' or res == 'None' or res == '0':
                    if sum_timeout >= int(timeout):

                        self.comwx.postAppMessage(
                            'DW调度任务 {dag_id} 产出超时'.format(
                                dag_id=dag_id_name,
                                timeout=timeout
                            ),
                            '271'
                        )
    
                        logging.info("任务超时。。。。。")
                        sum_timeout=0
                else:
                    break

        except Exception as e:

            self.comwx.postAppMessage('DW调度任务 {dag_id} code 异常'.format(dag_id=dag_id_name),'271')

            logging.info(e)

            sys.exit(1)


    """
    设置任务超时监控
    @:param list 
    [{"db":"", "table":"table", "partitions":"country_code=nal", "timeout":"timeout"},]
    """
    def task_timeout_monitor(self, tables):
        commands = []
        for item in tables:
            table = item.get('table', None)
            db = item.get('db', None)
            partition = item.get('partitions', None) #分区地址
            timeout = item.get('timeout', None)

            if table is None or db is None or partition is None or timeout is None:
                return None

            #表的location
            location = None

            # hql = '''
            #     DESCRIBE FORMATTED {db}.{table}
            # '''.format(table=table, db=db)
            # logging.info(hql)
            # self.hive_cursor.execute(hql)
            # res = self.hive_cursor.fetchall()
            # for (col_name, col_type, col_comment) in res:
            #     col_name = col_name.lower().strip()
            #     if col_name == 'location:':
            #         location = col_type
            #         break

            # if location is None:
            #     return None

            #读取hive location地址
            location=get_hive_location(db,table)

            commands.append({
                'cmd': '''
                        hadoop fs -ls {path}/{partition}/dt={pt}/_SUCCESS >/dev/null 2>/dev/null && echo 1 || echo 0
                    '''.format(
                        pt=self.ds_date,
                        path=location,
                        partition=partition
                    ),
                'partition': partition,
                'timeout': timeout,
                'table': table
                }
            )

        loop = asyncio.get_event_loop()
        tasks = [self.task_trigger(items['cmd'], items['table'], items['timeout']) for items in commands]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

    """
    任务完成标识_SUCCESS
    @:param list 
    [{"db":"", "table":"table", "partitions":"country_code=nal"]
    """
    def task_touchz_success(self,tables):

      
        #表的location
        location = None

        try:

            for item in tables:
    
                table = item.get('table', None)
                db = item.get('db', None)
                partition = item.get('partitions', None) #分区地址

            #读取hive location地址
            location=get_hive_location(db,table)

            hdfs_data_dir_str=location+'/'+partition+'/dt='+self.ds_date
        
            #判断数据文件是否为0
            line_str="$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk \'{{print $1}}\'".format(hdfs_data_dir=hdfs_data_dir_str)
    
            logging.info(line_str)
        
            with os.popen(line_str) as p:
                line_num=p.read()
        
            #数据为0，发微信报警通知
            if line_num[0] == str(0):
                
                self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常，对应时间:{pt}'.format(jobname=table,pt=self.ds_date), '271')
        
                logging.info("Error : {hdfs_data_dir} is empty".format(hdfs_data_dir=hdfs_data_dir_str))
                sys.exit(1)
        
            else:  
                succ_str="$HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS".format(hdfs_data_dir=hdfs_data_dir_str)
    
                logging.info(succ_str)
        
                os.popen(succ_str)
        
                logging.info("DATA EXPORT Successed ......")
    
    
        except Exception as e:

            self.comwx.postAppMessage('DW调度系统任务 {jobname} 数据产出异常，对应时间:{pt}'.format(jobname=table,pt=self.ds_date),'271')

            logging.info(e)

            sys.exit(1)

    """
    任务完成标识_SUCCESS
    @:param list 
    [{"db":"db_name", "table":"table_name", "partitions":"country_code=nal"]
    """

    def tesk_dependence(self,tables):

        for item in tables:

            #读取 db、table、partition
            table = item.get('table', None)
            db = item.get('db', None)
            partition = item.get('partitions', None)
        
            if table is None or db is None or partition is None or job_date is None:
                return None
        
            location = None
        
            # hql = '''
            #     DESCRIBE FORMATTED {db}.{table}
            # '''.format(table=table, db=db)

            # self.hive_cursor.execute(hql)
            # res = self.hive_cursor.fetchall()
    
            # for (col_name, col_type, col_comment) in res:
            #     col_name = col_name.lower().strip()
            #     if col_name == 'location:':
            #         location = col_type
            #         break
        
            # if location is None:
            #     return None

            #读取hive location地址
            location=get_hive_location(db,table)
    
            #替换原有bucket
            location=location.replace('ufile://opay-datalake/','')
    
            #task_id 名称
            task_id_flag=table+"_task"
    
            #区分ods的依赖路径
            if db[-3:].lower()=='ods' or db[-2:].lower()=='bi':
    
                # 配置依赖关系(前一天分区)
                dependence_task_flag= HivePartitionSensor(
                    task_id='dependence_{task_id_name}'.format(task_id_name=task_id_flag),
                    table=table,
                    partition="dt='"+self.ds_date+"'",
                    schema=db,
                    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
                    dag=dag
                )
    
            else:
        
                # 配置依赖关系(前一天分区)
                dependence_task_flag = UFileSensor(
                    task_id='{task_id_name}'.format(task_id_name=task_id_flag),
                    filepath='{hdfs_path_name}/{partition_name}/dt={pt}/_SUCCESS'.format(
                        hdfs_path_name=location,
                        partition_name=partition,
                        pt=self.ds_date
                    ),
                    bucket_name='opay-datalake',
                    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
                    dag=dag
                    )
    
            # 加入调度队列
            dependence_task_flag    