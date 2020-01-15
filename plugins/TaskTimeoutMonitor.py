# -*- coding: utf-8 -*-
"""
监控任务执行是否超时
"""

import airflow
from utils.connection_helper import get_hive_cursor
import logging
import os,sys,time
import asyncio
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow import AirflowException
from airflow.models import DAG, TaskInstance, BaseOperator
from plugins.DingdingAlert_dev import DingdingAlert_dev

from html.parser import HTMLParser


"""
监控数据表分区产出的_SUCCESS文件
调用示例:
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor

def test_t11(**op_kwargs):
    sub = TaskTimeoutMonitor()
    tb = [
        {"dag":dag,"db": "oride_dw", "table": "app_oride_driver_base_d", "partition": "aaaaa", "timeout": "60"},
        {"dag":dag,"db": "oride_dw", "table": "app_oride_order_base_d", "partition": "type=all/country_code=nal/dt=2019-09-20", "timeout": "120"}
    ]

    sub.set_task_monitor(tb)

t1 = PythonOperator(
    task_id='test_t1',
    python_callable=test_t11,
    provide_context=True,
    dag=dag
)

t1
"""


class TaskTimeoutMonitor(object):

    hive_cursor = None
    dingding_alert = None

    def __init__(self):
        self.hive_cursor = get_hive_cursor()
        self.dingding_alert = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')

        #self.dingding_alert = DingdingAlert_dev('https://oapi.dingtalk.com/robot/send?access_token=c08440c8e569bb38ec358833f9d577b7638af5aaefbd55e3fd748b798fecc4d4')

        self.alert_url="http://8.208.14.165:8080/admin/airflow/tree?dag_id="

        self.owner_name=None

        self.hdfs_dir_name=None

    def __del__(self):
        self.hive_cursor.close()
        self.hive_cursor = None

    """
    检查文件，协程多个调用并发执行
    """

    #@asyncio.coroutine
    def task_trigger(self,command,dag_id_name, timeout):

        # timeout --时间偏移量
        # 时间偏移量= 任务正常执行结束时间(秒)+允许任务延迟的最大时间(秒)
        # 正常执行结束时间300秒+ 允许任务延迟的最大120秒=时间偏移量420 秒

        try:

            sum_timeout = 0 
            timeout_step = 120 #任务监控间隔时间(秒)
            command = command.strip()

            #次数
            num=0

            while sum_timeout <= int(timeout):
    
                logging.info("sum_timeout："+str(sum_timeout))
                logging.info("timeout："+str(timeout))
                logging.info(command)

                time.sleep(timeout_step)
    
                #yield from asyncio.sleep(int(timeout_step))
    
                sum_timeout += timeout_step
                out = os.popen(command, 'r')
                res = out.readlines()
    
                #res 获取返回值_SUCCESS是否存在(1 存在)
                res = 0 if res is None else res[0].lower().strip()
                out.close()
    
                logging.info("数据标识的返回值："+str(res))
    
                #判断数据文件是否生成
                if res == '' or res == 'None' or res == '0':

                    url="""
                        {alter_url}{dag_id}
                    """.format(alter_url=self.alert_url,dag_id=dag_id_name)

                    if sum_timeout >= int(timeout):

                        #换算分钟
                        format_date=int(int(timeout)/60)
                    	
                        num=num+1

                        self.dingding_alert.markdown_send("【及时性预警】",

                            "DW <font color=#000000 size=3 face=\"微软雅黑\">【监控】</font><font color=#FF0000 size=3 face=\"微软雅黑\">及时性预警 </font>\n\n"+

                            "**超时任务:** \n\n &nbsp;&nbsp;[{dag_id}]({url}) \n\n".format(
                                dag_id="dwd_oride_algo_driver_score_init_info_dischedule",
                                url=url)+

                            "**负 责 人 :** &nbsp;&nbsp;{owner_name} \n\n".format(
                                owner_name=self.owner_name)+

                            "**等待路径:** &nbsp;&nbsp;{hdfs_dir_name} \n\n".format(
                                hdfs_dir_name=self.hdfs_dir_name)+

                            "**预留时间:** &nbsp;&nbsp;{timeout} 分钟 \n\n".format(timeout=str(format_date))+

                            "**预警次数:** &nbsp;&nbsp;{num}".format(num=num)
                        )
                        
                        logging.info("任务超时... ...")

                        sum_timeout=0
                else:
                    num=0
                    break

        except Exception as e:

            logging.info(e)

            sys.exit(1)


    """
    设置任务监控
    @:param list 
    [{"db":"", "table":"table", "partition":"partition", "timeout":"timeout"},]
    """
    def set_task_monitor(self, tables):
        commands = []
        for item in tables:
            #
            db = item.get('db', None)
            partition = item.get('partition', None)
            timeout = item.get('timeout', None)
            dag=item.get('dag', None)

            if dag:

                table=dag.dag_id

                self.owner_name=dag.default_args.get("owner")

            else:

                self.owner_name="Null"

                table = item.get('table', None)


            if table is None or db is None or partition is None or timeout is None:
                return None

            location = None
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

            self.hdfs_dir_name=location+"/"+partition+"/_SUCCESS"

            commands.append({
                'cmd': '''
                        hadoop fs -ls {path}/{partition}/_SUCCESS >/dev/null 2>/dev/null && echo 1 || echo 0
                    '''.format(
                        timeout=timeout,
                        path=location,
                        partition=partition
                    ),
                'partition': partition,
                'timeout': timeout,
                'table': table
                }
            )

        for items in commands:

            self.task_trigger(items['cmd'], items['table'], items['timeout']) 
