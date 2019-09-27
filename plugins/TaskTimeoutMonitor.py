# -*- coding: utf-8 -*-
"""
监控任务执行是否超时
"""

import airflow
from utils.connection_helper import get_hive_cursor
from plugins.comwx import ComwxApi
import logging
import os,sys
import asyncio
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow import AirflowException
from airflow.models import DAG, TaskInstance, BaseOperator


"""
监控数据表分区产出的_SUCCESS文件
调用示例:
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor

def test_t11(**op_kwargs):
    sub = TaskTimeoutMonitor()
    tb = [
        {"db": "oride_dw", "table": "app_oride_driver_base_d", "partition": "aaaaa", "timeout": "60"},
        {"db": "oride_dw", "table": "app_oride_order_base_d", "partition": "type=all/country_code=nal/dt=2019-09-20", "timeout": "120"}
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
    comwx = None

    def __init__(self):
        self.hive_cursor = get_hive_cursor()
        self.comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

    def __del__(self):
        self.hive_cursor.close()
        self.hive_cursor = None

    """
    检查文件，协程多个调用并发执行
    """

    @asyncio.coroutine
    def task_trigger_old(self,command,dag_id_name, timeout):

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

            sys.exit(1)

            logging.info(e)


    """
    设置任务监控
    @:param list 
    [{"db":"", "table":"table", "partition":"partition", "timeout":"timeout"},]
    """
    def set_task_monitor(self, tables):
        commands = []
        for item in tables:
            table = item.get('table', None)
            db = item.get('db', None)
            partition = item.get('partition', None)
            timeout = item.get('timeout', None)

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

        loop = asyncio.get_event_loop()
        tasks = [self.task_trigger_old(items['cmd'], items['table'], items['timeout']) for items in commands]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()

      
    @provide_session
    @asyncio.coroutine
    def task_trigger(self,task, execution_date, dag,timeout, session=None,**_):

        try:

            print("========")

            sum_timeout = 0
            timeout_step = 120 #任务监控间隔时间(秒)

            print(task)
            print(execution_date)
            print(dag)


            upstream_task_instances = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.execution_date == execution_date,
                TaskInstance.task_id.in_(task.upstream_task_ids),
            )
            .all()
            )


            while sum_timeout <= int(timeout):
    
                logging.info("sum_timeout："+str(sum_timeout))
                logging.info("timeout："+str(timeout))
    
                yield from asyncio.sleep(int(timeout_step))
    
                sum_timeout += timeout_step


                upstream_states = [ti.state for ti in upstream_task_instances]
                fail_this_task = State.FAILED in upstream_states
    
                print("Do logic here...")
    
                if fail_this_task:
            
                    if sum_timeout >= int(timeout):

                        # self.comwx.postAppMessage(
                        #     'DW调度任务 {dag_id} 产出超时'.format(
                        #         dag_id=dag_id_name,
                        #         timeout=timeout
                        #     ),
                        #     '271'
                        # )

                        raise AirflowException("Failing task because one or more upstream tasks failed.")
    
                        logging.info("任务超时。。。。。")
                        sum_timeout=0
                else:
                    break

        except Exception as e:

            sys.exit(1)

            logging.info(e)