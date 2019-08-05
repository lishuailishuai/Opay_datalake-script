from datetime import datetime, timedelta

import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 7, 15),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'app_oride_order_tagids_detail',
    schedule_interval="40 02 * * *",
    default_args=args)


validate_partition_data = PythonOperator(
    task_id='validate_partition_data',
    python_callable=validate_partition,
    provide_context=True,
    op_kwargs={
        # 验证table
        "table_names":
            [
             'oride_dw.dwd_oride_order_base_include_test_di',
             'oride_dw.ods_log_oride_order_skyeye_di'
             ],
        # 任务名称
        "task_name": "订单天眼数据"
    },
    dag=dag
)



oride_order_base_validate_task = HivePartitionSensor(
    task_id="oride_order_base_validate_task",
    table="dwd_oride_order_base_include_test_di",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


oride_order_skyeye_validate_task = HivePartitionSensor(
    task_id="oride_order_skyeye_validate_task",
    table="ods_log_oride_order_skyeye_di",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


order_tags_info_to_msyql = HiveToMySqlTransfer(
    task_id='order_tags_info_to_msyql',
    sql="""
            select 
            ord.dt,
            ord.order_id,
            ord.driver_id,
            ord.passenge_id,
            ord.product_id,
            ord.city_id,
            ord.price,--（订单应付金额）
            ord.pay_amount,--（订单实付金额）
            ord.reward,--（给司机发放的奖励）
            nvl(concat_ws(',',eye.tag_ids),-1) as tag_name,--（命中tag，若有多个用英文逗号隔开,-1 订单未知）
            ord.start_name,--（订单起点）
            ord.end_name --（订单终点）
        from 
        (select 
            dt,
            order_id,
            driver_id,
            passenge_id,
            product_id,
            city_id,
            price,--（订单应付金额）
            pay_amount,--（订单实付金额）
            reward,--（给司机发放的奖励）
            start_name,--（订单起点）
            end_name --（订单终点）
        from oride_dw.dwd_oride_order_base_include_test_di where dt='{{ ds }}') ord
        left outer join
        (select order_id,tag_ids from oride_dw.ods_log_oride_order_skyeye_di where dt='{{ ds }}') eye
        on ord.order_id=eye.order_id
        
        """,
    mysql_conn_id='mysql_bi',
    mysql_table='app_oride_order_tagids_detail',
    dag=dag)


validate_partition_data >> oride_order_base_validate_task >> order_tags_info_to_msyql
validate_partition_data >> oride_order_skyeye_validate_task >> order_tags_info_to_msyql


