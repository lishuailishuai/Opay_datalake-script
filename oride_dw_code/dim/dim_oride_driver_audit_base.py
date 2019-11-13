# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'lili.chen',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dim_oride_driver_audit_base', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False)

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
ods_sqoop_mass_rider_signups_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_mass_rider_signups_df_prev_day_tesk",
      table="ods_sqoop_mass_rider_signups_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )


#依赖前一天分区
ods_sqoop_mass_driver_group_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_mass_driver_group_df_prev_day_tesk",
      table="ods_sqoop_mass_driver_group_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态 
      dag=dag
    )

#依赖前一天分区
ods_sqoop_mass_driver_team_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_mass_driver_team_df_prev_day_tesk",
      table="ods_sqoop_mass_driver_team_df",
      partition="dt='{{ds}}'",
      schema="oride_dw_ods",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------## 

db_name = "oride_dw"
table_name="dim_oride_driver_audit_base"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

def dim_oride_driver_audit_base_sql_task(ds):
    HQL ='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    
    select 
      dri.driver_id,-- 司机ID(司机_id、status联合主键), 
      driver_name ,-- 司机姓名, 
      driver_phone ,-- 电话号码, 
      gender,-- 1男，2女, 
      birthday,-- 生日, 
      dri.country,-- 国家, 
      state,-- 州, 
      city_id ,-- 城市(等同协会城市), 
      address,-- 详细地址, 
      address_photo,-- 地址验证图片., 
      address_status,-- 地址验证状态: 0:Pending 1:Passed 9:Failed, 
      address_status_note,-- 地址验证未通过原因, 
      adress_status_time,-- 地址验证时间, 
      address_status_admin_id,-- 地址验证管理员ID, 
      address_collecting_time,-- address veri time., 
      avator,-- 头像, 
      dirver_experience,-- 是否有驾驶经验：0没有，1有, 
      license_number,-- 驾照号, 
      holding_license_time,-- 驾照持有时间:1:less than 1 year2: 2~3 years3: More than 3 years, 
      gmail_account,-- gmail账号, 
      opay_account,-- opay账号, 
      drivers_test,-- 驾驶能力测试：0:Pending 1:Passed 9:Failed, 
      drivers_test_note,-- 测试结果备注或说明., 
      drivers_test_time,-- 驾驶能力测试时间, 
      drivers_test_admin_id,-- 测试管理员ID, 
      way_know,-- single selection 1. OPAY AGENT2. ADVERTISEMENT3. THROUGH A FRIEND 10 预注册, 
      base_finished_time,-- 基础信息完成时间:几乎等同于注册时间, 
      bvn_number,-- 银行信息身份验证_BVN码, 
      bnv_status,-- bvn码状态: 0:Pending 1:Passed 9:Failed, 
      bvn_status_note,-- bvn验证未通过原因, 
      bvn_time,-- 审核时间, 
      bvn_admin_id,-- 管理员ID, 
      veri_time,-- 总审核流程审查通过时间
      status,-- 骑手状态:0待审核,1正在审核,2通过审核,9审核失败,拒绝该账号, 
      note,-- 最终验证失败后的理由., 
      dri.admin_id,-- 最终验证人ID., 
      reg_code,-- 注册码, 
      dri.create_time,-- 报名注册时间(第一次报名), 
      dri.update_time,-- 数据更新时间, 
      rider_experience,-- Do you have any rider experience as a job before 1:XGo 2:Gokada 3: Other company 4: self business 5: Not at all 6: EasyM 7: MaxGo 8: JumiaFood 9: FedEx 10: DHL, 
      exp_cert_images,-- 竞对证据图片, 
      exp_plate_number,-- 竞对证据车牌号, 
      know_orider,-- How did you find out about ORide 1:OPay Agent 2:Advertisment 3:Through friend 4:field sales 5:telesales 6:self visit 7:through riders 8:HR Agent 9:Road show 10:ORide app, 
      know_orider_extend,-- 对应know_orider的号码, 
      agent_opay_account,-- Agent opay account, 
      field_sales_number,-- field sales number, 
      telesales_number,-- telesales number, 
      riders_number,-- through riders rider number, 
      road_show_number,-- 路演工作人员手机号, 
      hr_agent_company,-- agent id, 
      emergencies_name,-- In Case Of Emergencies who do we contact Name, 
      emergencies_mobile,-- In Case Of Emergencies who do we contact Mobile, 
      traing_test,-- 管理后台增加审核项——是否参加培训并通过测试:  1:passed、0:pengding 两种状态, 
      is_reward_amount,-- 是否领取新骑手奖励金额，1:passed 0:pengding, 
      reward_amount,-- 新骑手领取奖励金额, 
      marital_status,-- 1:marriged 2:unmarried 3:divorced, 
      religion,-- 1:Christians 2:Muslims 3:none 4:others, 
      religion_other,-- religion other, 
      id_number,-- Identify number, 
      online_test,-- 笔试测试：0:Pending 1:Passed 9:Failed, 
      online_test_note,-- 笔试未通过原因, 
      online_test_time,-- 笔试测试时间, 
      online_test_admin_id,-- 笔试管理员ID, 
      product_id ,-- 骑手类型：1 Oride-Green[专车], 2 Oride-Street[快车], 
      own_vehicle_brand,-- 第三方骑手车辆品牌, 
      own_vehicle_brand_other,-- 第三方骑手车辆其它品牌, 
      own_vehicle_model,-- 第三方骑手车辆型号, 
      own_plate_number,-- 第三方骑手车牌号, 
      own_chassis_number,-- 第三方骑手车架号, 
      own_engine_number,-- 第三方骑手发动机号, 
      own_engine_capacity,-- 第三方骑手发动机排量, 
      own_bike_photos,-- 第三方骑手车辆图片, 
      local_government,-- 第三方骑手地理围栏json, 
      vehicle_status,-- 车辆状况：0:Pending 1:Passed 9:Failed, 
      vehicle_status_note,-- 第三方骑手车辆验证未通过原因, 
      vehicle_status_time,-- 车辆状况检查时间, 
      vehicle_status_admin_id,-- 车辆管理员ID, 
      record_by,-- 填写人, 
      form_pics,-- 报名表照片等, 
      association_id,-- 骑手所属协会id, 
      team_id ,-- 骑手所属协会team id, 
      dri.id,  --数据主键
      nvl(driver_group.city,-1) as driver_group_city, --协会城市(-1 没有协会)
      nvl(driver_group.name,-1) as driver_group_name, --协会名称(-1 没有协会)
      nvl(driver_team.name,-1) as driver_team_name,  --团队名称(-1 没有司管团队)
      (case when dri.status=2 and dri.association_id >0 and dri.team_id > 0 then 1 else 0 end) as is_driver_audit_pass,
       --司机是否审核通过
       
      from_unixtime(veri_time,'yyyy-MM-dd') AS veri_audit_date,--总审核流程审查通过日期
      
         
       (case when dri.know_orider = 7  then 1 else 0 end ) as is_proxy_write_channel,
       --司机邀请司机渠道：代填写
       
       (case when dri.know_orider = 13  then 1 else 0 end ) as is_online_share_channel,
       --司机邀请司机渠道：线上分享
       
       (case when dri.know_orider = 14  then 1 else 0 end ) as is_onsite_audit_channel,
       --司机邀请司机渠道：现场审核
    
       'nal' AS country_code,
           --国家码字段
    
       '{pt}' as dt
    from 
    (select 
    *
    from 
    ( select
      driver_id,-- 司机ID, 
      name as driver_name ,-- 司机姓名, 
      mobile as driver_phone ,-- 电话号码, 
      gender,-- 1男，2女, 
      birthday,-- 生日, 
      country,-- 国家, 
      state,-- 州, 
      city as city_id ,-- 城市, 
      address,-- 详细地址, 
      address_photo,-- 地址验证图片., 
      address_status,-- 地址验证状态: 0:Pending 1:Passed 9:Failed, 
      address_status_note,-- 地址验证未通过原因, 
      adress_status_time,-- , 
      address_status_admin_id,-- , 
      address_collecting_time,-- address veri time., 
      avator,-- 头像, 
      dirver_experience,-- 是否有驾驶经验：0没有，1有, 
      license_number,-- 驾照号, 
      holding_license_time,-- 驾照持有时间:1:less than 1 year2: 2~3 years3: More than 3 years, 
      gmail_account,-- gmail账号, 
      opay_account,-- opay账号, 
      drivers_test,-- 驾驶能力测试：0:Pending 1:Passed 9:Failed, 
      drivers_test_note,-- 测试结果备注或说明., 
      drivers_test_time,-- 驾驶能力测试时间, 
      drivers_test_admin_id,-- , 
      way_know,-- single selection 1. OPAY AGENT2. ADVERTISEMENT3. THROUGH A FRIEND 10 预注册, 
      base_finished_time,-- 基础信息完成时间:几乎等同于注册时间, 
      bvn_number,-- BVN码, 
      bnv_status,-- bvn码状态: 0:Pending 1:Passed 9:Failed, 
      bvn_status_note,-- bvn验证未通过原因, 
      bvn_time,-- , 
      bvn_admin_id,-- , 
      veri_time,-- , 
      status,-- 骑手状态:0待审核,1正在审核,2通过审核,9审核失败,拒绝该账号, 
      note,-- 最终验证失败后的理由., 
      admin_id,-- 最终验证人ID., 
      reg_code,-- 注册码, 
      create_time,-- , 
      update_time,-- , 
      rider_experience,-- Do you have any rider experience as a job before 1:XGo 2:Gokada 3: Other company 4: self business 5: Not at all 6: EasyM 7: MaxGo 8: JumiaFood 9: FedEx 10: DHL, 
      exp_cert_images,-- 竞对证据图片, 
      exp_plate_number,-- 竞对证据车牌号, 
      know_orider,-- How did you find out about ORide 1:OPay Agent 2:Advertisment 3:Through friend 4:field sales 5:telesales 6:self visit 7:through riders 8:HR Agent 9:Road show 10:ORide app, 
      know_orider_extend,-- 对应know_orider的号码, 
      agent_opay_account,-- Agent opay account, 
      field_sales_number,-- field sales number, 
      telesales_number,-- telesales number, 
      riders_number,-- through riders:rider number, 
      road_show_number,-- 路演工作人员手机号, 
      hr_agent_company,-- agent id, 
      emergencies_name,-- In Case Of Emergencies who do we contact Name, 
      emergencies_mobile,-- In Case Of Emergencies who do we contact Mobile, 
      traing_test,-- 管理后台增加审核项——是否参加培训并通过测试:  1:passed、0:pengding 两种状态, 
      is_reward_amount,-- 是否领取新骑手奖励金额，1:passed 0:pengding, 
      reward_amount,-- 新骑手领取奖励金额, 
      marital_status,-- 1:marriged 2:unmarried 3:divorced, 
      religion,-- 1:Christians 2:Muslims 3:none 4:others, 
      religion_other,-- religion other, 
      id_number,-- Identify number, 
      online_test,-- 笔试测试：0:Pending 1:Passed 9:Failed, 
      online_test_note,-- 笔试未通过原因, 
      online_test_time,-- 笔试测试时间, 
      online_test_admin_id,-- , 
      driver_type as product_id ,-- 骑手类型：1 Oride-Green[专车], 2 Oride-Street[快车], 
      own_vehicle_brand,-- 第三方骑手车辆品牌, 
      own_vehicle_brand_other,-- 第三方骑手车辆其它品牌, 
      own_vehicle_model,-- 第三方骑手车辆型号, 
      own_plate_number,-- 第三方骑手车牌号, 
      own_chassis_number,-- 第三方骑手车架号, 
      own_engine_number,-- 第三方骑手发动机号, 
      own_engine_capacity,-- 第三方骑手发动机排量, 
      own_bike_photos,-- 第三方骑手车辆图片, 
      local_government,-- 第三方骑手地理围栏json, 
      vehicle_status,-- 车辆状况：0:Pending 1:Passed 9:Failed, 
      vehicle_status_note,-- 第三方骑手车辆验证未通过原因, 
      vehicle_status_time,-- 车辆状况检查时间, 
      vehicle_status_admin_id,-- , 
      record_by,-- 填写人, 
      form_pics,-- 报名表照片等, 
      association_id,-- 骑手所属协会id, 
      team_id,-- 骑手所属协会team id, 
      id,  --数据主键
      row_number() OVER(partition BY driver_id,status
                                   ORDER BY update_time DESC) AS rn1
          FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
          WHERE dt = '{pt}'
     )t1
    where rn1=1) dri
    left outer join
    (select * from oride_dw_ods.ods_sqoop_mass_driver_group_df WHERE dt = '{pt}') driver_group
    on dri.association_id = driver_group.id
    left outer join
    (select * from oride_dw_ods.ods_sqoop_mass_driver_team_df WHERE dt = '{pt}') driver_team
    on dri.team_id = driver_team.id
    where dri.driver_id<>1
    and city_id<>'999001' --去除测试数据
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct driver_id,status) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
      and country_code in ('nal')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_oride_driver_audit_base_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_oride_driver_audit_base_task = PythonOperator(
    task_id='dim_oride_driver_audit_base_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_mass_rider_signups_df_prev_day_tesk>>ods_sqoop_mass_driver_group_df_prev_day_tesk>>ods_sqoop_mass_driver_team_df_prev_day_tesk>>dim_oride_driver_audit_base_task

