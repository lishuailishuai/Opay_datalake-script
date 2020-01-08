# coding: utf-8
'''
add by duo.wu统计中台数据
'''

import airflow
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn
from airflow.operators.python_operator import PythonOperator
import logging
from airflow.hooks.mysql_hook import MySqlHook
import sys
from importlib import reload
reload(sys)
import MySQLdb

args = {
    'owner': 'wuduo',
    'start_date': datetime(2020, 1, 7),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'opayspread_daily_ali',
    schedule_interval="0 3 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args)

'''
预注册司机数hql know_orider = 4 --20190724 
'''
promoter_preregist_sql = '''
SELECT
    r.daily as daily,
    r.driver_type as driver_type, 
    r.know_orider as channel, 
    MAX(p.user_name) as name,
    MAX(p.name) as mobile,
    p.code as code,
    count(distinct r.id) as drivers 
FROM (select * from oride_dw_ods.ods_sqoop_promoter_promoter_user_df where dt = '{ds}' ) as p JOIN 
    (SELECT 
        from_unixtime(create_time, 'yyyy-MM-dd') as daily, 
        if(length(know_orider_extend)=10, concat('0',know_orider_extend), know_orider_extend) as know_orider_extend, 
        id, 
        know_orider, 
        driver_type 
    from oride_dw_ods.ods_sqoop_mass_rider_signups_df
    WHERE from_unixtime(create_time, 'yyyy-MM-dd') = '{ds}' and 
        dt = '{ds}' 
    ) as r 
ON p.name = r.know_orider_extend 
WHERE  
    p.dt = '{ds}' 
GROUP BY r.daily, r.know_orider, p.code, r.driver_type
'''
promoter_preregist_channel_sql = '''
SELECT 
    from_unixtime(create_time, 'yyyy-MM-dd') as daily, 
    know_orider,
    driver_type,  
    count(distinct id) as drivers  
from oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE from_unixtime(create_time, 'yyyy-MM-dd') = '{ds}' and 
    dt = '{ds}'  
GROUP BY from_unixtime(create_time, 'yyyy-MM-dd'), know_orider, driver_type
'''


'''
填写资料司机数 know_orider = 4 --20190724
'''
promoter_regist_sql = '''
SELECT
    r.daily as daily,
    r.driver_type as driver_type, 
    r.know_orider as channel, 
    MAX(p.user_name) as name,
    MAX(p.name) as mobile,
    p.code as code,
    count(distinct r.id) as drivers 
FROM (select * from oride_dw_ods.ods_sqoop_promoter_promoter_user_df where dt = '{ds}' ) as p JOIN 
    (SELECT 
        from_unixtime(create_time, 'yyyy-MM-dd') as daily, 
        if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) as know_orider_extend, 
        id, 
        know_orider, 
        driver_type 
    from oride_dw_ods.ods_sqoop_mass_rider_signups_df
    WHERE length(know_orider_extend)>0 and 
        record_by<>'' and 
        from_unixtime(create_time, 'yyyy-MM-dd') = '{ds}' and  
        dt = '{ds}' 
    ) as r 
ON p.name = r.know_orider_extend 
WHERE  
    p.dt = '{ds}' 
GROUP BY r.daily, r.know_orider, p.code, r.driver_type
'''
promoter_regist_channel_sql = '''
SELECT 
    from_unixtime(create_time, 'yyyy-MM-dd') as daily, 
    know_orider,
    driver_type,   
    count(distinct id) as drivers 
from oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE length(know_orider_extend)>0 and 
    record_by<>'' and 
    from_unixtime(create_time, 'yyyy-MM-dd') = '{ds}' and  
    dt = '{ds}' 
GROUP BY from_unixtime(create_time, 'yyyy-MM-dd'), know_orider, driver_type
'''


'''
笔试通过司机数 know_orider = 4 --20190724
'''
promoter_onlinetest_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(online_test_time, 'yyyy-MM-dd') AS daily,
          if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) AS know_orider_extend,
          id,
          know_orider,
          driver_type
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE online_test = 1
     AND from_unixtime(online_test_time, 'yyyy-MM-dd') = '{ds}'
     AND dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''
promoter_onlinetest_channel_sql = ''' 
SELECT from_unixtime(online_test_time, 'yyyy-MM-dd') AS daily,
       know_orider,
       driver_type,
       count(DISTINCT id) AS drivers
FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE online_test = 1
  AND from_unixtime(online_test_time, 'yyyy-MM-dd') = '{ds}'
  AND dt = '{ds}'
GROUP BY from_unixtime(online_test_time, 'yyyy-MM-dd'),
         know_orider,
         driver_type
'''


'''
驾驶测试通过司机数量 know_orider = 4 --20190724
'''
promoter_drivertest_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(drivers_test_time, 'yyyy-MM-dd') AS daily,
          if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) AS know_orider_extend,
          id,
          know_orider,
          driver_type
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE drivers_test = 1
     AND from_unixtime(drivers_test_time, 'yyyy-MM-dd') = '{ds}'
     AND dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''
promoter_drivertest_channel_sql = '''
SELECT 
    from_unixtime(drivers_test_time, 'yyyy-MM-dd') as daily,
    know_orider, 
    driver_type,   
    count(distinct id) as drivers
from oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE drivers_test = 1 and 
    from_unixtime(drivers_test_time, 'yyyy-MM-dd') = '{ds}' and  
    dt = '{ds}' 
GROUP BY from_unixtime(drivers_test_time, 'yyyy-MM-dd'), know_orider, driver_type
'''


'''
车辆状态检查司机数 know_orider = 4 --20190724
'''
promoter_vehicle_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(vehicle_status_time, 'yyyy-MM-dd') AS daily,
          if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) AS know_orider_extend,
          id,
          know_orider,
          driver_type
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE driver_type = 2
     AND vehicle_status = 1
     AND from_unixtime(vehicle_status_time, 'yyyy-MM-dd') = '{ds}'
     AND dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''
promoter_vehicle_channel_sql = '''
SELECT from_unixtime(vehicle_status_time, 'yyyy-MM-dd') AS daily,
       know_orider,
       driver_type,
       count(DISTINCT id) AS drivers
FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE driver_type = 2
  AND vehicle_status = 1
  AND from_unixtime(vehicle_status_time, 'yyyy-MM-dd') = '{ds}'
  AND dt = '{ds}'
GROUP BY from_unixtime(vehicle_status_time, 'yyyy-MM-dd'),
         know_orider,
         driver_type
'''


'''
地址验证通过司机数 know_orider = 4 --20190724
'''
promoter_address_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(adress_status_time, 'yyyy-MM-dd') AS daily,
          if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) AS know_orider_extend,
          id,
          know_orider,
          driver_type
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE address_status = 1
     AND from_unixtime(adress_status_time, 'yyyy-MM-dd') = '{ds}'
     AND dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''
promoter_address_channel_sql = '''
SELECT from_unixtime(adress_status_time, 'yyyy-MM-dd') AS daily,
       know_orider,
       driver_type,
       count(DISTINCT id) AS drivers
FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE address_status = 1
  AND from_unixtime(adress_status_time, 'yyyy-MM-dd') = '{ds}'
  AND dt = '{ds}'
GROUP BY from_unixtime(adress_status_time, 'yyyy-MM-dd'),
         know_orider,
         driver_type
'''


'''
完全通过司机数 know_orider = 4 --20190724
'''
promoter_status_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(veri_time, 'yyyy-MM-dd') AS daily,
          if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) AS know_orider_extend,
          id,
          know_orider,
          driver_type
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE status = 2
     AND from_unixtime(veri_time, 'yyyy-MM-dd') = '{ds}'
     AND dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''
promoter_status_channel_sql = '''
SELECT 
    from_unixtime(veri_time, 'yyyy-MM-dd') as daily,
    know_orider, 
    driver_type,   
    count(distinct id) as drivers 
from oride_dw_ods.ods_sqoop_mass_rider_signups_df
WHERE status = 2 and  
    from_unixtime(veri_time, 'yyyy-MM-dd') = '{ds}' and  
    dt = '{ds}' 
GROUP BY from_unixtime(veri_time, 'yyyy-MM-dd'), know_orider, driver_type
'''


'''
担保司机数 r.know_orider = 4 --20190724
'''
promoter_guarantors_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(g.create_time, 'yyyy-MM-dd') AS daily,
          if(length(r.know_orider_extend)=10, concat('0', r.know_orider_extend), r.know_orider_extend) AS know_orider_extend,
          r.id,
          r.know_orider,
          r.driver_type
   FROM
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
      WHERE dt = '{ds}') AS r
   JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_guarantors_df
      WHERE dt = '{ds}') AS g ON r.id = g.rider_id
   WHERE g.rider_id <> NULL
     AND from_unixtime(g.create_time, 'yyyy-MM-dd') = '{ds}'
     AND r.dt = '{ds}'
     AND g.dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''
promoter_guarantors_channel_sql = '''
SELECT from_unixtime(g.create_time, 'yyyy-MM-dd') AS daily,
       r.know_orider,
       r.driver_type,
       count(DISTINCT r.id) AS drivers
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE dt = '{ds}') AS r
JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_guarantors_df
   WHERE dt = '{ds}') AS g ON r.id = g.rider_id
WHERE g.rider_id <> NULL
  AND from_unixtime(g.create_time, 'yyyy-MM-dd') = '{ds}'
  AND r.dt = '{ds}'
  AND g.dt = '{ds}'
GROUP BY from_unixtime(g.create_time, 'yyyy-MM-dd'),
         know_orider,
         driver_type
'''


'''
当天接单数 r.know_orider = 4  --20190724
'''
promoter_ordertake_hql = '''
SELECT tm.daily AS DAY,
       tm.driver_type AS driver_type,
       tm.channel AS channel,
       p.user_name AS name,
       p.name AS mobile,
       p.code AS code,
       tm.orders
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(o.take_time, 'yyyy-MM-dd') AS daily,
          r.driver_type,
          MAX(r.know_orider) AS channel,
          if(length(r.know_orider_extend)=10, concat('0', r.know_orider_extend), r.know_orider_extend) AS know_orider_extend,
          count(DISTINCT o.id) AS orders
   FROM
     (SELECT *
      FROM oride_dw.dwd_oride_order_base_include_test_di
      WHERE dt = '{ds}') AS o
   JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
      WHERE dt = '{ds}') AS r ON o.driver_id = r.driver_id
   WHERE from_unixtime(o.take_time, 'yyyy-MM-dd') = '{ds}'
     AND r.dt = '{ds}'
     AND o.dt = '{ds}'
   GROUP BY from_unixtime(o.take_time, 'yyyy-MM-dd'),
            r.driver_type,
            r.know_orider_extend ) AS tm ON p.name = tm.know_orider_extend
WHERE p.dt = '{ds}'
'''
promoter_ordertake_channel_hql = '''
SELECT from_unixtime(o.take_time, 'yyyy-MM-dd') AS daily,
       r.know_orider AS channel,
       r.driver_type,
       count(DISTINCT o.id) AS orders
FROM
  (SELECT *
   FROM oride_dw.dwd_oride_order_base_include_test_di
   WHERE dt = '{ds}') AS o
JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE dt = '{ds}') AS r ON o.driver_id = r.driver_id
WHERE from_unixtime(o.take_time, 'yyyy-MM-dd') = '{ds}'
  AND r.dt = '{ds}'
  AND o.dt = '{ds}'
GROUP BY from_unixtime(o.take_time, 'yyyy-MM-dd'),
         r.know_orider,
         r.driver_type
'''


'''
当天邦车活跃 r.know_orider = 4  --20190724
'''
promoter_dirverdau_hql = '''
SELECT tm.daily AS DAY,
       tm.driver_type AS driver_type,
       tm.channel AS channel,
       p.user_name AS name,
       p.name AS mobile,
       p.code AS code,
       tm.online AS online
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT '{ds}' AS daily,
          r.driver_type,
          MAX(r.know_orider) AS channel,
          if(length(r.know_orider_extend)=10, concat('0', r.know_orider_extend), r.know_orider_extend) AS know_orider_extend,
          count(DISTINCT r.id) AS online
   FROM
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
      WHERE dt = '{ds}') AS r
   JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df
      WHERE dt = '{ds}') AS d ON r.driver_id = d.id
   WHERE from_unixtime(d.login_time, 'yyyy-MM-dd') = '{ds}'
     AND r.dt = '{ds}'
     AND d.dt = '{ds}'
   GROUP BY r.driver_type,
            r.know_orider_extend ) AS tm ON p.name = tm.know_orider_extend
WHERE p.dt = '{ds}'
'''
promoter_dirverdau_channel_hql = '''
SELECT from_unixtime(d.login_time, 'yyyy-MM-dd') AS daily,
       r.know_orider AS channel,
       r.driver_type,
       count(DISTINCT r.id) AS online
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE dt = '{ds}') AS r
JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df
   WHERE dt = '{ds}') AS d ON r.driver_id = d.id
WHERE from_unixtime(d.login_time, 'yyyy-MM-dd') = '{ds}'
  AND r.dt = '{ds}'
  AND d.dt = '{ds}'
GROUP BY from_unixtime(d.login_time, 'yyyy-MM-dd'),
         r.know_orider,
         r.driver_type
'''


'''
当天邦车司机数 r.know_orider = 4  --20190724
'''
promoter_driverbind_sql = '''
SELECT tm.daily AS DAY,
       tm.driver_type AS driver_type,
       tm.channel AS channel,
       p.user_name AS name,
       p.name AS mobile,
       p.code AS code,
       tm.bind AS bind
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT '{ds}' AS daily,
          r.driver_type,
          MAX(r.know_orider) AS channel,
          if(length(r.know_orider_extend)=10, concat('0', r.know_orider_extend), r.know_orider_extend) AS know_orider_extend,
          count(DISTINCT r.id) AS bind
   FROM
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
      WHERE dt = '{ds}') AS r
   JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df
      WHERE dt = '{ds}') AS d ON r.driver_id = d.id
   WHERE from_unixtime(d.first_bind_time, 'yyyy-MM-dd') = '{ds}'
     AND r.dt = '{ds}'
     AND d.dt = '{ds}'
   GROUP BY r.driver_type,
            r.know_orider_extend ) AS tm ON p.name = tm.know_orider_extend
WHERE p.dt = '{ds}'
'''
promoter_driverbind_channel_sql = '''
SELECT from_unixtime(d.first_bind_time, 'yyyy-MM-dd') AS daily,
       r.know_orider AS channel,
       r.driver_type,
       count(DISTINCT r.id) AS bind
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE dt = '{ds}') AS r
JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df
   WHERE dt = '{ds}') AS d ON r.driver_id = d.id
WHERE from_unixtime(d.first_bind_time, 'yyyy-MM-dd') = '{ds}'
  AND r.dt = '{ds}'
  AND d.dt = '{ds}'
GROUP BY from_unixtime(d.first_bind_time, 'yyyy-MM-dd'),
         r.know_orider,
         r.driver_type
'''

"""
vehicles， presignup
"""
promoter_vehicles_sql = '''
SELECT tm.daily AS DAY,
       tm.driver_type AS driver_type,
       tm.channel AS channel,
       p.user_name AS name,
       p.name AS mobile,
       p.code AS code,
       tm.vehicles
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT '{ds}' AS daily,
          r.driver_type,
          MAX(r.know_orider) AS channel,
          if(length(r.know_orider_extend)=10, concat('0', r.know_orider_extend), r.know_orider_extend) AS know_orider_extend,
          count(1) AS vehicles
   FROM
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_driver_bind_logs_df
      WHERE dt = '{ds}') AS o
   JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
      WHERE dt = '{ds}') AS r ON o.driver_id = r.driver_id
   WHERE o.operate_time >= '{ds} 00:00:00'
     AND o.operate_time <= '{ds} 23:59:59'
     AND r.dt = '{ds}'
     AND o.dt = '{ds}'
     AND o.operate = 0
   GROUP BY '{ds}',
            r.driver_type,
            r.know_orider_extend) AS tm ON p.name = tm.know_orider_extend
WHERE p.dt = '{ds}'
'''

promoter_presignup_sql = '''
SELECT r.daily AS daily,
       r.driver_type AS driver_type,
       r.know_orider AS channel,
       MAX(p.user_name) AS name,
       MAX(p.name) AS mobile,
       p.code AS code,
       count(DISTINCT r.driver_id) AS presignup
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT from_unixtime(create_time, 'yyyy-MM-dd') AS daily,
          if(length(know_orider_extend)=10, concat('0', know_orider_extend), know_orider_extend) AS know_orider_extend,
          driver_id,
          know_orider,
          driver_type
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE way_know = 10
     AND address <> ''
     AND from_unixtime(create_time, 'yyyy-MM-dd') = '{ds}'
     AND dt = '{ds}') AS r ON p.name = r.know_orider_extend
WHERE p.dt = '{ds}'
GROUP BY r.daily,
         r.know_orider,
         p.code,
         r.driver_type
'''


hive_tasks = [
    {'task': 'preregist', 'sql': promoter_preregist_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, allusers) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE allusers = values(allusers)'},
    {'task': 'regist', 'sql': promoter_regist_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, fullinfo) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE fullinfo = values(fullinfo)'},
    {'task': 'onlinetest', 'sql': promoter_onlinetest_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, online_test) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE online_test = values(online_test)'},
    {'task': 'drivertest', 'sql': promoter_drivertest_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, drivers_test) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE drivers_test = values(drivers_test)'},
    {'task': 'vehicle', 'sql': promoter_vehicle_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, vehicle_status) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE vehicle_status = values(vehicle_status)'},
    {'task': 'address', 'sql': promoter_address_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, address_status) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE address_status = values(address_status)'},
    {'task': 'status', 'sql': promoter_status_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, status) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE status = values(status)'},
    {'task': 'guarantors', 'sql': promoter_guarantors_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, guarantor) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE guarantor = values(guarantor)'},
    {'task': 'take', 'sql': promoter_ordertake_hql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, KPI) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE KPI = if(driver_type=2, values(KPI), KPI)'},
    {'task': 'bind', 'sql': promoter_driverbind_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, KPI) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE KPI = if(driver_type=1, values(KPI), KPI)'},
    {'task': 'vehicles', 'sql': promoter_vehicles_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, vehicles) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE vehicles = values(vehicles)'},
    {'task': 'presignup', 'sql': promoter_presignup_sql, 'sql_insert': 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, presignup) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE presignup = values(presignup)'}
]


def hiveresult_to_mysql(ds, **kwargs):
    cursor = get_hive_cursor()
    logging.info(kwargs['sql'].format(ds=ds))
    cursor.execute(kwargs['sql'].format(ds=ds))
    results = cursor.fetchall()
    mysql_conn = get_db_conn('oride_dw_ods_mysql')
    mcursor = mysql_conn.cursor()
    sql_insert = kwargs['sql_insert']
    sql_val = ''
    sql_ext = kwargs['sql_ext']
    sql_count = 0
    for day, driver_type, channel, name, mobile, code, drivers in results:
        sql_tmp = "('{day}', '{name}', '{mobile}',  '{code}', '{channel}', '{driver_type}', '{dirvers}')".format(
            day=day,
            name=name.replace("\\", "").replace("'", "\\'"),
            code=code,
            mobile=mobile if (len(mobile) < 20) else '',
            channel=channel,
            driver_type=driver_type,
            dirvers=drivers
        )
        if sql_val == '':
            sql_val = sql_tmp
        else:
            sql_val += ',' + sql_tmp
        sql_count += 1
        if sql_count >= 1000:
            sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
            #logging.info(sql)
            mcursor.execute(sql)
            sql_count = 0
            sql_val = ''

    if sql_count > 0:
        sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
        mcursor.execute(sql)

    mysql_conn.commit()
    cursor.close()
    mcursor.close()
    mysql_conn.close()


for my_task in hive_tasks:
    hive_result_to_mysql = PythonOperator(
        task_id='hive_result_to_mysql_{}'.format(my_task['task']),
        python_callable=hiveresult_to_mysql,
        provide_context=True,
        op_kwargs={'sql_insert': my_task['sql_insert'], 'sql_ext': my_task['sql_ext'], 'sql': my_task['sql']},
        dag=dag
    )

    hive_result_to_mysql


hive_channel_tasks = [
    {'task': 'pregist_channel', 'sql': promoter_preregist_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, allusers) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE allusers = values(allusers)'},
    {'task': 'regist_channel', 'sql': promoter_regist_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, fullinfo) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE fullinfo = values(fullinfo)'},
    {'task': 'onlinetest_channel', 'sql': promoter_onlinetest_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, online_test) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE online_test = values(online_test)'},
    {'task': 'drivertest_channel', 'sql': promoter_drivertest_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, drivers_test) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE drivers_test = values(drivers_test)'},
    {'task': 'vehicle_channel', 'sql': promoter_vehicle_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, vehicle_status) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE vehicle_status = values(vehicle_status)'},
    {'task': 'address_channel', 'sql': promoter_address_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, address_status) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE address_status = values(address_status)'},
    {'task': 'status_channel', 'sql': promoter_status_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, status) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE status = values(status)'},
    {'task': 'guarantors_channel', 'sql': promoter_guarantors_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, guarantor) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE guarantor = values(guarantor)'},
    {'task': 'ordertake_channel', 'sql': promoter_ordertake_channel_hql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, KPI) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE KPI = if(driver_type=2, values(KPI), KPI)'},
    {'task': 'driverbind_channel', 'sql': promoter_driverbind_channel_sql, 'sql_insert': 'INSERT INTO promoter_channel_day (day, channel, driver_type, KPI) VALUES', 'sql_ext': 'ON DUPLICATE KEY UPDATE KPI = if(driver_type=1, values(KPI), KPI)'}
]


def hiveresult_to_channel_mysql(ds, **kwargs):
    cursor = get_hive_cursor()
    logging.info(kwargs['sql'].format(ds=ds))
    cursor.execute(kwargs['sql'].format(ds=ds))
    results = cursor.fetchall()
    mysql_conn = get_db_conn('oride_dw_ods_mysql')
    mcursor = mysql_conn.cursor()
    sql_insert = kwargs['sql_insert']
    sql_val = ''
    sql_ext = kwargs['sql_ext']
    sql_count = 0
    for day, channel, driver_type, drivers in results:
        sql_tmp = "('{day}', '{channel}', '{driver_type}', '{dirvers}')".format(
            day=day,
            channel=channel,
            driver_type=driver_type,
            dirvers=drivers
        )
        if sql_val == '':
            sql_val = sql_tmp
        else:
            sql_val += ',' + sql_tmp
        sql_count += 1
        if sql_count >= 1000:
            sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
            # logging.info(sql)
            mcursor.execute(sql)
            sql_count = 0
            sql_val = ''

    if sql_count > 0:
        sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
        mcursor.execute(sql)

    mysql_conn.commit()
    cursor.close()
    mcursor.close()
    mysql_conn.close()


for my_task in hive_channel_tasks:
    hive_result_channel_to_mysql = PythonOperator(
        task_id='hiveresult_channel_to_mysql_{}'.format(my_task['task']),
        python_callable=hiveresult_to_channel_mysql,
        provide_context=True,
        op_kwargs={'sql_insert': my_task['sql_insert'], 'sql_ext': my_task['sql_ext'], 'sql': my_task['sql']},
        dag=dag
    )
    hive_result_channel_to_mysql


'''
首次订单数据 r.know_orider = 4  --20190724
'''
promoter_orderoverview_hql = '''
SELECT tm.daily AS DAY,
       tm.driver_type AS driver_type,
       tm.channel AS channel,
       p.user_name AS name,
       p.name AS mobile,
       p.code AS code,
       tm.firstorder,
       tm.tenorders
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_promoter_promoter_user_df
   WHERE dt = '{ds}') AS p
JOIN
  (SELECT t.daily,
          r.driver_type,
          MAX(r.know_orider) AS channel,
          if(length(r.know_orider_extend)=10, concat('0', r.know_orider_extend), r.know_orider_extend) AS know_orider_extend,
          count(DISTINCT if(t.orders=1, t.driver_id, NULL)) AS firstorder,
          count(DISTINCT if(t.orders=10, t.driver_id, NULL)) AS tenorders
   FROM
     (SELECT driver_id,
             arrive_time,
             from_unixtime(arrive_time, 'yyyy-MM-dd') AS daily,
             row_number() over(partition BY driver_id
                               ORDER BY arrive_time) orders
      FROM
        (SELECT *
         FROM oride_dw.dwd_oride_order_base_include_test_di
         WHERE dt = '{ds}')
      WHERE status IN (4,
                       5)
        AND dt='{ds}' ) t
   JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
      WHERE dt = '{ds}') AS r ON r.driver_id = t.driver_id
   WHERE (t.orders=1
          OR t.orders = 10)
     AND from_unixtime(t.arrive_time, 'yyyy-MM-dd') = '{ds}'
     AND r.dt = '{ds}'
   GROUP BY t.daily,
            r.know_orider_extend,
            r.driver_type ) AS tm ON p.name = tm.know_orider_extend
WHERE p.dt = '{ds}'
'''
promoter_orderoverview_channel_hql = '''
SELECT t.daily,
       r.know_orider AS channel,
       r.driver_type,
       count(DISTINCT if(t.orders=1, t.driver_id, NULL)) AS firstorder,
       count(DISTINCT if(t.orders=10, t.driver_id, NULL)) AS tenorders
FROM (
SELECT driver_id,
       arrive_time,
       from_unixtime(arrive_time, 'yyyy-MM-dd') AS daily,
       row_number() over(partition BY driver_id
                         ORDER BY arrive_time) orders
FROM
  (SELECT *
   FROM oride_dw.dwd_oride_order_base_include_test_di
   WHERE status IN (4,
                    5)
     AND dt='{ds}') t
JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_mass_rider_signups_df
   WHERE dt = '{ds}') AS r ON r.driver_id = t.driver_id
WHERE (t.orders=1
       OR t.orders = 10)
  AND from_unixtime(t.arrive_time, 'yyyy-MM-dd') = '{ds}'
  AND r.dt = '{ds}'
GROUP BY t.daily,
         r.know_orider,
         r.driver_type
'''


def order_result_to_mysql(ds, **kwargs):
    cursor = get_hive_cursor()
    logging.info(promoter_orderoverview_hql.format(ds=ds))
    cursor.execute(promoter_orderoverview_hql.format(ds=ds))
    results = cursor.fetchall()
    mysql_conn = get_db_conn('oride_dw_ods_mysql')
    mcursor = mysql_conn.cursor()

    sql_insert = 'INSERT INTO promoter_driver_day (day, name, mobile, code, channel, driver_type, firstbill) VALUES'
    sql_ext = 'ON DUPLICATE KEY UPDATE firstbill = values(firstbill)'
    sql_val = ''
    sql_count = 0
    for day, driver_type, channel, name, mobile, code, first, ten in results:
        sql_tmp = "('{day}', '{name}', '{mobile}',  '{code}', '{channel}', '{driver_type}', '{firstbill}')".format(
            day=day,
            name=name.replace("\\", "").replace("'", "\\'"),
            mobile=mobile if (len(mobile) < 20) else '',
            code=code,
            channel=channel,
            driver_type=driver_type,
            firstbill=(first if driver_type == 2 else 0)
        )

        if sql_val == '':
            sql_val = sql_tmp
        else:
            sql_val += ',' + sql_tmp
        sql_count += 1
        if sql_count >= 1000:
            sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
            mcursor.execute(sql)
            sql_count = 0
            sql_val = ''

    if sql_count > 0:
        sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
        mcursor.execute(sql)

    mysql_conn.commit()
    cursor.close()
    mcursor.close()
    mysql_conn.close()


orderresult_to_mysql = PythonOperator(
    task_id='order_result_to_mysql',
    python_callable=order_result_to_mysql,
    provide_context=True,
    dag=dag
)

orderresult_to_mysql


def orderresult_channel_to_mysql(ds, **kwargs):
    cursor = get_hive_cursor()
    logging.info(promoter_orderoverview_channel_hql.format(ds=ds))
    cursor.execute(promoter_orderoverview_channel_hql.format(ds=ds))
    results = cursor.fetchall()
    mysql_conn = get_db_conn('oride_dw_ods_mysql')
    mcursor = mysql_conn.cursor()

    sql_insert = 'INSERT INTO promoter_channel_day (day, channel, driver_type, firstbill) VALUES'
    sql_ext = 'ON DUPLICATE KEY UPDATE firstbill = values(firstbill)'
    sql_val = ''
    sql_count = 0
    for day, channel,  driver_type, first, ten in results:
        sql_tmp = "('{day}', '{channel}', '{driver_type}', '{firstbill}')".format(
            day=day,
            channel=channel,
            driver_type=driver_type,
            firstbill=(first if driver_type == 2 else 0)
        )

        if sql_val == '':
            sql_val = sql_tmp
        else:
            sql_val += ',' + sql_tmp
        sql_count += 1
        if sql_count >= 1000:
            sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
            mcursor.execute(sql)
            sql_count = 0
            sql_val = ''

    if sql_count > 0:
        sql = sql_insert + ' ' + sql_val + ' ' + sql_ext
        mcursor.execute(sql)

    mysql_conn.commit()
    cursor.close()
    mcursor.close()
    mysql_conn.close()


order_result_channel_to_mysql = PythonOperator(
    task_id='orderresult_channel_to_mysql',
    python_callable=orderresult_channel_to_mysql,
    provide_context=True,
    dag=dag
)

order_result_channel_to_mysql


# 中台数据查询
cssql = '''
SELECT if(TB.id IS NULL, 0, TB.id) AS driver_id,
       TA.dt AS dt,
       if(TB.real_name IS NULL, 0, TB.real_name) AS name,
       if(TB.phone_number IS NULL, 0, TB.phone_number) AS phone,
       if(TC.city_id IS NULL, 0, TC.city_id) AS city,
       if(TC.serv_type IS NULL, 0, TC.serv_type) AS TYPE,
       TA.distance AS distance,
       TA.money AS income,
       TA.olsettle AS onlineSettlement,
       TA.olsettlecount AS onlineTotal,
       TA.order_num AS total_orders,
       TA.arrived_order_num AS arrived_orders,
       TA.score_num AS COMMENT,
       TA.badcomments_num AS badcomments_num,
       TA.score_sum AS score,
       round(if(TD.driver_onlinerange IS NULL, 0, TD.driver_onlinerange/60),2) AS onlinetime
FROM (
SELECT max(a.dt) AS dt,
       a.driver_id,
       sum(if(a.status IN (4, 5),a.distance,0)) AS distance,
       sum(if(a.status IN (4, 5),a.price,0)) AS money,
       sum(if(c.price IS NOT NULL,c.price,0)) AS olsettle,
       sum(if(c.price IS NOT NULL,1,0)) AS olsettlecount,
       count(*) AS order_num,
       sum(if(a.status IN (4, 5),1,0)) AS arrived_order_num,
       sum(if(b.score IS NOT NULL, 1, 0)) AS score_num,
       sum(if((b.score IS NOT NULL)
              AND (b.score<=3), 1, 0)) AS badcomments_num,
       sum(if(b.score IS NOT NULL, b.score, 0)) AS score_sum
FROM
  (SELECT *
   FROM
     (SELECT *
      FROM oride_dw.dwd_oride_order_base_include_test_di
      WHERE dt = '{ds}'
        AND from_unixtime(create_time, 'yyyy-MM-dd') = '{ds}') a
   LEFT OUTER JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_driver_comment_df
      WHERE dt = '{ds}') b ON a.id = b.order_id
   LEFT OUTER JOIN
     (SELECT *
      FROM oride_dw_ods.ods_sqoop_base_data_order_payment_df
      WHERE dt = '{ds}'
        AND MODE>=0) c ON a.id = c.id
   GROUP BY a.driver_id) TA
LEFT JOIN
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_driver_df
   WHERE dt = '{ds}') TB ON TA.driver_id = TB.id
LEFT JOIN (
SELECT *
FROM
  (SELECT *
   FROM oride_dw_ods.ods_sqoop_base_data_driver_extend_df
   WHERE dt = '{ds}') TC ON TA.driver_id = TC.id
LEFT JOIN
  (SELECT *
   FROM oride_dw_ods.ods_log_oride_driver_timerange
   WHERE dt = '{ds}') TD ON TA.driver_id = TD.driver_id
WHERE TA.driver_id>0
  AND TC.serv_type=2
'''

def csresult_channel_to_mysql(ds, **kwargs):
    cursor = get_hive_cursor()
    logging.info(cssql.format(ds=ds))
    cursor.execute(cssql.format(ds=ds))
    results = cursor.fetchall()
    mysql_conn = get_db_conn('oride_dw_ods_mysql')
    mcursor = mysql_conn.cursor()

    sql_insert = '''
        INSERT INTO promoter_order_day (
            dt, driver_id, driver_type, name, mobile, city_id, distance, income, online_paid, online_total, total_orders,
            arrived_orders, total_comments, bad_comments, total_score, online_time
        ) VALUES
    '''
    sql_ext = '''
        ON DUPLICATE KEY UPDATE 
    '''
    sql_val = ''
    sql_count = 0
    for driver_id, dt, name, phone, city, type, distance, income, onlineSettlement, onlineTotal, total_orders, arrived_orders, comment, badcomments_num, score, onlinetime in results:
        sql_tmp = '''
            ('{dt}', '{driver_id}', '{driver_type}', '{name}', '{mobile}', '{city_id}', '{distance}', '{income}', '{online_paid}', '{online_total}', '{total_orders}', '{arrived_orders}', '{total_comments}', '{bad_comments}', '{total_score}', '{online_time}')
        '''.format(
            dt=dt,
            driver_id=driver_id,
            driver_type=type,
            name=name.replace("\\", "").replace("'", "\\'"),
            mobile=phone,
            city_id=city,
            distance=distance,
            income=income,
            online_paid=onlineSettlement,
            online_total=onlineTotal,
            total_orders=total_orders,
            arrived_orders=arrived_orders,
            total_comments=comment,
            bad_comments=badcomments_num,
            total_score=score,
            online_time=onlinetime
        )

        if sql_val == '':
            sql_val = sql_tmp
        else:
            sql_val += ',' + sql_tmp
        sql_count += 1
        if sql_count >= 1000:
            sql = sql_insert + ' ' + sql_val
            mcursor.execute(sql)
            sql_count = 0
            sql_val = ''

    if sql_count > 0:
        sql = sql_insert + ' ' + sql_val
        mcursor.execute(sql)

    mysql_conn.commit()
    cursor.close()
    mcursor.close()
    mysql_conn.close()


cs_result_channel_to_mysql = PythonOperator(
    task_id='csresult_channel_to_mysql',
    python_callable=csresult_channel_to_mysql,
    provide_context=True,
    dag=dag
)

cs_result_channel_to_mysql
