beeline -u "jdbc:hive2://10.52.5.190:10000/default" -n airflow -e "
create external table opay_dw.dwd_opay_client_event_base_di
(
client_ip string comment '用户ip',
server_ip string comment '服务端ip',
server_time string comment '服务器事件时间',
server_timestamp bigint comment '服务器事件时间戳',
user_id string comment '用户id, 如果用户未登录，传空字符串',
mobile string comment '用户手机号, 如果用户未登录，传空字符串',
city_id string comment '城市id',
client_report_time string comment '客户端向服务器提交事件日志时的时',
client_report_timestamp string comment '客户端向服务器提交事件日志时的时间戳，单位：秒，类似：1469687326',
platform string comment '操作系统平台，取值为Android或者iOS',
os_version string comment '操作系统版本，类似6.0, 9.1.2等',
app_name string comment '客户端名称',
app_version string comment '客户端版本号，比如5.1.4这种类型',
locale string comment '系统使用的语言，格式为：语言代码-区域代码，比如为zh-CN, en-US, en-CA, 语言代码小写，区域代码大写',
device_id string comment '设备id，用于唯一区分设备使用',
device_screen string comment '设备屏幕分辨率，类似1080x1920',
device_model string comment '设备型号，可以具体区分是哪种设备，比如iPhone6, iPhone6s',
device_manufacturer string comment '设备生产商',
is_root string comment 'ios是否越狱/android是否root, 两种取值，y: 已经越狱或root；n: 没有越狱或root',
channel string comment '渠道编号，用于区分是哪个渠道带来的安装，跟AppsFlyer相关',
subchannel string comment '子渠道编号，用于区分是哪个渠道带来的安装，跟AppsFlyer相关',
gaid string comment '广告id, Android使用gaid, iOS使用idfa',
appsflyer_id string comment 'appsflyer的唯一标示',
event_time string comment '事件生成时间',
event_timestamp bigint comment '事件生成时间戳',
event_name string comment '事件名称',
page string comment '页面',
source string comment '来源',
event_value string comment '事件值 json'
) comment 'client_event明细事实表'
partitioned by (country_code string comment '国家码', dt string comment '日期分区')
STORED AS orc
LOCATION 'oss://opay-datalake/opay/opay_dw/dwd_opay_client_event_base_di'
TBLPROPERTIES ('orc.compress'='SNAPPY')
"


select 
	ip as client_ip, server_ip, 
    from_unixtime(`timestamp`, 'yyyy-MM-dd HH:mm:ss') as server_time, `timestamp` as server_timestamp,
    common.user_id, common.user_number as mobile, common.city_id, 
    from_unixtime(cast(common.client_timestamp as BIGINT), 'yyyy-MM-dd HH:mm:ss') as client_report_time, cast(common.client_timestamp as BIGINT) as client_report_timestamp,
    common.platform, common.os_version, common.app_name, common.app_version, common.locale, common.device_id, 
    common.device_screen, common.device_model, common.device_manufacturer, common.is_root, common.channel, common.subchannel, 
    common.gaid, common.appsflyer_id, 
    from_unixtime(cast(cast(event.event_time as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as event_time, 
    cast(cast(event.event_time as bigint) / 1000 as bigint) as event_timestamp,
    event.event_name, event.page, event.source, event.event_value
from (
  select 
  *
  from client_event where dt = '2019-12-21'
) t1 lateral view explode(t1.events) event_value as event;


select 

where (dt = '${dt}' and hour < 23) or (dt=date_sub('${dt}', 1), 'yyyy-MM-dd') and hour = 23)

and create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')







