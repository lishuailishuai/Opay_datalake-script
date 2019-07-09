CREATE EXTERNAL TABLE `data_activity`(
  `id` bigint,
  `activity_name` string,
  `activity_type` int,
  `activity_status` tinyint,
  `activity_preview_img` string,
  `activity_main_img` string,
  `activity_start` bigint,
  `activity_end` bigint,
  `activity_describe` string,
  `activity_jump_url` string,
  `reward_id` int,
  `created_at` string,
  `updated_at` string,
  `validate` tinyint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_activity';


CREATE EXTERNAL TABLE `data_agenter_motorbike`(
  `id` bigint,
  `agenter_id` bigint,
  `motorbike_id` bigint,
  `driver_id` bigint,
  `status` tinyint,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_agenter_motorbike';


CREATE EXTERNAL TABLE `data_app_config`(
  `id` int,
  `start_time` int,
  `end_time` int,
  `mobile` string,
  `pax_cancel_limit` int,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_app_config';


CREATE EXTERNAL TABLE `data_billboard_config`(
  `id` bigint,
  `activity_type` string,
  `name` string,
  `rules` string,
  `begin_time` bigint,
  `end_time` bigint,
  `day_amount` string,
  `week_amount` string,
  `is_show` tinyint,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_billboard_config';



CREATE EXTERNAL TABLE `data_city_conf`(
  `id` int COMMENT '城市 ID',
  `name` string  COMMENT '城市名称',
  `country` string  COMMENT '国家',
  `shape` tinyint COMMENT '形状：1 圆形, 2 多边形',
  `area` string  COMMENT '区域设置数据',
  `serv_type` string COMMENT '开启的服务类型[1,2,99] 1 专车 2 快车 99 招手停',
  `validate` tinyint COMMENT '本条数据是否有效 0 无效，1 有效'
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_city_conf';


CREATE EXTERNAL TABLE `data_coupon`(
  `id` bigint,
  `user_id` bigint,
  `order_id` bigint,
  `name` string,
  `amount` decimal(10,2),
  `max_amount` decimal(10,2),
  `discount` int,
  `start_price` decimal(10,2),
  `type` int,
  `status` int,
  `source` int,
  `start_time` bigint,
  `expire_time` bigint,
  `used_time` bigint,
  `receive_time` bigint,
  `template_id` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_coupon';


CREATE EXTERNAL TABLE `data_coupon_log`(
  `id` bigint,
  `user_id` bigint,
  `amounts` decimal(10,2),
  `ctype` bigint,
  `expire_time` bigint,
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_coupon_log';


CREATE EXTERNAL TABLE `data_coupon_template`(
  `id` int,
  `name` string,
  `coupons` string,
  `budget` bigint,
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_coupon_template';



CREATE EXTERNAL TABLE `data_coupons_template`(
  `id` int,
  `name` string,
  `valid_day` int,
  `amount` decimal(10,2),
  `total` int,
  `is_new_customer` tinyint,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_coupons_template';


CREATE EXTERNAL TABLE `data_device`(
  `role` tinyint,
  `role_id` bigint,
  `device_id` string,
  `platform` tinyint,
  `os_version` string,
  `app_name` string,
  `app_version` string,
  `language` string,
  `notification_token` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_device';


CREATE EXTERNAL TABLE `data_driver`(
  `id` bigint,
  `phone_number` string,
  `password` string,
  `opay_account` string,
  `plate_number` string,
  `real_name` string,
  `birthday` string,
  `gender` tinyint,
  `government` string COMMENT 'Local Government',
  `country` string,
  `city` string,
  `black` tinyint,
  `group_id` int,
  `updated_at` string COMMENT '最后更新时间'
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver';



CREATE EXTERNAL TABLE `data_driver_balance_extend`(
  `id` bigint,
  `driver_id` bigint,
  `balance` decimal(10,2),
  `total_income` decimal(10,2),
  `total_pay` decimal(10,2) ,
  `total_service` decimal(10,2),
  `check_status` tinyint,
  `pay_status` tinyint,
  `payed_at` bigint,
  `checked_at` bigint,
  `success_checked_at` bigint,
  `created_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_balance_extend';


CREATE EXTERNAL TABLE `data_driver_balance_records`(
  `id` bigint,
  `driver_id` bigint,
  `type` tinyint,
  `amount` decimal(10,2),
  `amount_str` string,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_balance_records';


CREATE EXTERNAL TABLE `data_driver_comment`(
  `id` bigint,
  `order_id` bigint,
  `user_id` bigint,
  `driver_id` bigint,
  `score` int,
  `content` string,
  `create_time` bigint,
  `is_grade` tinyint,
  `is_show` tinyint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_comment';


CREATE EXTERNAL TABLE `data_driver_discount`(
  `id` int,
  `driver_id` bigint,
  `reward` decimal(10,2),
  `status` int,
  `create_time` bigint,
  `use_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_discount';


CREATE EXTERNAL TABLE `data_driver_extend`(
  `id` bigint COMMENT '司机 ID',
  `serv_mode` int COMMENT '服务模式 (0: no service, 1: in service)',
  `serv_status` int COMMENT '服务状态 (0: wait assign, 1: pick up, 2: send)',
  `order_rate` int COMMENT '接单率',
  `assign_order` int COMMENT '派单数量',
  `take_order` int COMMENT '接单数量',
  `avg_score` decimal(10,4) COMMENT '平均评分',
  `total_score` int COMMENT '总评分',
  `score_times` int COMMENT '评分次数',
  `last_order_id` bigint COMMENT '最近一个订单的ID',
  `register_time` bigint COMMENT '注册时间',
  `login_time` bigint COMMENT '最后登陆时间',
  `is_bind` tinyint COMMENT '状态 0 未绑定 1 已绑定',
  `first_bind_time` bigint COMMENT '初次绑定时间',
  `total_pay` decimal(10,2) COMMENT '总计-已打款收入',
  `inviter_role` int,
  `inviter_id` bigint,
  `block` int COMMENT '后台管理司机接单状态(0: 允许 1:不允许)',
  `serv_type` tinyint COMMENT '1 专车 2 快车',
  `serv_score` decimal(10,4) COMMENT '司机服务分',
  `local_gov_ids` string COMMENT '行会ID,json',
  `updated_at` string COMMENT '最后更新时间',
  `fault` tinyint COMMENT '故障状态(0:正常1:事故中2:无文件3:扣车中4:维修中5:回炉培训中6:其他)',
  `city_id` bigint COMMENT '所属城市ID',
  `language` string COMMENT '客户端语言',
  `end_service_time` bigint COMMENT '专车司机结束收份子钱时间',
  `last_online_time` bigint COMMENT '最后上线时间',
  `last_offline_time` bigint COMMENT '最后下线时间'
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_extend';


CREATE EXTERNAL TABLE `data_driver_fee_blacklist`(
  `id` bigint,
  `month` int,
  `day` int,
  `checked_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_fee_blacklist';


CREATE EXTERNAL TABLE `data_driver_group`(
  `id` bigint,
  `group_name` string,
  `group_leader` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_group';


CREATE EXTERNAL TABLE `data_driver_operation_log`(
  `id` bigint,
  `operation_type` tinyint,
  `operation_uid` int,
  `driver_id` bigint,
  `recharge_id` bigint,
  `pay_id` bigint,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_operation_log';


CREATE EXTERNAL TABLE `data_driver_pay_records`(
  `id` bigint,
  `driver_id` bigint,
  `amount` decimal(10,2),
  `amount_str` string,
  `status` tinyint,
  `record_days` string,
  `opay_result` string,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_pay_records';


CREATE EXTERNAL TABLE `data_driver_recharge_records`(
 `id` bigint,
  `driver_id` bigint,
  `amount` decimal(10,2),
  `amount_str` string,
  `created_at` bigint,
  `updated_at` bigint,
  `amount_reason` tinyint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_recharge_records';




CREATE EXTERNAL TABLE `data_driver_records_day`(
  `id` bigint,
  `agenter_id` bigint,
  `driver_id` bigint,
  `day` int,
  `count_orders_assign` int,
  `count_orders_all` int,
  `count_orders_finish` int,
  `count_orders_complaint` int,
  `count_orders_reward` int,
  `amount_all` decimal(10,2),
  `amount_pay_online` decimal(10,2) ,
  `amount_pay_offline` decimal(10,2),
  `amount_reward` decimal(10,2),
  `amount_reward_json` string,
  `amount_reward_type_invite` decimal(10,2) ,
  `amount_reward_type_1` decimal(10,2),
  `amount_reward_type_0` decimal(10,2),
  `is_finish_service` tinyint,
  `amount_service` decimal(10,2) ,
  `amount_platform` decimal(10,2),
  `amount_agenter` decimal(10,2),
  `amount_true` decimal(10,2),
  `amount_recharge` decimal(10,2),
  `passenger_orders` int,
  `passenger_amount` decimal(10,2),
  `work_hours` decimal(10,2),
  `payment_status` tinyint,
  `balance_status` tinyint,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_records_day';



CREATE EXTERNAL TABLE `data_driver_reward`(
  `id` bigint,
  `driver_id` bigint,
  `order_id` bigint,
  `reward_type` int,
  `reward_id` bigint,
  `reward_name` string,
  `order_num` int,
  `amount` decimal(10,2),
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_reward';



CREATE EXTERNAL TABLE `data_driver_reward_push`(
  `id` bigint,
  `driver_id` bigint,
  `reward_data` string,
  `reward_type` int,
  `amount` decimal(10,2),
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_driver_reward_push';


CREATE EXTERNAL TABLE `data_fcm_template`(
  `id` int,
  `title` string,
  `content` string,
  `action` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_fcm_template';



CREATE EXTERNAL TABLE `data_invite`(
  `id` int,
  `role` int,
  `uid` bigint,
  `invitee_role` int,
  `invitee_id` bigint,
  `invitee_phone` string,
  `invitee_order` bigint,
  `timestamp` bigint,
  `award` decimal(10,2)
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_invite';



CREATE EXTERNAL TABLE `data_invite_conf`(
  `id` int,
  `type` int,
  `status` int,
  `start_time` bigint,
  `end_time` bigint,
  `inviter_award` string,
  `invitee_award` string,
  `limit` int
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_invite_conf';



CREATE EXTERNAL TABLE `data_motorbike`(
  `id` bigint,
  `plate_number` string,
  `vehicle_number` string,
  `engine_number` string,
  `motor_driver` string,
  `phone_number` string,
  `gps_number` string,
  `gps_phone_number` string,
  `status` tinyint,
  `remarks` string,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_motorbike';



CREATE EXTERNAL TABLE `data_motorbike_extend`(
  `id` bigint,
  `motorbike_id` bigint,
  `assign_at` bigint,
  `check_status` tinyint,
  `belongto_at` bigint,
  `created_at` bigint,
  `updated_at` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_motorbike_extend';



CREATE EXTERNAL TABLE `data_novice_coupons_conf`(
  `id` int,
  `enable` tinyint,
  `start_time` int,
  `end_time` bigint,
  `template_id` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_novice_coupons_conf';



CREATE EXTERNAL TABLE `data_opay_transaction`(
  `reference` string,
  `token` string,
  `transaction_id` string,
  `instrument_id` string,
  `instrument_type` string,
  `amount` decimal(10,2) ,
  `fee` decimal(10,2) ,
  `currency` string,
  `country` string,
  `channel` string,
  `refunded` int,
  `status` string,
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_opay_transaction';



CREATE EXTERNAL TABLE `data_order`(
  `id` bigint COMMENT '订单 ID',
  `user_id` bigint COMMENT '乘客 ID',
  `start_name` string COMMENT '起点名称',
  `start_lng` decimal(10,6) COMMENT '起点经度',
  `start_lat` decimal(10,6) COMMENT '起点纬度',
  `end_name` string COMMENT '终点名称',
  `end_lng` decimal(10,6) COMMENT '终点经度',
  `end_lat` decimal(10,6) COMMENT '终点纬度',
  `duration` bigint COMMENT '订单持续时间',
  `distance` bigint COMMENT '订单距离',
  `basic_fare` decimal(10,2) COMMENT '起步价',
  `dst_fare` decimal(10,2) COMMENT '里程费',
  `dut_fare` decimal(10,2) COMMENT '时长费',
  `dut_price` decimal(10,2) COMMENT '时长价格',
  `dst_price` decimal(10,2) COMMENT '距离价格',
  `price` decimal(10,2) COMMENT '订单价格',
  `reward` decimal(10,2) COMMENT '司机奖励',
  `driver_id` bigint COMMENT '司机 ID',
  `plate_num` string COMMENT '车牌号',
  `take_time` bigint COMMENT '接单时间',
  `wait_time` bigint COMMENT '到达接送点时间',
  `pickup_time` bigint COMMENT '接到乘客时间',
  `arrive_time` bigint COMMENT '到达终点时间',
  `finish_time` bigint COMMENT '订单完成时间',
  `cancel_role` tinyint COMMENT '取消人角色(1: 用户, 2: 司机, 3:系统 4:Admin)',
  `cancel_time` bigint COMMENT '取消时间',
  `cancel_type` int COMMENT '取消原因类型',
  `cancel_reason` string COMMENT '取消原因',
  `status` int COMMENT '订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel)',
  `create_time` bigint COMMENT '创建时间',
  `fraud` tinyint COMMENT '是否欺诈(0否1是)',
  `driver_serv_type` int COMMENT '司机服务类型(1: Direct 2:Street )',
  `serv_type` int COMMENT '订单车辆类型(0: all 1:driect 2: street)',
  `refund_before_pay` int COMMENT '支付前资金调整',
  `refund_after_pay` int COMMENT '支付后资金调整',
  `abnormal` tinyint COMMENT '异常状态(0 否 1 逃单)',
  `flag_down_phone` string COMMENT '招手停上报手机号',
  `zone_hash` string COMMENT '所属区域 hash',
  `updated_at` string COMMENT '最后更新时间',
  `city_id` bigint COMMENT '所属城市'
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_order';



CREATE EXTERNAL TABLE `data_order_payment`(
  `id` bigint,
  `driver_id` bigint,
  `mode` int,
  `price` decimal(10,2),
  `coupon_id` bigint,
  `coupon_name` string,
  `coupon_amount` decimal(10,2),
  `amount` decimal(10,2),
  `bonus` decimal(10,2),
  `balance` decimal(10,2),
  `opay_amount` decimal(10,2),
  `reference` string,
  `currency` string,
  `country` string,
  `status` int,
  `modify_time` bigint,
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_order_payment';


CREATE EXTERNAL TABLE `data_payconf`(
  `id` int,
  `name` string,
  `flag_down_fare` int,
  `mileage_fee` string,
  `duration_fee` string,
  `offline_pay` tinyint,
  `create_date` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_payconf';



CREATE EXTERNAL TABLE `data_promo_code`(
  `id` int,
  `name` string,
  `code` string,
  `coupons_temp_id` int,
  `total` int,
  `start_time` bigint,
  `end_time` bigint,
  `status` tinyint,
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_promo_code';



CREATE EXTERNAL TABLE `data_recharge_conf`(
  `id` int,
  `activity_img` string,
  `activity_url` string,
  `reward_start` bigint,
  `reward_end` bigint,
  `created_at` string,
  `updated_at` string,
  `validate` tinyint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_recharge_conf';


CREATE EXTERNAL TABLE `data_recharge_options`(
  `id` int,
  `recharge_amount` int,
  `reward_amount` int,
  `limit_type` int,
  `limit_times` int,
  `created_at` string,
  `updated_at` string,
  `validate` tinyint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_recharge_options';



CREATE EXTERNAL TABLE `data_reward_conf`(
  `id` int,
  `reward_name` string,
  `reward_city` string,
  `reward_rule` string,
  `reward_start` bigint,
  `reward_end` bigint,
  `join_condition` tinyint,
  `reward_mode` tinyint,
  `is_hide` tinyint,
  `reward_description` string,
  `reward_type` int,
  `validate` tinyint,
  `updated_at` string,
  `created_at` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_reward_conf';



CREATE EXTERNAL TABLE `data_role_invite`(
  `id` bigint,
  `role` int,
  `role_id` bigint,
  `invite_type` int,
  `invite_num` int,
  `invite_complete_num` int,
  `invite_award` int
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_role_invite';



CREATE EXTERNAL TABLE `data_sms_template`(
  `id` int,
  `content` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_sms_template';



CREATE EXTERNAL TABLE `data_user`(
  `id` bigint,
  `phone_number` string,
  `first_name` string,
  `last_name` string,
  `promoter_code` string
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_user';



CREATE EXTERNAL TABLE `data_user_comment`(
  `id` bigint,
  `order_id` bigint,
  `user_id` bigint,
  `driver_id` bigint,
  `score` int,
  `content` string,
  `create_time` bigint,
  `is_grade` tinyint,
  `is_show` tinyint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_user_comment';



CREATE EXTERNAL TABLE `data_user_complaint`(
  `id` bigint,
  `user_id` bigint,
  `driver_id` bigint,
  `type` tinyint,
  `wish_order_status` tinyint,
  `description` string,
  `processing_result` string,
  `status` tinyint,
  `create_time` bigint,
  `processing_time` bigint,
  `complete_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_user_complaint';



CREATE EXTERNAL TABLE `data_user_extend`(
  `id` bigint,
  `take_order` int,
  `avg_score` decimal(10,4),
  `total_score` int,
  `score_times` int,
  `bonus` decimal(10,2),
  `balance` decimal(10,2) ,
  `last_order_id` bigint,
  `register_time` bigint,
  `login_time` bigint,
  `inviter_role` int,
  `inviter_id` bigint,
  `invite_num` int,
  `invite_complete_num` int,
  `invite_award` int
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_user_extend';


CREATE EXTERNAL TABLE `data_user_recharge`(
  `id` bigint,
  `user_id` bigint,
  `recharge_id` bigint,
  `bonus` decimal(10,2) ,
  `balance` decimal(10,2),
  `amount` decimal(10,2) ,
  `reference` string,
  `currency` string,
  `country` string,
  `status` int,
  `modify_time` bigint,
  `create_time` bigint
)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_user_recharge';


CREATE EXTERNAL TABLE `data_abnormal_order`(
  `id` bigint COMMENT '自增id',
  `order_id` bigint COMMENT '涉及order_id',
  `driver_id` bigint COMMENT '司机id',
  `user_id` bigint COMMENT '用户id',
  `behavior_ids` string COMMENT '命中规则id',
  `rule_names` string COMMENT '命中规则名称',
  `is_revoked` tinyint COMMENT '是否撤销，1是，0否',
  `create_time` bigint COMMENT '创建时间',
  `update_time` bigint COMMENT '更新时间',
  `score` decimal(10,2) COMMENT '每单扣除分数',
  `amount` decimal(10,2) COMMENT '扣款金额')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_abnormal_order';


CREATE EXTERNAL TABLE `data_anti_fraud_strategy`(
  `id` int COMMENT 'ID',
  `role` int COMMENT '策略针对的角色：1.乘客,2.司机',
  `behavior` int COMMENT '异常行为',
  `name` string COMMENT '策略名称',
  `tpl_id` int COMMENT '策略模版ID',
  `rules` string COMMENT '策略参数值列表',
  `black_score` int COMMENT '黑名单分值',
  `punish_result` int COMMENT '惩处结果',
  `status` int COMMENT '状态',
  `created_at` string COMMENT '申请时间',
  `updated_at` string COMMENT '更新时间')
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_anti_fraud_strategy';


CREATE EXTERNAL TABLE `appsflyer_opay_install_log`(
    attributed_touch_type string,
    attributed_touch_time string,
    install_time string,
    event_time string,
    event_name string,
    event_value string,
    event_revenue string,
    event_revenue_currency string,
    event_revenue_usd string,
    event_source string,
    is_receipt_validated string,
    partner string,
    media_source string,
    channel string,
    keywords string,
    campaign string,
    campaign_id string,
    adset string,
    adset_id string,
    ad string,
    ad_id string,
    ad_type string,
    site_id string,
    sub_site_id string,
    sub_param_1 string,
    sub_param_2 string,
    sub_param_3 string,
    sub_param_4 string,
    sub_param_5 string,
    cost_model string,
    cost_value string,
    cost_currency string,
    contributor_1_partner string,
    contributor_1_media_source string,
    contributor_1_campaign string,
    contributor_1_touch_type string,
    contributor_1_touch_time string,
    contributor_2_partner string,
    contributor_2_media_source string,
    contributor_2_campaign string,
    contributor_2_touch_type string,
    contributor_2_touch_time string,
    contributor_3_partner string,
    contributor_3_media_source string,
    contributor_3_campaign string,
    contributor_3_touch_type string,
    contributor_3_touch_time string,
    region string,
    country_code string,
    state string,
    city string,
    postal_code string,
    dma string,
    ip string,
    wifi string,
    operator string,
    carrier string,
    language string,
    appsflyer_id string,
    advertising_id string,
    idfa string,
    android_id string,
    customer_user_id string,
    imei string,
    idfv string,
    platform string,
    device_type string,
    os_version string,
    app_version string,
    sdk_version string,
    app_id string,
    app_name string,
    bundle_id string,
    is_retargeting string,
    retargeting_conversion_type string,
    attribution_lookback string,
    reengagement_window string,
    is_primary_attribution string,
    user_agent string,
    http_referrer string,
    original_url string,
    install_app_store string,
    match_type string,
    contributor_1_match_type string,
    contributor_2_match_type string,
    contributor_3_match_type string,
    device_category string,
    google_play_referrer string,
    google_play_click_time string,
    google_play_install_begin_time string,
    amazon_fire_id string,
    keyword_match_type string
)
PARTITIONED BY (
   `dt` string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION
   'ufile://opay-datalake/oride/appsflyer/opay_install_log'
TBLPROPERTIES (
   'skip.header.line.count'='1'
)

CREATE EXTERNAL TABLE `appsflyer_opay_event_log`(
    agency string,
    media_source string,
    campaign string,
    impressions string,
    clicks string,
    ctr string,
    installs string,
    conversion_rate string,
    sessions string,
    loyal_users string,
    loyal_users_installs string,
    total_revenue string,
    total_cost string,
    roi string,
    arpu string,
    average_ecpi string,
    estimated_price_cllick_request_event_counter int,
    oride_cllick_request_event_counter int
)
PARTITIONED BY (
   `dt` string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION
   'ufile://opay-datalake/oride/appsflyer/opay_event_log'
TBLPROPERTIES (
   'skip.header.line.count'='1'
)