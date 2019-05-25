CREATE EXTERNAL TABLE `db_data_user_extend`(
  `id` bigint,
  `take_order` int,
  `avg_score` float,
  `total_score` int ,
  `score_times` int,
  `bonus` float ,
  `balance` float ,
  `last_order_id` bigint ,
  `register_time` bigint ,
  `login_time` bigint ,
  `inviter_role` int,
  `inviter_id` bigint,
  `invite_num` int,
  `invite_complete_num` int,
  `invite_award` int)
PARTITIONED BY (
  `dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'ufile://opay-datalake/oride/db/data_user_extend'


