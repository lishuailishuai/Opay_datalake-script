CREATE EXTERNAL TABLE `client_event_dev`(
    `ip` string,
    `server_ip` string,
    `timestamp` bigint,
    `common` struct<
        `user_id`:string,
        `user_number`:string,
        `client_timestamp`:string,
        `platform`:string,
        `os_version`:string,
        `app_name`:string,
        `app_version`:string,
        `locale`:string,
        `device_id`:string,
        `device_screen`:string,
        `device_model`:string,
        `device_manufacturer`:string,
        `is_root`:string,
        `channel`:string,
        `subchannel`:string,
        `gaid`:string,
        `appsflyer_id`:string
    >,
    `events` Array<
        struct<
            `event_time`:string,
            `event_name`:string,
            `page`:string,
            `source`:string,
            `event_value`:string
        >
    >
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/oride-dev/client_event'

CREATE EXTERNAL TABLE oride_source.`server_event`(
    `ip` string,
    `server_ip` string,
    `timestamp` bigint,
    `common` struct<
        `user_id`:string,
        `user_number`:string,
        `client_timestamp`:string,
        `platform`:string,
        `os_version`:string,
        `app_name`:string,
        `app_version`:string,
        `locale`:string,
        `device_id`:string,
        `device_screen`:string,
        `device_model`:string,
        `device_manufacturer`:string,
        `is_root`:string,
        `channel`:string,
        `subchannel`:string,
        `gaid`:string,
        `appsflyer_id`:string
    >,
    `events` Array<
        struct<
            `event_time`:string,
            `event_name`:string,
            `page`:string,
            `source`:string,
            `event_value`:string
        >
    >
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/oride_buried/ordm.server_event'


CREATE EXTERNAL TABLE oride_source.`client_event`(
    `ip` string,
    `server_ip` string,
    `timestamp` bigint,
    `common` struct<
        `user_id`:string,
        `user_number`:string,
        `client_timestamp`:string,
        `platform`:string,
        `os_version`:string,
        `app_name`:string,
        `app_version`:string,
        `locale`:string,
        `device_id`:string,
        `device_screen`:string,
        `device_model`:string,
        `device_manufacturer`:string,
        `is_root`:string,
        `channel`:string,
        `subchannel`:string,
        `gaid`:string,
        `appsflyer_id`:string
    >,
    `events` Array<
        struct<
            `event_time`:string,
            `event_name`:string,
            `page`:string,
            `source`:string,
            `event_value`:string
        >
    >
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/oride_buried/ordm.client_event'


CREATE EXTERNAL TABLE oride_source.`server_magic`(
    `event_name` string,
    `event_values` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/oride_buried/ordm.server_magic'


CREATE EXTERNAL TABLE oride_source.`dispatch_tracker_server_magic`(
    `event_name` string,
    `event_values` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/oride_buried/ordm.dispatch_tracker_server_magic'


CREATE EXTERNAL TABLE `anti_fraud`(
    `action` string,
    `driverId` bigint,
    `userId` bigint,
    `isSlient` boolean,
    `reason` int
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/oride-research/anti_fraud'


CREATE EXTERNAL TABLE oride_source.`h5_event`(
    `ip` string,
    `server_ip` string,
    `timestamp` bigint,
    `common` struct<
        `user_id`:string,
        `user_number`:string,
        `client_timestamp`:string,
        `platform`:string,
        `os_version`:string,
        `app_name`:string,
        `app_version`:string,
        `locale`:string,
        `device_id`:string,
        `device_screen`:string,
        `device_model`:string,
        `device_manufacturer`:string,
        `is_root`:string,
        `channel`:string,
        `subchannel`:string,
        `gaid`:string,
        `appsflyer_id`:string
    >,
    `events` Array<
        struct<
            `event_time`:string,
            `event_name`:string,
            `page`:string,
            `source`:string,
            `event_value`:string
        >
    >
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/oride_buried/ordm.h5_event'

CREATE EXTERNAL TABLE oride_dw.`ods_log_oride_driver_skyeye_di`(
    `ver` int,
    `driver_id` int,
    `tag_ids` Array<string>
)
PARTITIONED BY (
    `dt` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/oride-research/tags/driver_tags'


CREATE EXTERNAL TABLE oride_dw.`ods_log_oride_order_skyeye_di`(
    `ver` int,
    `order_id` int,
    `tag_ids` Array<string>
)
PARTITIONED BY (
    `dt` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/oride-research/tags/order_tags'

CREATE EXTERNAL TABLE oride_dw.`ods_log_oride_passenge_skyeye_di`(
    `ver` int,
    `passenge_id` int,
    `tag_ids` Array<string>
)
PARTITIONED BY (
    `dt` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/oride-research/tags/passenge_tags'

CREATE EXTERNAL TABLE oride_source.`algo_accept_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.accept_order_hook'

CREATE EXTERNAL TABLE oride_source.`algo_arrive_order_dest_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.arrive_order_dest_hook';

CREATE EXTERNAL TABLE oride_source.`algo_arrive_order_start_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.arrive_order_start_hook';


CREATE EXTERNAL TABLE oride_source.`algo_assign_sheet_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.assign_sheet_hook';


CREATE EXTERNAL TABLE oride_source.`algo_assign_sheet_timeout_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.assign_sheet_timeout_hook';


CREATE EXTERNAL TABLE oride_source.`algo_cancel_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.cancel_order_hook';

CREATE EXTERNAL TABLE oride_source.`algo_create_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.create_order_hook';


CREATE EXTERNAL TABLE oride_source.`algo_estimate_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.estimate_order_hook';


CREATE EXTERNAL TABLE oride_source.`algo_finish_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.finish_order_hook';

CREATE EXTERNAL TABLE oride_source.`algo_refuse_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.refuse_order_hook';

CREATE EXTERNAL TABLE oride_source.`algo_send_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.send_hook';

CREATE EXTERNAL TABLE oride_source.`algo_send_order_hook`(
    `event_type` int,
    `ts` bigint,
    `user_role` int,
    `city_id` bigint,
    `trip_id` bigint,
    `order_ids` Array<bigint>,
    `driver_id` bigint,
    `driver_location` Struct<lng:float, lat:float>,
    `passenger_id` bigint,
    `extra` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/algo.send_order_hook';

CREATE EXTERNAL TABLE oride_source.`uops_user_user_tag`(
    `user_id` bigint,
    `tag_id` bigint,
    `city_id` bigint,
    `ts` bigint
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/uops_user.user_tag';

CREATE EXTERNAL TABLE oride_source.`uops_user_driver_tag`(
    `user_id` bigint,
    `tag_id` bigint,
    `city_id` bigint,
    `ts` bigint
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 's3a://opay-bi/algorithm_buried/uops_user.driver_tag';



