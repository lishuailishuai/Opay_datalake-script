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

