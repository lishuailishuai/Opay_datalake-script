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

CREATE EXTERNAL TABLE `server_event`(
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
LOCATION 'ufile://opay-datalake/oride/server_event'


CREATE EXTERNAL TABLE `client_event`(
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
LOCATION 'ufile://opay-datalake/oride/client_event'


CREATE EXTERNAL TABLE `server_magic`(
    `event_name` string,
    `event_values` string
)
PARTITIONED BY (
    `dt` string,
    `hour` string)
ROW FORMAT SERDE
   'org.openx.data.jsonserde.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/oride/server_magic'

