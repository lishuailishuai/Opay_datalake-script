CREATE EXTERNAL TABLE okash_dw.`ods_log_client_hi`(
    `common` struct<
        `user_id`:string,
        `is_newuser`:string,
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
        `appsflyer_id`:string,
        `longtitude`:string,
        `latitude`:string
    >,
    `events` Array<
        struct<
            `event_time`:string,
            `event_name`:string,
            `page`:string,
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
LOCATION 'ufile://okash/okash/okash/client'