CREATE EXTERNAL TABLE `businesses` (
    `_id` struct<`$oid`:string>,
    `merchant_id` struct<`$oid`:string>,
    `linked_users` Array<struct<`$oid`:string>>,
    `key_pairs` array<
        struct<
            `public_key`:string,
            `tag`:string,
            `private_key`:struct<
                    `cipher_text`:struct<
                        `$binary`:string,
                        `$type`:string
                    >,
                    `mac`:struct<
                        `$binary`:string,
                        `$type`:string
                    >
            >
        >
    >,
    `webhook_url` string,
    `allowed_to_go_live` boolean,
    `instant_settlement` boolean,
    `skip_commit_stage` boolean,
    `fee` string,
    `pos_transaction_limit` struct<`value`:string, `floatValue`:float, `currency`:string>,
    `email_settings` struct<
        `enable_transaction_notifications`:boolean,
        `enable_transfer_notifications`:boolean,
        `enable_low_balance_notifications`:boolean,
        `balance_notification_sent`:boolean,
        `balance_limit`:struct<
            `value`:string,
            `floatValue`:float,
            `currency`:string
        >
    >
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/businesses'


CREATE EXTERNAL TABLE `business_users` (
    `_id` struct<`$oid`:string>,
    `linked_businesses` Array<struct<`$oid`:string>>
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/business_users'


CREATE EXTERNAL TABLE `chain_boxes` (
    `_id` struct<`$oid`:string>,
    `version` string,
    `signature` struct<
        `$binary`:string,
        `$type`:string
    >,
    `transactions` array<
        struct<
            `transaction`:struct<
                `_id` :struct<`$oid`:string>,
                `version`:string,
                `incoming`:boolean,
                `opcode`:int,
                `type`:string,
                `account`:string,
                `timestamp`:struct<
                    `$date`:string
                >,
                `description`:string,
                `ext_ref`:string,
                `currency_amount`:struct<
                    `value`:string,
                    `floatValue`:float,
                    `currency`:string
                >
            >,
             `string`:string,
             `meta`:struct<
                `session_id`:string,
                `ip_address`:string,
                `xff`:array<string>,
                `user_agent`:string
             >,
             `signature`:struct<
                 `$binary`:string,
                 `$type`:string
             >
        >
    >
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/opay_mongodb/chain_boxes'


CREATE EXTERNAL TABLE `chains` (
    `_id` struct<`$oid`:string>,
    `version` string,
    `deleted` boolean,
    `box_ids` Array<struct<`$oid`:string>>,
    `lock` string,
    `updated_at` struct<`$date`:string>
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/chains'


CREATE EXTERNAL TABLE `fees` (
    `_id` struct<`$oid`:string>,
    `merchant_id` string,
    `business_id` struct<`$oid`:string>,
    `NG` struct<
        `gateway`:struct<
             `fixed`:string,
             `relative`:string,
             `chargeType`:string,
             `max`:string
        >,
        `tokenization`:struct<
            `fixed`:string,
            `relative`:string,
            `chargeType`:string,
            `max`:string
        >,
        `bank`:struct<
            `fixed`:string,
            `relative`:string,
            `chargeType`:string,
            `max`:string
        >
    >
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/fees'

CREATE EXTERNAL TABLE `financial_institutions` (
    `_id` struct<`$oid`:string>,
    `alt_code` string,
    `code` string,
    `display_name` string,
    `category_code` string,
    `category` string,
    `country` string,
    `update_batch_number` string
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/financial_institutions'


CREATE EXTERNAL TABLE `merchants` (
    `_id` struct<`$oid`:string>
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/merchants'


CREATE EXTERNAL TABLE `payment_order_data` (
    `_id` struct<`$oid`:string>,
    `order_id` string,
    `account` string,
    `paybill` string,
    `hmac` struct<`$binary`:string, `$type`:string>
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/payment_order_data'


CREATE EXTERNAL TABLE `settlements` (
    `_id` struct<`$oid`:string>,
    `type` string,
    `country` string,
    `transactions` array<
        struct<`$oid`:string>
    >,
    `timestamp` struct<`$date`:string>,
    `amount` struct<
        `value`:string,
        `floatValue`:float,
        `currency`:string
    >,
    `business` struct<
        `_id`:struct<`$oid`:string>,
        `merchant_id`:struct<`$oid`:string>,
        `linked_users`:array<struct<`$oid`:string>>,
        `name`:string,
        `key_pairs`:array<
            struct<
                `public_key`:string,
                `tag`:string,
                `private_key`:struct<
                        `cipher_text`:struct<
                            `$binary`:string,
                            `$type`:string
                        >,
                        `mac`:struct<
                            `$binary`:string,
                            `$type`:string
                        >
                >
            >
        >,
        `webhook_url`:string,
        `active_environment`:string,
        `skip_commit_stage`:string,
        `fee`:string,
        `countries`:string
    >
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/settlements'



CREATE EXTERNAL TABLE `terminal_owners` (
    `_id` struct<`$oid`:string>,
    `terminal_id` struct<`$oid`:string>,
    `pos_id` string,
    `serial` string,
    `owner_id` struct<`$oid`:string>,
    `owner_type` string,
    `created` struct<`$date`:string>
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/terminal_owners'


CREATE EXTERNAL TABLE `terminals` (
    `_id` struct<`$oid`:string>,
    `payment_terminal_provider_id` struct<`$oid`:string>,
    `pos_id` string,
    `terminal_id` string,
    `created` struct<`$date`:string>,
    `bank` string
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION 'ufile://opay-datalake/opay_mongodb/terminals'


CREATE EXTERNAL TABLE `transactions` (
    `_id` struct<`$oid`:string>,
    `state` struct<
        `type`:string,
        `failed`:boolean,
        `closed`:boolean,
        `refunded`:boolean,
        `charged_back`:boolean,
        `close_reason`:string,
        `refund_external_reason`:string,
        `refund_internal_reason`:string,
        `chargeback_external_reason`:string,
        `chargeback_internal_reason`:string,
        `message`:string,
        `updated_at`:struct<`$date`:string>,
        `auto_refundable`:boolean
    >,
    `timestamp` struct<`$date`:string>,
    `version` string,
    `business` struct<
        `_id`:struct<`$oid`:string>,
        `name`:string
    >,
    `payment_data` struct<
        `country`:string,
        `service_type`:string,
        `service_id`:string,
        `customer_phone`:string,
        `customer_email`:string,
        `recipient_bank`:struct<
            `code`:string,
            `name`:string,
            `imagesrc`:string,
            `displayname`:string,
            `country`:string,
            `altcode`:string
        >,
        `username`:string,
        `recipient`:struct<
            `_id`:struct<`$oid`:string>,
            `version`:string,
            `type`:string
            -- user object
        >,
        `recipient_name`:string,
        `push_type`:int,
        `multiplier`:struct<
            `coefficient`:struct<`value`:string>,
            `active`:boolean
        >,
        `campaign_tag`:string,
        `cashback`:struct<
            `coefficient`:struct<`value`:string>,
            `active`:boolean
        >,
        `cashback_box_name`:string,
        `ext_ref`:string,
        `gateway_type`:string
    >,
    -- instrument
    `amount` struct<
        `value`:string,
        `currency`:string,
        `floatValue`:float
    >,
    `fee` struct<
        `fixed`:string,
        `relative`:string,
        `max`:string,
        `min`:string,
        `chargeType`:string,
        `net`:string
    >,
    `amount_hash` string,
    `details` string,
    `session_id` string,
    `ip_address` string,
    `xff` array<string>,
    `user_agent` string,
    `request_origin` string,
    -- user
    `user` struct<
        `_id`:struct<`$oid`:string>,
        phonenumber:string
    >,
    `initiator` string,
    -- sender,
    `hmac` struct<`$binary`:string, `$type`:string>,
    `lock` struct<
        `by`:struct<`$oid`:string>,
        `by_runner`:struct<`$oid`:string>,
        `until`:struct<`$date`:string>
    >
) PARTITIONED BY ( `dt` string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
with SERDEPROPERTIES("ignore.malformed.json"="true")
LOCATION 'ufile://opay-datalake/opay_mongodb/transactions'


