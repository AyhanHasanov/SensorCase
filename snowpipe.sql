CREATE DATABASE IF NOT EXISTS  sensorcase;
USE sensorcase;

CREATE STORAGE INTEGRATION IF NOT EXISTS snowflake_si
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::385554867724:role/snowflake-si-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://sensorcase/sensors/');

DESC INTEGRATION snowflake_si;

CREATE TABLE IF NOT EXISTS raw_data(
    file_name STRING,
    json_body STRING,
    file_content_key STRING,
    file_last_modified TIMESTAMP_TZ,
    start_scan_time TIMESTAMP_TZ
);

CREATE FILE FORMAT IF NOT EXISTS json_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO';

CREATE OR REPLACE STAGE aws_ext_stage
    URL = 's3://sensorcase/sensors/'
    STORAGE_INTEGRATION = snowflake_si
    FILE_FORMAT = (TYPE = JSON);

LIST @aws_ext_stage;

CREATE PIPE IF NOT EXISTS sensor_data_pipe
AUTO_INGEST = TRUE
AS COPY INTO raw_data(file_name, json_body, file_content_key, file_last_modified, start_scan_time)
FROM(
    SELECT
        metadata$filename AS file_name,
        $1 AS json_body,
        metadata$file_content_key AS file_content_key,
        CONVERT_TIMEZONE('UTC', metadata$file_last_modified) AS file_last_modified,
        CONVERT_TIMEZONE('UTC', metadata$start_scan_time) AS start_scan_time
    FROM @aws_ext_stage/
)
FILE_FORMAT = JSON_FORMAT;

SHOW PIPES;

SELECT
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$FILE_CONTENT_KEY,
    METADATA$FILE_LAST_MODIFIED,
    METADATA$START_SCAN_TIME
FROM @aws_ext_stage/;

CREATE OR REPLACE STREAM raw_data_stream ON TABLE raw_data;

CREATE OR REPLACE TASK insert_into_stable_records
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('raw_data_stream')
AS
INSERT INTO stable_records (sensor_id, timestamp, temperature, frequency, energy_output, energy_conversion_efficiency, status)
SELECT
    f.value:sensor_id::VARCHAR(255) AS sensor_id,
    TO_TIMESTAMP(f.value:timestamp::STRING) AS timestamp,
    f.value:temperature::FLOAT AS temperature,
    f.value:frequency::FLOAT AS frequency,
    f.value:energy_output::FLOAT AS energy_output,
    f.value:energy_conversion_efficiency::FLOAT AS energy_conversion_efficiency,
    f.value:status::VARCHAR(255) AS status
FROM raw_data_stream r,
LATERAL FLATTEN(input => PARSE_JSON(r.json_body)) f;

ALTER TASK insert_into_stable_records RESUME;

SHOW TASKS;
SHOW STREAMS;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE name = 'INSERT_INTO_STABLE_RECORDS'
ORDER BY scheduled_time asc;

-- select * from raw_data;
-- list @aws_ext_stage;
-- select * from stable_records order by 2 asc, 1 asc;


