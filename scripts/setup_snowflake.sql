-- Setup script for Snowflake

-- 0. Set Role
USE ROLE SYSADMIN; -- Or whatever role owns the schema

-- 1. Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS airflow_etl_wh
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- 2. Create Database
CREATE DATABASE IF NOT EXISTS airflow_etl_db;

-- 3. Create Schema
CREATE SCHEMA IF NOT EXISTS airflow_etl_db.weather_data;

-- 4. Create Table
CREATE OR REPLACE TABLE airflow_etl_db.weather_data.weather_log (
    dt TIMESTAMP,
    city_name STRING,
    temp FLOAT,
    feels_like FLOAT,
    conditions STRING,
    humidity FLOAT,
    wind_speed FLOAT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Create Stage using the Storage Integration you just created
USE ROLE SYADMIN; 

CREATE OR REPLACE STAGE airflow_etl_db.weather_data.weather_s3_stage
    URL = 's3://airflow-snowflake-etl-data/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
