CREATE DATABASE <your_db_name>;
CREATE SCHEMA <your_schema_name>;

-- creating table
CREATE OR REPLACE TABLE <your_table_name> (
    job_id VARCHAR,
    job_title VARCHAR,
    company VARCHAR,
    job_location VARCHAR,
    min_salary NUMBER,
    max_salary NUMBER,
    employment_type VARCHAR,
    source VARCHAR,
    url VARCHAR,
    date_posted DATE,
    description VARCHAR
);

-- Creating comma-separated FF for CSVs
CREATE OR REPLACE FILE FORMAT PIPE_SEPARATED_FF
TYPE = 'CSV'
FIELD_DELIMITER = '|'
SKIP_HEADER = 1
SKIP_BLANK_LINES = True
EMPTY_FIELD_AS_NULL = true
TRIM_SPACE = True;

-- Create external stage for AWS S3
CREATE STAGE jobs_stage
  URL='<s3-uri>'
  CREDENTIALS=(AWS_KEY_ID='<aws-access-key>' AWS_SECRET_KEY='<aws-secret-key>');