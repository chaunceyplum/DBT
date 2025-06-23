

-- Summary of the Workflow:
-- New data is streamed into the incoming_records table.
-- A Snowflake Stream tracks the new rows in incoming_records.
-- The stored procedure processes new records from the stream:
-- It checks if the email exists in the customers table.
-- If the email does not exist in the customers table, it is added to the prospects table.
-- A Snowflake Task automates the execution of the stored procedure, running at regular intervals to process new data.
-- This solution efficiently handles streaming data and ensures that your use case—separating prospects from customers—is managed in near real-time within Snowflake.

USE WAREHOUSE CP;

ALTER WAREHOUSE CP
SET WAREHOUSE_SIZE=XSMALL;
-- DATABASE CREATION
CREATE DATABASE STREAMING_SOLUTION;
USE DATABASE STREAMING_SOLUTION;
-- SCHEMA CREATION (A SCHEMA IS A LOGICAL GROUPING OF TABLES)
CREATE SCHEMA LIVE;
USE SCHEMA LIVE;

CREATE TABLE IF NOT EXISTS  INCOMING_PROFILE(

    PERSON_ID VARCHAR(255) UNIQUE,
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    EMAIL VARCHAR(255) UNIQUE,
    CRM_ID VARCHAR(255),
    TIMESTAMP TIMESTAMP_NTZ NOT NULL
);



-- CUSTOMER TABLE CREATION
CREATE TABLE IF NOT EXISTS  CUSTOMER(
    PERSON_ID VARCHAR(255) UNIQUE,
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    EMAIL VARCHAR(255) UNIQUE,
    CRM_ID VARCHAR(255),
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    INGESTED_AT TIMESTAMP_NTZ 
);

-- PROSPECT TABLE CREATION
CREATE TABLE IF NOT EXISTS  PROSPECT(
    PERSON_ID VARCHAR(255) UNIQUE,
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    EMAIL VARCHAR(255) UNIQUE,
    CRM_ID VARCHAR(255),
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    INGESTED_AT TIMESTAMP_NTZ 
);

ALTER TABLE INCOMING_PROFILE 
MODIFY COLUMN TIMESTAMP NOT NULL;

ALTER TABLE CUSTOMER 
MODIFY COLUMN TIMESTAMP NOT NULL;

ALTER TABLE PROSPECT 
MODIFY COLUMN TIMESTAMP NOT NULL;

-- STREAM CREATION SO DATA CAN BE PROCESSED INCREMENTALLY
CREATE OR REPLACE STREAM INCOMING_PROFILE_STREAM
ON TABLE INCOMING_PROFILE
SHOW_INITIAL_ROWS = TRUE;


-- CREATED A STORED PROCEDURE TO READ DATA IN STREAM
CREATE OR REPLACE PROCEDURE UPDATE_PROSPECTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert new prospects from the stream
    INSERT INTO PROSPECT
    (PERSON_ID, FIRST_NAME, LAST_NAME, EMAIL, CRM_ID, TIMESTAMP)
    SELECT 
    INCOMING_PROFILE_STREAM.PERSON_ID,
    INCOMING_PROFILE_STREAM.FIRST_NAME,
    INCOMING_PROFILE_STREAM.LAST_NAME,
    INCOMING_PROFILE_STREAM.EMAIL,
    INCOMING_PROFILE_STREAM.CRM_ID,
    INCOMING_PROFILE_STREAM.TIMESTAMP 
    FROM INCOMING_PROFILE_STREAM 
    LEFT JOIN CUSTOMER
    ON INCOMING_PROFILE_STREAM.EMAIL = CUSTOMER.EMAIL
    WHERE CUSTOMER.EMAIL IS NULL  -- Only if the email is not in customers
    AND NOT EXISTS (
        SELECT 1
        FROM PROSPECT
        WHERE PROSPECT.EMAIL = INCOMING_PROFILE_STREAM.EMAIL
    );  -- Avoid duplicates in prospects

    RETURN 'Prospects updated with new records from stream!';
END;
$$;

-- SNOWFLAKE TASK TO AUTOMATE THE PROCESS 


CREATE OR REPLACE TASK UPDATE_PROSPECTS_TASK
  WAREHOUSE = 'CP'  -- Use your Snowflake warehouse
  SCHEDULE = '1440 MINUTE'  -- Adjust to your preferred interval
  COMMENT = 'Task to update prospects with streamed data'
AS
  CALL UPDATE_PROSPECTS();

-- STARTS THE TASK

ALTER TASK UPDATE_PROSPECTS_TASK RESUME;

-- suspends the task
ALTER TASK UPDATE_PROSPECTS_TASK SUSPEND;

select count(email) from incoming_profile;

-- MONITOR STREAM AND TASKS
SHOW TASKS;


-- Summary of the Workflow:
-- New data is streamed into the incoming_records table.
-- A Snowflake Stream tracks the new rows in incoming_records.
-- The stored procedure processes new records from the stream:
-- It checks if the email exists in the customers table.
-- If the email does not exist in the customers table, it is added to the prospects table.
-- A Snowflake Task automates the execution of the stored procedure, running at regular intervals to process new data.
-- This solution efficiently handles streaming data and ensures that your use case—separating prospects from customers—is managed in near real-time within Snowflake.









-- part 2
-- Complete Example Workflow
-- Create UDF to validate emails.
-- Create a stored procedure to apply the validation across the table.
-- Add a column in the table to store results (optional).
-- Create and schedule a Snowflake Task to automate running the procedure.
-- Monitor and manage the task to ensure it runs smoothly.
-- This setup provides a fully automated solution for validating emails in a Snowflake table on a regular schedule.


ALTER TABLE CUSTOMER 
ADD COLUMN IS_EMAIL_VALID BOOLEAN;

-- BUILT A UDF TO VALIDATE EMAILS

CREATE OR REPLACE FUNCTION EMAIL_VALIDATION_FUNC(EMAIL STRING)
  RETURNS BOOLEAN
  LANGUAGE SQL
  AS
$$
  REGEXP_LIKE(EMAIL, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
$$;


-- CREATED A STORED FUNCTION TO PROCESS EMAIL

CREATE OR REPLACE PROCEDURE EMAIL_VALIDATION_ON_CUSTOMER()
  RETURNS STRING
  LANGUAGE SQL
  EXECUTE AS CALLER
  AS
$$
  BEGIN
    -- Update each record's email validity in a new column
    UPDATE CUSTOMER
    SET IS_EMAIL_VALID = EMAIL_VALIDATION_FUNC(EMAIL);

    RETURN 'Email validation completed successfully';
  END;
$$;

-- CREATED A TASK TO AUTOMATE THE PROCESS

CREATE OR REPLACE TASK EMAIL_VALIDATION_TASK
  WAREHOUSE = CP -- Replace with your warehouse
  SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Runs daily at midnight (UTC)
AS
  CALL EMAIL_VALIDATION_ON_CUSTOMER();


ALTER TASK EMAIL_VALIDATION_TASK SUSPEND;
ALTER TASK UPDATE_PROSPECTS_TASK suspend;


SHOW TASKS;

-- Complete Example Workflow
-- Create UDF to validate emails.
-- Create a stored procedure to apply the validation across the table.
-- Add a column in the table to store results (optional).
-- Create and schedule a Snowflake Task to automate running the procedure.
-- Monitor and manage the task to ensure it runs smoothly.
-- This setup provides a fully automated solution for validating emails in a Snowflake table on a regular schedule.


CREATE OR REPLACE PIPE AUTO_iNGEST_PROFILE
AUTO_INGEST = TRUE
AS
COPY INTO STREAMING_SOLUTION.LIVE.INCOMING_PROFILE
FROM @STREAMING_SOLUTION.LIVE.S3_PROFILE_STAGE
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'CONTINUE';

CREATE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::129579558702:role/mysnowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://baisleylake/AEP/', 's3://baisleylake/*');
  -- [ STORAGE_BLOCKED_LOCATIONS = ('<protocol>://<bucket>/<path>/', '<protocol>://<bucket>/<path>/') ]

DESC INTEGRATION S3_INTEGRATION;

alter stage s3_profile_stage
set storage_integration = S3_INTEGRATION;

CREATE OR REPLACE user snowpipe_user;
GRANT ROLE ACCOUNTADMIN TO user snowpipe_user;
show pipes;

arn:aws:sqs:us-east-1:120569628294:sf-snowpipe-AIDARYEUCQ2DF3YIQBMOC-bibq0Rgj6qSj8VmIiYNe8w

select system$get_aws_sns_iam_policy('arn:aws:sqs:us-east-1:129579558702:UPDATED_BUCKET');

USE ROLE ACCOUNTADMIN;
SELECT * FROM information_schema  ;

SELECT * FROM COPY_HISTORY;
(
      TABLE_NAME => 'INCOMING_PROFILE'
);


INSERT INTO CUSTOMER 
(PERSON_ID, FIRST_NAME,LAST_NAME,EMAIL,CRM_ID, TIMESTAMP)
VALUES
('29059cd2-3147-46b1-b827-08da5d7a0dee',	'Alisander',	'Carbry',	'acarbry1@statcounter.com',	'8e5755b1-e0dd-414a-991f-dead50eb13fc',	'2021-7-1T8:43:28.123456'
);

SELECT * FROM TASK_HISTORY(
      -- [ SCHEDULED_TIME_RANGE_START => <constant_expr> ]
      -- [, SCHEDULED_TIME_RANGE_END => <constant_expr> ]
      -- [, RESULT_LIMIT => <integer> ]
      [, TASK_NAME = 'UPDATE_PROSPECTS_TASK' ]
      [, ERROR_ONLY = TRUE  ]
       )



INSERT INTO PROSPECT
    (PERSON_ID, FIRST_NAME, LAST_NAME, EMAIL, CRM_ID, TIMESTAMP)
    SELECT 
    INCOMING_PROFILE_STREAM.PERSON_ID,
    INCOMING_PROFILE_STREAM.FIRST_NAME,
    INCOMING_PROFILE_STREAM.LAST_NAME,
    INCOMING_PROFILE_STREAM.EMAIL,
    INCOMING_PROFILE_STREAM.CRM_ID,
    INCOMING_PROFILE_STREAM.TIMESTAMP 
    FROM INCOMING_PROFILE_STREAM 
    LEFT JOIN CUSTOMER
    ON INCOMING_PROFILE_STREAM.EMAIL = CUSTOMER.EMAIL
    WHERE CUSTOMER.EMAIL IS NULL  -- Only if the email is not in customers
    AND NOT EXISTS (
        SELECT 1
        FROM PROSPECT
        WHERE PROSPECT.EMAIL = INCOMING_PROFILE_STREAM.EMAIL
    );  -- Avoid duplicates in prospects



    SELECT COUNT(EMAIL) FROM PROSPECT;

GRANT USAGE ROLE TO USER DHANESHS;


DELETE  FROM  PROSPECT WHERE 1=1;
DELETE  FROM INCOMING_PROFILE WHERE 1=1;
SELECT * FROM INCOMING_PROFILE;
