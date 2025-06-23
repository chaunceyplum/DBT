CREATE DATABASE postgres
USE DATABASE postgres_batch;
USE WAREHOUSE BETBACK;


-- STAGE CREATION
CREATE OR REPLACE STORAGE INTEGRATION iceberg_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::129579558702:role/mysnowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://baisleylake/rds/Iceberg_lakehouse/');




CREATE OR REPLACE CATALOG INTEGRATION iceberg_glue_catalog_integration
  CATALOG_SOURCE = GLUE
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::129579558702:role/mysnowflakerole'
  GLUE_CATALOG_ID = '129579558702'
  GLUE_REGION = 'us-east-1'
  CATALOG_NAMESPACE = 'iceberg'
  ENABLED = TRUE
  REFRESH_INTERVAL_SECONDS = 3600
  COMMENT = 'Integration for Iceberg tables in AWS Glue catalog';

  
CREATE OR REPLACE FILE FORMAT PARQUET
  TYPE = PARQUET;

CREATE OR REPLACE STAGE ICEBERG_STAGE
 URL = 's3://baisleylake/rds/Iceberg_lakehouse/'
FILE_FORMAT = PARQUET
STORAGE_INTEGRATION = iceberg_s3_integration;

LIST @ICEBERG_STAGE


SELECT * FROM @ICEBERG_STAGE/person/data/
  (FILE_FORMAT => PARQUET);

CREATE OR REPLACE EXTERNAL CATALOG iceberg_catalog
  CATALOG_INTEGRATION = iceberg_glue_catalog_integration;

CREATE OR REPLACE ICEBERG TABLE person_iceberg
EXTERNAL_VOLUME = 'iceberg_external_volume'
CATALOG = 'iceberg_glue_catalog_integration'
CATALOG_NAMESPACE = 'postgres'
CATALOG_TABLE_NAME = 'person'
COMMENT = 'Iceberg table for persons';

CREATE OR REPLACE ICEBERG TABLE product_iceberg
EXTERNAL_VOLUME = 'iceberg_external_volume'
CATALOG = 'iceberg_glue_catalog_integration'
CATALOG_NAMESPACE = 'postgres'
CATALOG_TABLE_NAME = 'product'
COMMENT = 'Iceberg table for products';

CREATE OR REPLACE ICEBERG TABLE order_item_iceberg
EXTERNAL_VOLUME = 'iceberg_external_volume'
CATALOG = 'iceberg_glue_catalog_integration'
CATALOG_NAMESPACE = 'postgres'
CATALOG_TABLE_NAME = 'orderitem'
COMMENT = 'Iceberg table for order items';

CREATE OR REPLACE ICEBERG TABLE transaction_iceberg
EXTERNAL_VOLUME = 'iceberg_external_volume'
CATALOG = 'iceberg_glue_catalog_integration'
CATALOG_NAMESPACE = 'postgres'
CATALOG_TABLE_NAME = 'transactions'
COMMENT = 'Iceberg table for transactions';

select COUNT(*) from PRODUCT_iceberg;









  



