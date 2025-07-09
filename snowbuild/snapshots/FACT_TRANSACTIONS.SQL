{% snapshot FACT_TRANSACTIONS %}

{{

 config(
  target_schema='SILVER',
  unique_key='TRANSACTION_ID',
  strategy='timestamp',
  updated_at='TRANSACTION_DATE',
  invalidate_hard_deletes=False



 )
}}


select * from POSTGRES_BATCH.SILVER.FACT_TRANSACTIONS 


{%  endsnapshot %}