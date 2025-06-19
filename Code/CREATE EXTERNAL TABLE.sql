CREATE DATABASE SCOPED CREDENTIAL cred_mrunal
WITH
      IDENTITY= 'Managed Identity'


CREATE EXTERNAL DATA SOURCE source_silver
WITH
(

    LOCATION='https://awsstoragedatalakeee.blob.core.windows.net/silver',
    CREDENTIAL= cred_mrunal
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(

    LOCATION='https://awsstoragedatalakeee.blob.core.windows.net/gold',
    CREDENTIAL= cred_mrunal
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.GzipCodec'
)


CREATE EXTERNAL TABLE gold.extsales2
WITH
(
    LOCATION='extsales2',
    DATA_SOURCE=source_gold,
    FILE_FORMAT=format_parquet
)
AS
SELECT*FROM gold.extsales2

CREATE EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION = 'extcustomers', -- Folder or file path in your data lake
    DATA_SOURCE = source_gold, -- The external data source configured in Synapse
    FILE_FORMAT = format_parquet -- Predefined file format in Synapse for Parquet files
)
AS
SELECT * FROM gold.extcustomers





