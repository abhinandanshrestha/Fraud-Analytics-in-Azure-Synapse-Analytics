--create a new schema called "synapse" in your SQL pool (Warehouse)
CREATE SCHEMA synapse; 

/* Create a master key in the SQL pool and encrypts it using a password that you provide. 
The master key is used to protect other keys and secrets stored in the SQL pool, such as column-level encryption keys. */
-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<Password>'
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Test@12345'


/* 
Create a database scoped credential in Azure Synapse Analytics. 
This credential is used to securely store and manage the necessary credentials required to access an external Azure Storage account.

        CREATE DATABASE SCOPED CREDENTIAL AzureStorageAccountKey
        WITH IDENTITY = '<Storage Account Name>',
        SECRET = '<Storage Account Access Key>';

*/
CREATE DATABASE SCOPED CREDENTIAL AzureStorageAccountKey
WITH IDENTITY = 'frauddetectiontuts',
SECRET = '0zsdDrkJfrNAyGS8cDou3O4koBmtV4yj+AJc1bVD5yJmuN7HbEuaY4x3NN+H5yrOaarTO6JoyN8q+AStUB+XLQ==';


/* Create an external data source named CSVDATASOURCE in Azure Synapse Analytics using Scoped Credentials. 
The external data source is configured to access data stored in Azure Blob Storage. 

    CREATE EXTERNAL DATA SOURCE CSVDATASOURCE WITH (
        TYPE=HADOOP,
        LOCATION = 'wasbs://synapse@<Storage Account Name>.blob.core.windows.net',
        CREDENTIAL = AzureStorageAccountKey
    );

*/
-- DROP EXTERNAL DATA SOURCE CSVDATASOURCE;

CREATE EXTERNAL DATA SOURCE CSVDATASOURCE WITH (
        TYPE=HADOOP,
        LOCATION = 'wasbs://files@frauddetectiontuts.blob.core.windows.net',
        CREDENTIAL = AzureStorageAccountKey
);



-- create two external file formats (CSV1 and CSV2) in Azure Synapse Analytics with different format options
CREATE EXTERNAL FILE FORMAT CSV1
WITH(
    FORMAT_TYPE=DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR=',',
        STRING_DELIMITER='""',
        FIRST_ROW=2,
        USE_TYPE_DEFAULT=TRUE
    )
);
GO

CREATE EXTERNAL FILE FORMAT CSV2
WITH(
    FORMAT_TYPE=DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR=',',
        STRING_DELIMITER='',
        DATE_FORMAT='',
        USE_TYPE_DEFAULT=FALSE
    )
);
GO

-- Create External table using external file format
CREATE EXTERNAL TABLE synapse.exCreditCard
(
    [Time] FLOAT,
    [V1] FLOAT,
    [V2] FLOAT,
    [V3] FLOAT,
    [V4] FLOAT,
    [V5] FLOAT,
    [V6] FLOAT,
    [V7] FLOAT,
    [V8] FLOAT,
    [V9] FLOAT,
    [V10] FLOAT,
    [V11] FLOAT,
    [V12] FLOAT,
    [V13] FLOAT,
    [V14] FLOAT,
    [V15] FLOAT,
    [V16] FLOAT,
    [V17] FLOAT,
    [V18] FLOAT,
    [V19] FLOAT,
    [V20] FLOAT,
    [V21] FLOAT,
    [V22] FLOAT,
    [V23] FLOAT,
    [V24] FLOAT,
    [V25] FLOAT,
    [V26] FLOAT,
    [V27] FLOAT,
    [V28] FLOAT,
    [Amount] FLOAT,
    [Class] BIGINT,
    [id] BIGINT
)
WITH (
    LOCATION = 'CreditCard.csv',
    DATA_SOURCE = [CSVDATASOURCE],
    FILE_FORMAT = [CSV1]
);
GO

-- Create external table to store a machine learning model in a binary format
CREATE EXTERNAL TABLE synapse.[MLModelExt](
    [Model] [VARBINARY](max)NULL
)
WITH
(
    LOCATION='credit_card_model.onnx.hex',
    DATA_SOURCE=[CSVDATASOURCE],
    FILE_FORMAT=CSV2,
    REJECT_TYPE=VALUE,
    REJECT_VALUE=0
);
GO

-- * Use onnx model which we have registered as external table to predict if the transaction is fraud or not

-- 1. Declare a variable named @modelexample of type VARBINARY(max) and assigning it the value retrieved from the Model column of the MLModelExt external table in the synapse schema of your Azure Synapse Analytics
DECLARE @modelexample VARBINARY(max)= (SELECT [Model] FROM synapse.[MLModelExt]);

-- 2.  Perform a prediction using the machine learning model stored in the @modelexample variable, and then inserts the predicted results into a new table named CreditCard in the synapse schema
SELECT 
d.*, p.*
INTO synapse.CreditCard
FROM PREDICT(MODEL=@modelexample,
    Data=synapse.exCreditCard as d,
    runtime =onnx) with (output_label bigint) as p


-- Query newly created Credit Card table
SELECT TOP (100) [Time]
,[V1]
,[V2]
,[V3]
,[V4]
,[V5]
,[V6]
,[V7]
,[V8]
,[V9]
,[V10]
,[V11]
,[V12]
,[V13]
,[V14]
,[V15]
,[V16]
,[V17]
,[V18]
,[V19]
,[V20]
,[V21]
,[V22]
,[V23]
,[V24]
,[V25]
,[V26]
,[V27]
,[V28]
,[Amount]
,[Class]
,[id]
,[output_label]
 FROM [synapse].[CreditCard]