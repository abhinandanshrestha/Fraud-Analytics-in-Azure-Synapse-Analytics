-- Create a new serverless database
Create database Synapse_serverless

-- Create a new VIEW table for the analysis of Fraud by city, country and creditcardoutput csv  
CREATE VIEW dbo.CreditCardLonLat AS
SELECT 
    credit.Time,
    city.name,
    city.lon,
    city.lat,
    city.alpha2,
    country.companyen,
    credit.V1,
    credit.V2,
    credit.V3,
    credit.V4,
    credit.V5,
    credit.V6,
    credit.V7,
    credit.V8,
    credit.V9,
    credit.V10,
    credit.V11,
    credit.V12,
    credit.V13,
    credit.V14,
    credit.V15,
    credit.V16,
    credit.V17,
    credit.V18,
    credit.V19,
    credit.V20,
    credit.V21,
    credit.V22,
    credit.V23,
    credit.V24,
    credit.V25,
    credit.V26,
    credit.V27,
    credit.V28,
    credit.Amount,
    credit.Class,
    credit.id
FROM
OPENROWSET(
    BULK 'https://frauddetectiontuts.blob.core.windows.net/files/creditoutput.csv',
    FORMAT='CSV',
    FIELDTERMINATOR=',',
    FIRSTROW=2,
    ESCAPECHAR='\\'
)
WITH(
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
) AS [credit]
LEFT JOIN 
OPENROWSET(
    BULK 'https://frauddetectiontuts.blob.core.windows.net/files/CityList.csv',
    FORMAT='CSV',
    FIELDTERMINATOR=',',
    FIRSTROW=2,
    ESCAPECHAR='\\'
)
WITH(
    [id] VARCHAR(20),
    [name]VARCHAR(100) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
    [state] varchar(10),
    [alpha2] varchar(2),
    [lon] float,
    [lat] float    
) AS [city] ON credit.id=city.id
LEFT JOIN 
OPENROWSET(
    BULK 'https://frauddetectiontuts.blob.core.windows.net/files/CountryList.csv',
    FORMAT='CSV',
    FIELDTERMINATOR=',',
    FIRSTROW=2,
    ESCAPECHAR='\\'
)
WITH(
    [companyjp] varchar(20) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
     [companyen] varchar(100) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
     [numeric] DECIMAL,
     [alpha3] VARCHAR(3),
     [alpha2] varchar(2),
     [location] varchar(100) collate Latin1_General_100_CI_AI_SC_UTF8,
     [subvivision] varchar(15) collate Latin1_General_100_CI_AI_SC_UTF8
) AS [country] ON city.alpha2=country.alpha2



/*
Creates a new view table named "CreditCardLonLat" in the database.
The view retrieves data from three CSV files located at the specified URLs and joins the data based on matching columns:

a. The first CSV file "creditoutput.csv" contains credit card transaction data. The data is loaded using the OPENROWSET function with the BULK option, specifying the file URL. The file is in CSV format, and the FORMAT parameter is set to 'CSV'. The FIELDTERMINATOR parameter defines the field separator as a comma. The FIRSTROW parameter indicates that the data starts from the second row. The ESCAPECHAR parameter specifies the escape character for special characters in the file.
b. The second CSV file "CityList.csv" contains city information. It is loaded using the OPENROWSET function with similar parameters as the first file.
c. The third CSV file "CountryList.csv" contains country information. It is also loaded using the OPENROWSET function with similar parameters as the previous files.

The columns from these three data sources are selected and combined in the SELECT statement. 
The resulting dataset includes columns such as transaction time, city name, geographical coordinates (longitude and latitude), alpha2 country code, company name, transaction features (V1 to V28), transaction amount, transaction class, and transaction ID.
The LEFT JOIN clauses are used to join the tables based on matching IDs and alpha2 country codes.
The LEFT JOIN is used in this code to join the tables together based on matching columns, while ensuring that all records from the left table (credit in this case) are included in the result, even if there is no match in the right table (city or country).
In this specific scenario, the LEFT JOIN is used to associate the credit card transaction data with the corresponding city and country information. The credit table contains transaction data, and the city and country tables contain location-related information.
By using a LEFT JOIN, all records from the credit table will be included in the result, regardless of whether a match is found in the city and country tables. 
If there is a match, the columns from the respective tables will be included in the result. If there is no match, NULL values will be populated for the columns from the city and country tables.
The purpose of using a LEFT JOIN in this code is to ensure that all transaction records are retained in the final result, even if some location information is missing or not available for certain transactions. This way, the analysis of fraud by location can still be performed using the available data, without excluding any transaction records.
*/