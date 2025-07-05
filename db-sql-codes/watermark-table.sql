CREATE TABLE electronics_watermark_table 
(
    last_load VARCHAR(2000)
);

-- Query to get minimum Transaction_ID from source data (for initial setup)

SELECT MIN(Transaction_ID) FROM source_electronics_data;

-- Initialize watermark table with starting value
-- You can get the minimum Transaction_ID from your electronics data

INSERT INTO electronics_watermark_table
VALUES('TXN00000'); -- Replace with actual minimum Transaction_ID from your data
