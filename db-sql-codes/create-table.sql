CREATE TABLE Electronics_SalesData (
    Store_ID VARCHAR(10) NOT NULL,
    Vendor_ID VARCHAR(10) NOT NULL,
    Product_SKU VARCHAR(15) NOT NULL,
    Revenue DECIMAL(10,2) NOT NULL,
    Units_Sold INT NOT NULL,
    Transaction_ID VARCHAR(15) NOT NULL,
    Day TINYINT NOT NULL CHECK (Day BETWEEN 1 AND 31),
    Month TINYINT NOT NULL CHECK (Month BETWEEN 1 AND 12),
    Year SMALLINT NOT NULL,
    Quarter TINYINT NOT NULL CHECK (Quarter BETWEEN 1 AND 4),
    StoreName VARCHAR(30) NOT NULL,
    VendorName VARCHAR(25) NOT NULL,
    Category VARCHAR(20) NOT NULL,
    Product_Brand VARCHAR(20) NOT NULL,
    Discount_Applied DECIMAL(4,3) NOT NULL CHECK (Discount_Applied BETWEEN 0 AND 1),
    Customer_Segment VARCHAR(15) NOT NULL,
    Sales_Channel VARCHAR(10) NOT NULL CHECK (Sales_Channel IN ('Online', 'In-Store')),
    
    -- Primary Key
    CONSTRAINT PK_Electronics_SalesData PRIMARY KEY (Transaction_ID),
    
    -- Indexes for better query performance
    INDEX IX_Electronics_SalesData_Date (Year, Month, Day),
    INDEX IX_Electronics_SalesData_Store (Store_ID),
    INDEX IX_Electronics_SalesData_Category (Category),
    INDEX IX_Electronics_SalesData_Channel (Sales_Channel)
);
