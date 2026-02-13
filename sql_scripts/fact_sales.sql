/*
=============================================================================
  Fact_Sales — Sales Order Fact Table (Star Schema)
  Purpose: Central fact table containing transactional sales metrics with
           foreign keys to dimension tables. Designed for high-performance
           analytical queries in Power BI / Tableau.
  Target:  Azure SQL Database (sql-dw-gold), Gold schema
  
  Loading Logic (handled by ADF Pipeline PL_Load_Gold):
    - Point-in-time join to Dim_Customer ensures historical accuracy:
      F4211.ABAN8 = Dim_Customer.CustomerID
      AND OrderDate >= ValidFrom
      AND OrderDate < ISNULL(ValidTo, '9999-12-31')
    - DateKey derived from converted OrderDate (YYYYMMDD integer)
=============================================================================
*/

IF OBJECT_ID('Gold.Fact_Sales', 'U') IS NOT NULL
    DROP TABLE Gold.Fact_Sales;
GO

CREATE TABLE Gold.Fact_Sales (
    -- Surrogate Key
    SalesKey        INT             IDENTITY(1,1)   PRIMARY KEY,

    -- Dimension Foreign Keys
    CustomerKey     INT             NOT NULL,       -- FK → Dim_Customer
    DateKey         INT             NOT NULL,       -- FK → Dim_Date (YYYYMMDD)

    -- Degenerate Dimensions (from source, no separate dim table)
    OrderNumber     INT             NOT NULL,       -- SDDOCO
    OrderType       NVARCHAR(5)     NOT NULL,       -- SDDCTO
    ItemNumber      NVARCHAR(25)    NOT NULL,       -- SDLITM

    -- Measures (already converted from implicit decimals)
    Quantity        DECIMAL(18,2)   NOT NULL,       -- SDUORG / 100
    ExtendedAmount  DECIMAL(18,2)   NOT NULL,       -- SDAEXP / 100
    UnitPrice       DECIMAL(18,2)   NOT NULL,       -- Calculated: ExtendedAmount / Quantity

    -- Audit
    LoadDate        DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

    -- Foreign Key Constraints
    CONSTRAINT FK_FactSales_DimCustomer
        FOREIGN KEY (CustomerKey) REFERENCES Gold.Dim_Customer(CustomerKey),
    CONSTRAINT FK_FactSales_DimDate
        FOREIGN KEY (DateKey) REFERENCES Gold.Dim_Date(DateKey)
);
GO

-- Indexes optimized for typical analytical query patterns
CREATE NONCLUSTERED INDEX IX_FactSales_CustomerKey
    ON Gold.Fact_Sales (CustomerKey)
    INCLUDE (DateKey, ExtendedAmount);
GO

CREATE NONCLUSTERED INDEX IX_FactSales_DateKey
    ON Gold.Fact_Sales (DateKey)
    INCLUDE (CustomerKey, ExtendedAmount, Quantity);
GO

PRINT 'Gold.Fact_Sales created with FK constraints and analytical indexes.';
GO
