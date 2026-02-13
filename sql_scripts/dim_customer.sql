/*
=============================================================================
  Dim_Customer — Customer Dimension Table (SCD Type 2)
  Purpose: Tracks historical changes to customer attributes using Slowly
           Changing Dimensions Type 2. Each change creates a new row with
           versioning metadata (ValidFrom, ValidTo, IsActive).
  Target:  Azure SQL Database (sql-dw-gold), Gold schema
  
  SCD2 Logic (handled by ADF Data Flow DF_SCD2_Customer):
    - New customers    → INSERT with IsActive=1, ValidTo=NULL
    - Changed customers → UPDATE existing row (IsActive=0, ValidTo=now)
                          INSERT new version (IsActive=1, ValidTo=NULL)
    - Unchanged        → No action (detected via SHA256 hash comparison)
=============================================================================
*/

IF OBJECT_ID('Gold.Dim_Customer', 'U') IS NOT NULL
    DROP TABLE Gold.Dim_Customer;
GO

CREATE TABLE Gold.Dim_Customer (
    -- Surrogate Key (auto-generated, warehouse-internal)
    CustomerKey     INT             IDENTITY(1,1)   PRIMARY KEY,

    -- Business Key (from JDE F0101.ABAN8)
    CustomerID      INT             NOT NULL,       -- ABAN8

    -- Business Attributes (tracked for changes)
    CustomerName    NVARCHAR(100)   NOT NULL,       -- ABALPH
    SearchType      NVARCHAR(10)    NOT NULL,       -- ABAT1
    CategoryCode    NVARCHAR(10)    NOT NULL,       -- ABAC01

    -- SCD Type 2 Metadata
    IsActive        BIT             NOT NULL DEFAULT 1,
    ValidFrom       DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    ValidTo         DATETIME2       NULL,           -- NULL = current version

    -- Hash for efficient change detection
    RowHash         NVARCHAR(64)    NULL,           -- SHA256 of business columns

    -- Audit columns
    CreatedDate     DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
    ModifiedDate    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Index for business key lookups during SCD2 merge
CREATE NONCLUSTERED INDEX IX_DimCustomer_BusinessKey
    ON Gold.Dim_Customer (CustomerID, IsActive)
    INCLUDE (ValidFrom, ValidTo);
GO

-- Index for point-in-time joins from Fact_Sales
CREATE NONCLUSTERED INDEX IX_DimCustomer_PointInTime
    ON Gold.Dim_Customer (CustomerID, ValidFrom, ValidTo)
    INCLUDE (CustomerKey);
GO

PRINT 'Gold.Dim_Customer created with SCD2 support and optimized indexes.';
GO
