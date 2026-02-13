/*
=============================================================================
  Schema Creation Script
  Purpose: Creates Bronze, Silver, and Gold schemas for the Medallion Architecture
  Target:  Azure SQL Database (sql-dw-gold)
=============================================================================
*/

-- Bronze Schema: Raw ingestion zone (mirrors source system structure)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Bronze')
    EXEC('CREATE SCHEMA Bronze');
GO

-- Silver Schema: Enriched/cleansed zone (standardized, historized)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Silver')
    EXEC('CREATE SCHEMA Silver');
GO

-- Gold Schema: Curated business zone (star schema, consumption-ready)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Gold')
    EXEC('CREATE SCHEMA Gold');
GO

PRINT 'All schemas created successfully.';
GO
