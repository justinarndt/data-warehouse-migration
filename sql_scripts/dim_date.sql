/*
=============================================================================
  Dim_Date — Date Dimension Table
  Purpose: Pre-populated calendar table spanning 2020–2040 for time-intelligence
           queries in Power BI (e.g., YoY growth, same period last year)
  Target:  Azure SQL Database (sql-dw-gold), Gold schema
  Run:     Execute once as a post-deployment step
=============================================================================
*/

-- Drop if exists for idempotency
IF OBJECT_ID('Gold.Dim_Date', 'U') IS NOT NULL
    DROP TABLE Gold.Dim_Date;
GO

CREATE TABLE Gold.Dim_Date (
    DateKey         INT             PRIMARY KEY,    -- YYYYMMDD format
    FullDate        DATE            NOT NULL,
    DayOfMonth      INT             NOT NULL,
    DayOfWeekName   NVARCHAR(10)    NOT NULL,
    DayOfWeekNumber INT             NOT NULL,
    WeekOfYear      INT             NOT NULL,
    MonthNumber     INT             NOT NULL,
    MonthName       NVARCHAR(10)    NOT NULL,
    Quarter         INT             NOT NULL,
    Year            INT             NOT NULL,
    IsWeekend       BIT             NOT NULL,
    IsLeapYear      BIT             NOT NULL,
    FiscalQuarter   INT             NOT NULL,       -- Assumes Jan fiscal start
    FiscalYear      INT             NOT NULL
);
GO

-- Seed the date dimension with 20 years of calendar data
DECLARE @StartDate DATE = '2020-01-01';
DECLARE @EndDate   DATE = '2040-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO Gold.Dim_Date (
        DateKey, FullDate, DayOfMonth, DayOfWeekName, DayOfWeekNumber,
        WeekOfYear, MonthNumber, MonthName, Quarter, Year,
        IsWeekend, IsLeapYear, FiscalQuarter, FiscalYear
    )
    SELECT
        CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT),
        @StartDate,
        DAY(@StartDate),
        DATENAME(WEEKDAY, @StartDate),
        DATEPART(WEEKDAY, @StartDate),
        DATEPART(WEEK, @StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DATEPART(QUARTER, @StartDate),
        YEAR(@StartDate),
        CASE WHEN DATENAME(WEEKDAY, @StartDate) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
        CASE WHEN (YEAR(@StartDate) % 4 = 0 AND YEAR(@StartDate) % 100 != 0)
                OR YEAR(@StartDate) % 400 = 0 THEN 1 ELSE 0 END,
        DATEPART(QUARTER, @StartDate),  -- Fiscal = Calendar (adjust if needed)
        YEAR(@StartDate);

    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;
GO

PRINT 'Gold.Dim_Date populated: 2020-01-01 through 2040-12-31';
GO
