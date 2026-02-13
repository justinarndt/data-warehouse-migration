/*
=============================================================================
  load_gold_data.sql — Populate Gold Star Schema
  Purpose: Loads Dim_Customer and Fact_Sales from the synthetic JDE data.
           Run this after the CSV files have been generated.
  
  Usage:  Paste and run in Azure SQL Query Editor
  Note:   Dim_Date is already populated by dim_date.sql (7,671 rows)
=============================================================================
*/

-- ============================================================
-- STEP 1: Load Dim_Customer from F0101 data
-- ============================================================
PRINT 'Loading Dim_Customer...';

INSERT INTO Gold.Dim_Customer (CustomerID, CustomerName, SearchType, CategoryCode, IsActive, ValidFrom, RowHash)
VALUES
(93810, 'Johnson LLC', 'C', '300', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Johnson LLC', '|', 'C', '|', '300'))),
(39256, 'Doyle Ltd', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Doyle Ltd', '|', 'C', '|', '100'))),
(81482, 'Garza Inc', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Garza Inc', '|', 'C', '|', '100'))),
(79822, 'Smith Group', 'C', '200', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Smith Group', '|', 'C', '|', '200'))),
(54231, 'Williams Corp', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Williams Corp', '|', 'C', '|', '100'))),
(67890, 'Davis & Sons', 'C', '300', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Davis & Sons', '|', 'C', '|', '300'))),
(12345, 'Anderson Holdings', 'C', '200', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Anderson Holdings', '|', 'C', '|', '200'))),
(88341, 'Miller Enterprises', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Miller Enterprises', '|', 'C', '|', '100'))),
(45678, 'Taylor Industries', 'C', '300', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Taylor Industries', '|', 'C', '|', '300'))),
(23456, 'Brown Manufacturing', 'C', '200', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Brown Manufacturing', '|', 'C', '|', '200'))),
(34567, 'Wilson Tech', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Wilson Tech', '|', 'C', '|', '100'))),
(56789, 'Moore Solutions', 'C', '300', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Moore Solutions', '|', 'C', '|', '300'))),
(78901, 'Jackson Partners', 'C', '200', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Jackson Partners', '|', 'C', '|', '200'))),
(11111, 'White Logistics', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('White Logistics', '|', 'C', '|', '100'))),
(22222, 'Harris Global', 'C', '300', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Harris Global', '|', 'C', '|', '300'))),
(33333, 'Martin Supply', 'C', '200', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Martin Supply', '|', 'C', '|', '200'))),
(44444, 'Thompson Electronics', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Thompson Electronics', '|', 'C', '|', '100'))),
(55555, 'Garcia Foods', 'C', '300', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Garcia Foods', '|', 'C', '|', '300'))),
(66666, 'Martinez Auto', 'C', '200', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Martinez Auto', '|', 'C', '|', '200'))),
(77777, 'Robinson Healthcare', 'C', '100', 1, SYSUTCDATETIME(), HASHBYTES('SHA2_256', CONCAT('Robinson Healthcare', '|', 'C', '|', '100')));

DECLARE @CustCount INT = (SELECT COUNT(*) FROM Gold.Dim_Customer);
PRINT CONCAT('Dim_Customer loaded: ', @CustCount, ' rows');
GO

-- ============================================================
-- STEP 2: Load Fact_Sales with proper surrogate key lookups
-- ============================================================
PRINT 'Loading Fact_Sales...';

-- Generate sales orders with proper FK relationships
INSERT INTO Gold.Fact_Sales (CustomerKey, DateKey, OrderNumber, OrderType, ItemNumber, Quantity, ExtendedAmount, UnitPrice)
SELECT 
    c.CustomerKey,
    d.DateKey,
    s.OrderNumber,
    'SO' AS OrderType,
    s.ItemNumber,
    s.Quantity,
    s.ExtendedAmount,
    CASE WHEN s.Quantity > 0 THEN s.ExtendedAmount / s.Quantity ELSE 0 END AS UnitPrice
FROM (
    VALUES 
    -- OrderNumber, CustomerID, OrderDate, ItemNumber, Quantity, ExtendedAmount
    (683824, 81482, '2025-08-21', '8013267736027', 78.00, 14301.30),
    (2261, 79822, '2025-07-08', '6474687234302', 94.00, 29241.52),
    (150432, 93810, '2025-03-15', '1234567890123', 45.50, 8925.00),
    (275891, 39256, '2025-05-22', '9876543210987', 120.00, 36000.00),
    (331045, 54231, '2025-01-10', '5551234567890', 22.75, 4550.00),
    (442718, 67890, '2025-09-03', '4447890123456', 65.00, 19500.00),
    (558392, 12345, '2025-02-28', '3332345678901', 88.25, 26475.00),
    (663201, 88341, '2025-06-14', '2223456789012', 33.50, 6700.00),
    (771834, 45678, '2025-11-05', '1114567890123', 150.00, 45000.00),
    (889456, 23456, '2025-04-19', '9995678901234', 42.00, 12600.00),
    (901234, 34567, '2025-08-30', '8886789012345', 71.25, 21375.00),
    (112345, 56789, '2025-10-12', '7777890123456', 95.50, 28650.00),
    (223456, 78901, '2025-07-25', '6668901234567', 18.00, 5400.00),
    (334567, 11111, '2025-03-08', '5559012345678', 200.00, 60000.00),
    (445678, 22222, '2025-12-01', '4440123456789', 55.75, 16725.00),
    (556789, 33333, '2025-06-20', '3331234567890', 82.00, 24600.00),
    (667890, 44444, '2025-09-15', '2222345678901', 37.50, 11250.00),
    (778901, 55555, '2025-01-30', '1113456789012', 105.00, 31500.00),
    (889012, 66666, '2025-05-11', '9994567890123', 63.25, 18975.00),
    (990123, 77777, '2025-11-22', '8885678901234', 48.00, 14400.00),
    (101010, 93810, '2025-07-01', '7776789012345', 125.50, 37650.00),
    (202020, 39256, '2025-02-14', '6667890123456', 91.00, 27300.00),
    (303030, 81482, '2025-04-05', '5558901234567', 160.75, 48225.00),
    (404040, 79822, '2025-08-18', '4449012345678', 29.50, 8850.00),
    (505050, 54231, '2025-10-30', '3330123456789', 74.00, 22200.00),
    (606060, 67890, '2025-06-07', '2221234567890', 110.25, 33075.00),
    (707070, 12345, '2025-12-15', '1112345678901', 56.00, 16800.00),
    (808080, 88341, '2025-03-22', '9993456789012', 83.75, 25125.00),
    (909090, 45678, '2025-09-28', '8884567890123', 41.50, 12450.00),
    (111213, 23456, '2025-01-05', '7775678901234', 195.00, 58500.00)
) AS s(OrderNumber, CustomerID, OrderDate, ItemNumber, Quantity, ExtendedAmount)
INNER JOIN Gold.Dim_Customer c ON c.CustomerID = s.CustomerID AND c.IsActive = 1
INNER JOIN Gold.Dim_Date d ON d.FullDate = CAST(s.OrderDate AS DATE);

DECLARE @SalesCount INT = (SELECT COUNT(*) FROM Gold.Fact_Sales);
PRINT CONCAT('Fact_Sales loaded: ', @SalesCount, ' rows');
GO

-- ============================================================
-- STEP 3: Verify the star schema
-- ============================================================
PRINT '';
PRINT '=== VERIFICATION ===';

SELECT 'Dim_Date' AS TableName, COUNT(*) AS RowCount FROM Gold.Dim_Date
UNION ALL
SELECT 'Dim_Customer', COUNT(*) FROM Gold.Dim_Customer
UNION ALL
SELECT 'Fact_Sales', COUNT(*) FROM Gold.Fact_Sales;
GO

-- Sample star schema join
SELECT TOP 10
    f.OrderNumber,
    c.CustomerName,
    c.CategoryCode,
    d.FullDate AS OrderDate,
    d.MonthName,
    d.Quarter,
    f.Quantity,
    f.ExtendedAmount,
    f.UnitPrice
FROM Gold.Fact_Sales f
INNER JOIN Gold.Dim_Customer c ON f.CustomerKey = c.CustomerKey
INNER JOIN Gold.Dim_Date d ON f.DateKey = d.DateKey
ORDER BY f.ExtendedAmount DESC;
GO

PRINT 'Gold layer load complete.';
GO
