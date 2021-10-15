USE [master]
GO

CREATE DATABASE [pcardDW]
GO

USE [pcardDW]
GO

CREATE TABLE [FactsPurchases] (
  [batch_id] nvarchar(75) PRIMARY KEY,
  [DateKey] int,
  [division_id] int,
  [tran_date_key] int,
  [pstd_date_key] int,
  [merchant_id] int,
  [tran_amt] numeric(18,4),
  [tran_crcny_id] int,
  [orig_amt] numeric(18,4),
  [orig_crncy_id] int,
  [gl_acct_id] int,
  [cc_wbs_ord_id] int,
  [purpose] nvarchar(150)
)
GO

CREATE TABLE [DimCurrency] (
  [currency_id] int PRIMARY KEY IDENTITY(1, 1),
  [currency_code] nvarchar(75),
  [currency_desc] nvarchar(75)
)
GO

CREATE TABLE [DimDivision] (
  [division_id] int PRIMARY KEY IDENTITY(1, 1),
  [division_name] nvarchar(75),
  [division_sup_id] int,
  [start_date] datetime,
  [end_date] datetime,
  [isCurrent] bit
)
GO

CREATE TABLE [DimMerchants] (
  [merchant_id] int PRIMARY KEY IDENTITY(1, 1),
  [merchant_name] nvarchar(75),
  [merchant_type] int,
  [merchant_type_desc] nvarchar(75),
  [start_date] datetime,
  [end_date] datetime,
  [isCurrent] bit
)
GO

CREATE TABLE [DimCostCenter] (
  [cc_wbs_ord_id] int PRIMARY KEY IDENTITY(1, 1),
  [cc_wbs_ord] nvarchar(75),
  [cc_wbs_ord_desc] nvarchar(75),
  [start_date] datetime,
  [end_date] datetime,
  [isCurrent] bit
)
GO

CREATE TABLE [DimDate] (
  [DateKey] INT PRIMARY KEY,
  [Date] DATE,
  [Day] TINYINT,
  [DaySuffix] CHAR,
  [Weekday] TINYINT,
  [WeekDayName] VARCHAR,
  [WeekDayName_Short] CHAR,
  [WeekDayName_FirstLetter] CHAR,
  [DOWInMonth] TINYINT,
  [DayOfYear] SMALLINT,
  [WeekOfMonth] TINYINT,
  [WeekOfYear] TINYINT,
  [Month] TINYINT,
  [MonthName] VARCHAR,
  [MonthName_Short] CHAR,
  [MonthName_FirstLetter] CHAR,
  [Quarter] TINYINT,
  [QuarterName] VARCHAR,
  [Year] INT,
  [MMYYYY] CHAR,
  [MonthYear] CHAR,
  [IsWeekend] BIT
)
GO

CREATE TABLE [DimGeneralLedger] (
  [gl_acct_id] int PRIMARY KEY IDENTITY(1, 1),
  [gl_acct] nvarchar(75),
  [gl_acct_steward_id] int,
  [gl_acct_desc] nvarchar(75),
  [start_date] datetime,
  [end_date] datetime,
  [isCurrent] BIT
)
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([division_id]) REFERENCES [DimDivision] ([division_id])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([tran_date_key]) REFERENCES [DimDate] ([DateKey])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([pstd_date_key]) REFERENCES [DimDate] ([DateKey])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([merchant_id]) REFERENCES [DimMerchants] ([merchant_id])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([tran_crcny_id]) REFERENCES [DimCurrency] ([currency_id])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([orig_crncy_id]) REFERENCES [DimCurrency] ([currency_id])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([gl_acct_id]) REFERENCES [DimGeneralLedger] ([gl_acct_id])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([cc_wbs_ord_id]) REFERENCES [DimCostCenter] ([cc_wbs_ord_id])
GO

ALTER TABLE [FactsPurchases] ADD FOREIGN KEY ([DateKey]) REFERENCES [DimDate] ([DateKey])
GO
