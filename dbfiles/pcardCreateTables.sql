USE [master]
GO

CREATE DATABASE [pcard]
GO


USE [pcard]
GO

CREATE TABLE [purchases] (
  [batch_id] nvarchar(75) PRIMARY KEY,
  [division] nvarchar(75),
  [tran_date] date,
  [pstd_date] date,
  [merchant_name] nvarchar(75),
  [tran_amt] numeric(18,4),
  [tran_crncy] nvarchar(75),
  [orig_amt] numeric(18,4),
  [orig_crcny] nvarchar(75),
  [gl_acct] nvarchar(75),
  [cc_wbs_ord] nvarchar(75),
  [purpose] nvarchar(150)
)
GO

CREATE TABLE [merchants] (
  [merchant_name] nvarchar(75) PRIMARY KEY,
  [merchant_type] int,
  [merchant_type_desc] nvarchar(75)
)
GO

CREATE TABLE [costCenter] (
  [cc_wbs_ord] nvarchar(75) PRIMARY KEY,
  [cc_wbs_ord_desc] nvarchar(75)
)
GO

CREATE TABLE [generalLedger] (
  [gl_acct] nvarchar(75) PRIMARY KEY,
  [gl_acct_desc] nvarchar(75)
)
GO

ALTER TABLE [purchases] ADD FOREIGN KEY ([merchant_name]) REFERENCES [merchants] ([merchant_name])
GO

ALTER TABLE [purchases] ADD FOREIGN KEY ([cc_wbs_ord]) REFERENCES [costCenter] ([cc_wbs_ord])
GO

ALTER TABLE [purchases] ADD FOREIGN KEY ([gl_acct]) REFERENCES [generalLedger] ([gl_acct])
GO

