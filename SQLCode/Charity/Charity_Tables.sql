IF OBJECT_ID('dbo.SHOV_Charity_API_WorkList') IS NOT NULL
BEGIN
	DROP TABLE dbo.SHOV_Charity_API_WorkList
END
GO

IF OBJECT_ID('dbo.SHOV_Charity_Requests') IS NOT NULL
BEGIN
	DROP TABLE dbo.SHOV_Charity_Requests
END
GO

IF OBJECT_ID('dbo.SHOV_Charity_API_WorkList') IS NULL
BEGIN
	CREATE TABLE dbo.SHOV_Charity_API_WorkList
	(
	WorklistID int identity(1,1)
	, ReqDate date DEFAULT(GETDATE())
	, RequestType varchar(5)
	, RequestNo varchar(40)
	, FacilityCode varchar(4)
	, MRN varchar(40)
	, AccountNo varchar(40)
	, AccountType varchar(10)
	, StatusCode tinyint DEFAULT(1) --- 1 - In-Progress, 4- Completed, 2-In-Review, 3-Dismissed
	, StageCode tinyint DEFAULT(0)
	, LockFlag tinyint DEFAULT(0)
	, ValidationMsg varchar(max)
	, UserName varchar(40)
	, ExceptionMsg varchar(max)
	)
END
GO

IF OBJECT_ID('dbo.SHOV_Charity_Requests') IS NULL
BEGIN
	CREATE TABLE dbo.SHOV_Charity_Requests
	(
	ReqID int identity(1,1) Primary key clustered
	, ReqDate date Default(GETDATE())
	, AccountNumber varchar(40)
	, FacilityCode varchar(4)
	, RequestType varchar(5)
	, PatientName varchar(100)
	, MRN varchar(40)
	, DOB date
	, GuarantorName varchar(100)
	, AddressLine1 varchar(100)
	, AddressLine2 varchar(100)
	, AddressCity varchar(20)
	, AddressState varchar(20)
	, ZipCode varchar(20)
	, StatusCode tinyint DEFAULT(1)--- 1 - In-Progress, 4- Completed, 2-In-Review, 3-Dismissed
	, StageCode tinyint DEFAULT(0)
	, LockFlag tinyint DEFAULT(0)
	, ValidationMsg varchar(max)--- Based on exception rules 
	, UserName varchar(40) DEFAULT(HOST_NAME())
	, ExceptionMsg varchar(max)
	)
END
GO

IF OBJECT_ID('dbo.SHOV_Charity_Map') IS NULL
BEGIN
	CREATE TABLE [dbo].[SHOV_Charity_Map]
	(
		[MapID] [int] IDENTITY(1,1) NOT NULL UNIQUE,
		[FacilityCode] [varchar](4) NOT NULL PRIMARY KEY CLUSTERED,
		[HospitalName] [varchar](120) NOT NULL,
		[ApplicationName] [varchar](60) NULL,
		[NumOfPages] [int] NULL,

	)
END
GO

IF OBJECT_ID('dbo.SHOV_Charity_ExceptionList') IS NULL
BEGIN
	CREATE TABLE dbo.SHOV_Charity_ExceptionList
	(
	ExceptionID INT IDENTITY(1,1) UNIQUE nonclustered
	, FacilityCode varchar(4) primary key clustered
	, FacilityName varchar(100)
	, RequestType varchar(max)
	)
END
GO
