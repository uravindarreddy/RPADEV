CREATE OR ALTER PROCEDURE dbo.usp_SHOV_Charity_CheckAccount
(
	@AccntJSON as nvarchar(max)
)
AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY
BEGIN TRAN
DECLARE @IdentityOutput TABLE ( ID int )

INSERT INTO dbo.SHOV_Charity_Requests
( AccountNumber 
, FacilityCode
, RequestType 
, PatientName 
, MRN
, DOB 
, GuarantorName
, AddressLine1 
, AddressLine2 
, AddressCity 
, AddressState 
, ZipCode 
)
OUTPUT INSERTED.ReqID INTO @IdentityOutput(ID)
SELECT AccountNo, FacilityCode, JSON_VALUE(@AccntJSON, '$.Account.meta.tag[0].code') as RequestType
, JSON_VALUE(patient, '$.name[0].given[0]') + ' ' + JSON_VALUE(patient, '$.name[0].family') as PatientName
, MRN
, DOB
, guarantorname
, line1, line2
, city, state, postalCode

FROM OPENJSON(@AccntJSON)
WITH
(
	MRN VARCHAR(40) '$.Patient.mrn'
	, FacilityCode varchar(4) '$.Account.facilityCode'
	, AccountNo varchar(40) '$.Account.accountNumber'
	, DOB DATE '$.Patient.dateOfBirth'
	, [Patient] nvarchar(max) AS JSON
) as pat
OUTER APPLY OPENJSON(pat.[patient]) 
WITH
(
[address] nvarchar(max) as json
, guarantorname varchar(100) '$.identifier[0].value'
) as p
OUTER APPLY OPENJSON (p.[address])
WITH
(
	city varchar(20)
	, state varchar(20)
	, postalCode varchar(20)
	, line1 varchar(100) '$.line[0]'
	, line2 varchar(100) '$.line[1]'
) as addr

DECLARE @ReqID int 

SET @ReqID = (SELECT ID FROM @IdentityOutput);

UPDATE SCR
SET SCR.ValidationMsg = 'Unable to process. Incomplete address fields'
, SCR.StatusCode = 3 ----Dismissed
, SCR.ExceptionMsg = 'Business rule - Dismissed request - incomplete address fields'
FROM dbo.SHOV_Charity_Requests AS SCR
WHERE  SCR.ReqID = @ReqID
AND (SCR.AddressLine1 IS NULL
OR SCR.AddressCity IS NULL
OR SCR.AddressState IS NULL
OR SCR.ZipCode IS NULL
)

UPDATE SCR
SET SCR.ValidationMsg = ISNULL(SCR.ValidationMsg, '') + 'Patient Name is missing.'
, SCR.StatusCode = 3 ----Dismissed
, SCR.ExceptionMsg = ISNULL(SCR.ExceptionMsg, '') + 'Business rule - Dismissed request - Patient Name is missing.'
FROM dbo.SHOV_Charity_Requests AS SCR
WHERE SCR.ReqID = @ReqID
AND SCR.PatientName IS NULL

COMMIT TRAN
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 
	BEGIN
		ROLLBACK TRAN
	END

SELECT ERROR_MESSAGE() as ErrorMsg
END CATCH	
END
GO

