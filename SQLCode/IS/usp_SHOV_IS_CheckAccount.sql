CREATE OR ALTER PROCEDURE [dbo].[usp_SHOV_IS_CheckAccount]
(
	@AccntJSON AS NVARCHAR(max)
)
AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY
BEGIN TRAN

DECLARE @IdentityOutput TABLE ( ID int )

INSERT INTO dbo.SHOV_IS_Requests
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
SELECT NULLIF(AccountNo, '')
, NULLIF(FacilityCode, '')
, NULLIF(JSON_VALUE(@AccntJSON, '$.Account.meta.tag[0].code'), '') as RequestType
, NULLIF(JSON_VALUE(patient, '$.name[0].given[0]') + ' ' + JSON_VALUE(patient, '$.name[0].family'), '') as PatientName
, NULLIF(MRN, '')
, DOB
, NULLIF(guarantorname, '')
, NULLIF(line1, ''), NULLIF(line2, '')
, NULLIF(city, ''), NULLIF(state, ''), NULLIF(postalCode, '')

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
, SCR.LockFlag = 1
FROM dbo.SHOV_IS_Requests AS SCR
WHERE SCR.ReqID = @ReqID
AND (
SCR.AddressLine1 IS NULL
OR SCR.AddressCity IS NULL
OR SCR.AddressState IS NULL
OR SCR.ZipCode IS NULL
)

UPDATE SCR
SET SCR.ValidationMsg = ISNULL(SCR.ValidationMsg + '; ', '') + 'Patient Name is missing.'
, SCR.StatusCode = 3 ----Dismissed
, SCR.ExceptionMsg = ISNULL(SCR.ExceptionMsg + '; ', '') + 'Business rule - Dismissed request - Patient Name is missing'
, SCR.LockFlag = 1
FROM dbo.SHOV_IS_Requests AS SCR
WHERE SCR.ReqID = @ReqID
AND SCR.PatientName IS NULL;

IF EXISTS (SELECT 1 FROM dbo.SHOV_IS_Requests AS SCR WHERE SCR.ReqID = @ReqID
AND StatusCode = 1)
BEGIN
	
-------------========Summary==========-----------
INSERT INTO SHOV_IS_Summary
(ReqID, ChargeAmt, PaidAmt, AdjustmentAmt, PatientPaidAmt, PayerPaidAmt)
SELECT @ReqID, ChargeAmt, PaidAmt, Adjustment, PatientPaidAmt, PayerPaidAmt
FROM OPENJSON(@AccntJSON, '$.Account.summary')
with (
ChargeAmt decimal(10,2) '$.charged.value'
,PaidAmt decimal(10,2) '$.paid.value'
, Adjustment decimal(10,2) '$.contractualAdjustment.value'
, patientSummary nvarchar(max) as json 
, payerSummary nvarchar(max) as json
)
OUTER APPLY openjson(patientSummary)
with(
 PatientPaidAmt decimal(10,2) '$.paid.value'
)
OUTER APPLY openjson(payerSummary)
with(
 PayerPaidAmt decimal(10,2) '$.paid.value'
)
-------------========Summary==========-----------

-------------========Charges==========-----------
INSERT INTO dbo.SHOV_IS_Charges
(ReqID, ChargeDesc, PostedDate, ChargeCode, GLCode, Units, ChargeAmt)
SELECT @ReqID, ChargeDesc, PostedDate, ChargeCode, glCode,  Units, ChargeAmt
FROM OPENJSON(@AccntJSON, '$.Account.charge')
WITH (
ChargeDesc varchar(2000) '$.chargeDesc'
,ChargeCode Varchar(20) '$.chargeItemCode'
, PostedDate date '$.date'
, glCode varchar(20) '$.glCode'
, Units smallint '$.service.supplement.value'
, ChargeAmt decimal(10,2) '$.amount.value'
)

-------------========Charges==========-----------

-------------========Payments==========-----------
INSERT INTO dbo.SHOV_IS_Payments
(ReqID, PaymentDesc, PostedDate, RemittanceCode, PaymentType, PayerPlanName, PaymentAmt)
SELECT @ReqID, PaymentDesc, PostedDate, RemittanceCode, PaymentType, PayerPlanName, PaymentAmt
FROM OPENJSON(@AccntJSON, '$.Account.payment')
with (
		 PaymentDesc varchar(2000) '$.type'
		, PostedDate date '$.date'
		, RemittanceCode Varchar(20) '$.remittanceCode'
		, PaymentType varchar(10) '$.paymentMode.type'
		, PayerPlanName Varchar(2000) '$.payerPlan.display'
		, PaymentAmt decimal(10,2) '$.paymentPlan.amount.value'
)

-------------========Payments==========-----------


-------------========Adjustments==========-----------
INSERT INTO dbo.SHOV_IS_Adjustments
(ReqID, AdjustmentDesc, PostedDate, PayerPlanName, AdjustmentAmt)
SELECT @ReqID, AdjustmentDesc, PostedDate, PayerPlanName, AdjustmentAmt
FROM OPENJSON(@AccntJSON, '$.Account.adjustment')
with (
		 AdjustmentDesc varchar(2000) '$.type'
		, PostedDate date '$.date'
		, PayerPlanName Varchar(2000) '$.payerPlan.display'
		, AdjustmentAmt decimal(10,2) '$.amount.value'
)

-------------========Adjustments==========-----------

END

UPDATE SCR
SET SCR.ValidationMsg = ISNULL(SCR.ValidationMsg + '; ', '') + 'Unable to process. Charges and payments data unavailable.'
, SCR.StatusCode = 3 ----Dismissed
, SCR.ExceptionMsg = ISNULL(SCR.ExceptionMsg + '; ', '') + 'Business rule - Under review request - charges and payments data unavailable'
FROM dbo.SHOV_IS_Requests AS SCR
WHERE SCR.ReqID = @ReqID
AND NOT EXISTS (SELECT 1 FROM dbo.SHOV_IS_Charges as sic
where sic.ReqID = @ReqID);

COMMIT TRAN
END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000);  
    DECLARE @ErrorSeverity INT;  
    DECLARE @ErrorState INT;  
  
    SELECT   
        @ErrorMessage = ERROR_MESSAGE(),  
        @ErrorSeverity = ERROR_SEVERITY(),  
        @ErrorState = ERROR_STATE();  
  

    RAISERROR (@ErrorMessage, -- Message text.  
               @ErrorSeverity, -- Severity.  
               @ErrorState -- State.  
               );  

	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRAN
	END
END CATCH	
END
GO
