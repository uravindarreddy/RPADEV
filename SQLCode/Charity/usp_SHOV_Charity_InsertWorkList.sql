CREATE OR ALTER PROCEDURE dbo.usp_SHOV_Charity_InsertWorkList
(
	@json AS NVARCHAR(MAX)
)
AS
BEGIN
SET NOCOUNT ON;

BEGIN TRY
BEGIN TRAN

DECLARE @WorkListItems AS TABLE (WorkListID int)

INSERT INTO dbo.SHOV_Charity_API_WorkList
(RequestType, RequestNo, FacilityCode, AccountType, AccountNo, MRN, UserName)
OUTPUT inserted.WorklistID INTO @WorkListItems(WorkListID)
SELECT RequestType, RequestNumber, FacilityCode, Replace(AccountType, 'Account-', '') AS AccountType,  AccountNumber, MRN, HOST_NAME()
FROM OPENJSON (@json)
WITH
(
	  RequestType varchar(5) '$.partOf.identifier.value'
	, task nvarchar(max) as json
) as part
CROSS APPLY OPENJSON (part.task)
WITH
(
	RequestNumber varchar(50) '$.focus.reference'
	, FacilityCode varchar(4) '$.focus.display'
	, AccountNumber varchar(50) '$.focus.identifier.value'
	, AccountType varchar(50) '$.focus.type'
	, MRN VARCHAR(50) '$.focus.supplement.value'
)

UPDATE SC
SET SC.ValidationMsg = 'Document does not exists.'
, SC.StatusCode = 3 --- Dismissed
, SC.ExceptionMsg = REPLACE ('Operational skip – Under review request - Charity file rule is missing for <Fcode>', '<Fcode>', sc.FacilityCode)
FROM dbo.SHOV_Charity_API_WorkList AS SC
INNER JOIN @WorkListItems AS W
ON SC.WorklistID = W.WorkListID
LEFT JOIN dbo.SHOV_Charity_Map AS SCM
ON SCM.FacilityCode = SC.FacilityCode
WHERE  
( 
		ApplicationName IS NULL 
	OR NumOfPages IS NULL
)
		
	UPDATE SC
	SET SC.ValidationMsg = ISNULL(SC.ValidationMsg + '; ', '') + 'Unable to process. Direct patient to financial assistance'
	, SC.StatusCode = 3 --- Dismissed
	, SC.ExceptionMsg = ISNULL(SC.ExceptionMsg + '; ', '') 
	+ REPLACE(REPLACE( 'Business rule – Dismissed request - site exception – <FCode> excludes <RequestType>', '<FCode>', SC.FacilityCode ), '<RequestType>', SC.RequestType)
	FROM dbo.SHOV_Charity_API_WorkList AS SC
	INNER JOIN @WorkListItems AS W
	ON SC.WorklistID = W.WorkListID
	WHERE EXISTS 
	(
		SELECT 1 FROM dbo.SHOV_Charity_ExceptionList AS sce
		CROSS APPLY  STRING_SPLIT(sce.RequestType, ',') as d
		WHERE trim(d.value) = SC.RequestType
		AND sce.FacilityCode = SC.FacilityCode
	)

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

