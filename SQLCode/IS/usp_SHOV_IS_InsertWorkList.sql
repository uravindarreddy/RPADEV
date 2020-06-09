CREATE OR ALTER PROCEDURE [dbo].[usp_SHOV_IS_InsertWorkList]
(
	@json AS NVARCHAR(MAX)
)
AS
BEGIN
SET NOCOUNT ON;

BEGIN TRY
BEGIN TRAN
DECLARE @WorkListItems AS TABLE (WorkListID int)

	INSERT INTO dbo.SHOV_IS_API_WorkList
	(RequestType, RequestNo, FacilityCode, AccountType, AccountNo, MRN, UserName)
	OUTPUT inserted.WorklistID INTO @WorkListItems (WorkListID)
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

	UPDATE S
	SET S.ValidationMsg = 'Unable to process. Account in bad debt'
	, S.StatusCode = 3 --- Dismissed
	, S.ExceptionMsg = 'Business rule - Dismissed request - account in bad debt'
	, S.LockFlag = 1
	FROM dbo.SHOV_IS_API_WorkList AS S
	INNER JOIN @WorkListItems as w
	ON S.WorklistID = w.WorkListID
	WHERE S.AccountType = 'BD';



COMMIT TRAN
END TRY
BEGIN CATCH

	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRAN
	END;
	THROW;
END CATCH
END
GO
