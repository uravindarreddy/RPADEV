CREATE OR ALTER PROCEDURE dbo.usp_SHOV_Charity_GetAPIWorkList
AS
BEGIN
SET NOCOUNT ON;
---- Fetch all the Pending WorkList Items
---- This will be used to call Checkout Account API
	SELECT 
		WorklistID
		,ReqDate
		,RequestType
		,RequestNo
		,FacilityCode
		,MRN
		,AccountNo
		,AccountType
		,StatusCode
		,LockFlag
		,ValidationMsg
	FROM dbo.SHOV_Charity_API_WorkList 
	WHERE StatusCode = 1
	AND LockFlag = 0; ---- Only Pending WorkList Items
	
END
GO

