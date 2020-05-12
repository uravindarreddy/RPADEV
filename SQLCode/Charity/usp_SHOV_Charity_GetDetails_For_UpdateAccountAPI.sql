CREATE OR ALTER PROCEDURE dbo.usp_SHOV_Charity_GetDetails_For_UpdateAccountAPI
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED


	SELECT FacilityCode, RequestType, AccountNumber, UserName, StatusCode, ValidationMsg as Notes
	FROM SHOV_Charity_Requests as SCR
	WHERE StatusCode <> 1
	UNION ALL
	SELECT FacilityCode, RequestType, AccountNo, UserName, StatusCode, ValidationMsg
	FROM SHOV_Charity_API_WorkList as SCAW
	WHERE StatusCode <> 1
	AND NOT EXISTS (SELECT 1 
	FROM SHOV_Charity_Requests as SCR
	WHERE SCR.FacilityCode = SCAW.FacilityCode
	AND SCR.AccountNumber = SCAW.AccountNo
	AND SCR.MRN = SCAW.MRN
	AND SCR.ReqDate = SCAW.ReqDate)	
END
GO