
CREATE OR ALTER PROCEDURE [dbo].[usp_SHOV_Charity_IndexData]
(
 @ReqID int
	, @ShippingMethod VARCHAR(100) --- This value is passed from Bot Config reader
)
AS
BEGIN
	
	SELECT 
	CAST( ReqDate as date) as ReqDate
	, AccountNumber
	, SCR.FacilityCode
	, RequestType
	, PatientName
	, DOB
	, ISNULL(AddressLine1, '') AS AddressLine1
	, ISNULL(AddressLine2, '') AS AddressLine2
	, ISNULL(AddressCity, '') AS AddressCity
	, ISNULL(AddressState, '') AS AddressState
	, ISNULL(ZipCode, '') AS ZipCode
	, SCM.ApplicationName as DocFileName
	, SCM.NumOfPages AS NoOfPages
	, @ShippingMethod AS ShippingMethod
	, UserName
	 
	FROM [dbo].[SHOV_Charity_Requests] AS SCR
	INNER JOIN dbo.SHOV_Charity_Map AS SCM
	ON SCM.FacilityCode = SCR.FacilityCode
	WHERE scr.ReqID = @ReqID
	AND ( SCM.ApplicationName IS NOT NULL 
			AND SCM.NumOfPages IS NOT NULL
		)
	

END
GO