CREATE OR ALTER PROCEDURE dbo.usp_SHOV_Charity_GetWorkList
AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY
	BEGIN TRAN

	DECLARE @WorkList AS TABLE
	(
			ReqID int 
		, ReqDate date 
		, AccountNumber varchar(40)
		, FacilityCode varchar(4)
		, RequestType varchar(5)
		, PatientName varchar(40)
		, DOB date
		, GuarantorName varchar(40)
		, AddressLine1 varchar(40)
		, AddressLine2 varchar(40)
		, AddressCity varchar(40)
		, AddressState varchar(40)
		, ZipCode varchar(20)
		, StatusCode tinyint 
		, StageCode tinyint 
		, LockFlag tinyint
		, ValidationMsg varchar(max)
		, UserName varchar(40)
		, MRN varchar(40)
	)

	;WITH GetWorkList
	AS
	(
		SELECT TOP(1) 
		 SCR.[ReqID]
		, SCR.[ReqDate]
		, SCR.[AccountNumber]
		, SCR.[FacilityCode]
		, SCR.[RequestType]
		, SCR.[PatientName]
		, SCR.[DOB]
		, SCR.[GuarantorName]
		, SCR.[AddressLine1]
		, SCR.[AddressLine2]
		, SCR.[AddressCity]
		, SCR.[AddressState]
		, SCR.[ZipCode]
		, SCR.[StatusCode]
		, SCR.[StageCode]
		, SCR.[LockFlag]
		, SCR.[ValidationMsg]
		, SCR.[UserName]
		, SCR.MRN
		FROM dbo.SHOV_Charity_Requests AS SCR
		WHERE SCR.LockFlag = 0
		AND SCR.StatusCode = 1 --- Pending Requests
	)
	UPDATE GetWorkList
	SET LockFlag = 1
	OUTPUT 
	 inserted.[ReqID]
	, inserted.[ReqDate]
	, inserted.[AccountNumber]
	, inserted.[FacilityCode]
	, inserted.[RequestType]
	, inserted.[PatientName]
	, inserted.[DOB]
	, inserted.[GuarantorName]
	, inserted.[AddressLine1]
	, inserted.[AddressLine2]
	, inserted.[AddressCity]
	, inserted.[AddressState]
	, inserted.[ZipCode]
	, inserted.[StatusCode]
	, inserted.[StageCode]
	, inserted.[LockFlag]
	, inserted.[ValidationMsg]
	, inserted.[UserName]
	, inserted.[MRN]
	INTO @WorkList
	COMMIT TRAN

	SELECT 
	w.ReqID
	,w.ReqDate
	,w.AccountNumber
	,w.FacilityCode
	,w.RequestType
	,w.PatientName
	,w.DOB
	,w.GuarantorName
	,w.AddressLine1
	,w.AddressLine2
	,w.AddressCity
	,w.AddressState
	,w.ZipCode
	,w.StatusCode
	,w.StageCode
	,w.LockFlag
	,w.ValidationMsg
	,w.UserName
	, scm.ApplicationName AS DocFileName
	, scm.NumOfPages as NumOfPages
	, w.MRN
	FROM @WorkList as w
	INNER JOIN dbo.SHOV_Charity_Map AS SCM
	ON SCM.FacilityCode = w.FacilityCode
	WHERE SCM.ApplicationName IS NOT NULL
	AND SCM.NumOfPages IS NOT NULL;

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

