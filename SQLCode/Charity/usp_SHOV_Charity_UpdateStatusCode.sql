CREATE OR ALTER PROCEDURE dbo.usp_SHOV_Charity_UpdateStatusCode
(@ReqID int, @StatusCode tinyint)
AS
BEGIN
SET NOCOUNT ON;
BEGIN TRY
	BEGIN TRAN

	IF @ReqID IS NULL OR @StatusCode IS NULL
	BEGIN
		RAISERROR('The paramter value/s is/are invalid.', 16,1)
	END 
	ELSE
	BEGIN
		UPDATE SHOV_Charity_Requests
		SET StatusCode = @StatusCode
		WHERE ReqID = @ReqID
	END
	COMMIT TRAN

END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK TRAN
	END
END CATCH
END
GO

