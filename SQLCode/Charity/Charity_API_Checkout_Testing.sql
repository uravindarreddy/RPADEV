
DECLARE @json AS NVARCHAR(MAX) = ''
-- Load file contents into a variable
--SELECT @json = BulkColumn
-- FROM OPENROWSET (BULK 'D:\HOV\api-response\checkout-response-051901178007.json', SINGLE_CLOB) as j
 --SELECT @json = BulkColumn
 --FROM OPENROWSET (BULK 'D:\HOV\api-response\checkout-response-101381123005.json', SINGLE_CLOB) as j
--SELECT @json = BulkColumn
-- FROM OPENROWSET (BULK 'D:\HOV\api-response\checkout-response-101657602104.json', SINGLE_CLOB) as j
--SELECT @json = BulkColumn
-- FROM OPENROWSET (BULK 'D:\HOV\api-response\checkout-response-101893204103.json', SINGLE_CLOB) as j

EXEC [dbo].[usp_SHOV_Charity_IndexData] 'FirstClass'

EXEC dbo.usp_SHOV_Charity_CheckAccount @json

SELECT * FROM dbo.SHOV_Charity_Requests

--TRUNCATE TABLE dbo.SHOV_Charity_Requests
