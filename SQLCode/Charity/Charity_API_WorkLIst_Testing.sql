
DECLARE @json AS NVARCHAR(MAX) = ''
-- Load file contents into a variable
SELECT @json = BulkColumn
 FROM OPENROWSET (BULK 'D:\HOV\api-response\worklist-response.json', SINGLE_CLOB) as j


EXEC dbo.usp_SHOV_Charity_InsertWorkList @json

SELECT *
FROM dbo.SHOV_Charity_API_WorkList AS SC	
WHERE EXISTS 
	(
		SELECT 1 FROM dbo.SHOV_Charity_ExceptionList AS sce
		CROSS APPLY  STRING_SPLIT(sce.RequestType, ',') as d
		WHERE trim(d.value) = SC.RequestType
		AND sce.FacilityCode = SC.FacilityCode
	)

SELECT *, trim(value) as v
FROM dbo.SHOV_Charity_ExceptionList AS sce
CROSS APPLY  STRING_SPLIT(sce.RequestType, ',') as d

SELECT * FROM dbo.SHOV_Charity_API_WorkList
where RequestType in ('CHRTY', 'CSUCF')
and ValidationMsg is not null

select * from SHOV_Charity_Map

SELECT distinct RequestType FROM dbo.SHOV_Charity_API_WorkList

SELECT * 
FROM dbo.SHOV_Charity_API_WorkList as a
inner join dbo.SHOV_Charity_ExceptionList as b
on a.FacilityCode = b.FacilityCode


--TRUNCATE TABLE dbo.SHOV_Charity_API_WorkList


select * from SHOV_Charity_Map