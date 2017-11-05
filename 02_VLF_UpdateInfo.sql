CREATE PROCEDURE dbo.VLF_UpdateInfo
AS
SET NOCOUNT ON
 
DECLARE 
	@DBName SYSNAME,
	@vlfcount INT,
	@activevlfcount INT,
	@dbccquery varchar(1000),
	@currentlogsizeMB INT
 
CREATE TABLE #VLFSummary
    (
      DBName SYSNAME ,
      NumOfVLFs INT ,
      ActiveVLfs INT ,
      RecoveryMode VARCHAR(99) ,
      LogSizeMB INT
    )
 
CREATE TABLE #VLFInfo
    (
      RecoveryUnitId TINYINT ,
      FileId TINYINT ,
      FileSize BIGINT ,
      StartOffset BIGINT ,
      FSeqNo INT ,
      [Status] TINYINT ,
      Parity TINYINT ,
      CreateLSN NUMERIC(25, 0)
    )
 
DECLARE csr CURSOR FAST_FORWARD READ_ONLY
FOR
SELECT name
FROM master.sys.databases WHERE database_id <> 2 
AND [state] = 0
AND is_read_only = 0 
 
OPEN csr
 
FETCH NEXT FROM csr INTO @dbname
 
WHILE (@@fetch_status <> -1)
BEGIN
	SET @dbccquery = REPLACE(REPLACE(
		'DBCC loginfo ("{{DatabaseName}}") WITH NO_INFOMSGS, TABLERESULTS'
		,'"','''')
		,'{{DatabaseName}}', @dbname)
 
	TRUNCATE TABLE #VLFInfo
		
	INSERT INTO #VLFInfo
	EXEC (@dbccquery)
 
	SET @vlfcount = @@rowcount
 
	SELECT @activevlfcount = COUNT(*) 
	FROM #VLFInfo WHERE [Status] = 2
 
	SELECT @currentlogsizeMB = (size/128) 
	FROM master.sys.master_files 
	WHERE type_desc = 'log' 
		AND DB_NAME(database_id)=@dbname
 
	INSERT INTO #VLFSummary
	VALUES(@dbname, @vlfcount, @activevlfcount, CONVERT(VARCHAR(7),DATABASEPROPERTYEX(@dbname, 'Recovery')), @currentlogsizeMB)
 
	FETCH NEXT FROM csr INTO @dbname
END
 
CLOSE csr
DEALLOCATE csr
 
TRUNCATE TABLE dbo.VLFInfo
 
INSERT INTO dbo.VLFInfo (DBName, NumOfVLFs, ActiveVLFs, RecoveryMode, LogSizeMB)
SELECT DBName, NumOfVLFs, ActiveVLFs, RecoveryMode, LogSizeMB 
FROM #VLFSummary
 
DROP TABLE #VLFSummary  
GO