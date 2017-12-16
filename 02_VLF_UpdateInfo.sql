CREATE PROCEDURE [dbo].[VLF_UpdateInfo]
AS
SET NOCOUNT ON
 
DECLARE 
	@DBName SYSNAME,
	@vlfcount INT,
	@activevlfcount INT,
	@DBCCQuery varchar(1000),
	@currentlogsizeMB INT
 
CREATE TABLE #VLFSummary
    (
      DBName SYSNAME ,
      NumOfVLFs INT ,
      ActiveVLFs INT ,
      RecoveryMode VARCHAR(99) ,
      LogSizeMB INT
    )
 
-- Need to accomodate SQL Server 2012 (version 11.0)
DECLARE @versionString            VARCHAR(20),
        @serverVersion            DECIMAL(10,5),
        @sqlServer2012Version    DECIMAL(10,5)
 
SET        @versionString    = CAST(SERVERPROPERTY('productversion') AS VARCHAR(20))
SET        @serverVersion = CAST(LEFT(@versionString,CHARINDEX('.', @versionString)) AS DECIMAL(10,5))
SET        @sqlServer2012Version = 11.0 -- SQL Server 2012
 
IF(@serverVersion >= @sqlServer2012Version)
    BEGIN
        -- Use the new version of the table  
        CREATE TABLE #VLFInfo2012
            (
            [RecoveryUnitId]    INT NULL,
            [FileId]            INT NULL,
            [FileSize]            BIGINT NULL,
            [StartOffset]        BIGINT NULL,
            [FSeqNo]            INT NULL,
            [Status]            INT NULL,
            [Parity]            TINYINT NULL,
            [CreateLSN]            NUMERIC(25, 0) NULL
            )
    END  
ELSE  
    BEGIN
        -- Use the old version of the table
        CREATE TABLE #VLFInfo2008
            (
            [FileId]            INT NULL,
            [FileSize]            BIGINT NULL,
            [StartOffset]        BIGINT NULL,
            [FSeqNo]            INT NULL,
            [Status]            INT NULL,
            [Parity]            TINYINT NULL,
            [CreateLSN]            NUMERIC(25, 0) NULL
            )
 
    END
 
DECLARE csr CURSOR FAST_FORWARD READ_ONLY
FOR
SELECT name
FROM master.sys.databases WHERE database_id <> 2 
AND [state] = 0
AND is_read_only = 0 
 
OPEN csr
 
FETCH NEXT FROM csr INTO @DBName
 
WHILE (@@fetch_status <> -1)
BEGIN
	SET @DBCCQuery = REPLACE(REPLACE(
		'DBCC loginfo ("{{DatabaseName}}") WITH NO_INFOMSGS, TABLERESULTS'
		,'"','''')
		,'{{DatabaseName}}', @DBName)
IF(@serverVersion >= @sqlServer2012Version)
	BEGIN
	TRUNCATE TABLE #VLFInfo2012
		
	INSERT INTO #VLFInfo2012
		

	EXEC (@DBCCQuery)
 
	SET @vlfcount = @@rowcount
 
	SELECT @activevlfcount = COUNT(*) 
	FROM #VLFInfo2012 WHERE [Status] = 2
	END
	ELSE
	BEGIN
	TRUNCATE TABLE #VLFInfo2008
		
	INSERT INTO #VLFInfo2008
		

	EXEC (@DBCCQuery)
 
	SET @vlfcount = @@rowcount
 
	SELECT @activevlfcount = COUNT(*) 
	FROM #VLFInfo2008 WHERE [Status] = 2
	END
	SELECT @currentlogsizeMB = (size/128) 
	FROM master.sys.master_files 
	WHERE type_desc = 'log' 
		AND DB_NAME(database_id)=@DBName
 
	INSERT INTO #VLFSummary
	VALUES(@DBName, @vlfcount, @activevlfcount, CONVERT(VARCHAR(7),DATABASEPROPERTYEX(@DBName, 'Recovery')), @currentlogsizeMB)
 
	FETCH NEXT FROM csr INTO @DBName
END
 
CLOSE csr
DEALLOCATE csr
 
TRUNCATE TABLE dbo.VLFInfo
 
INSERT INTO dbo.VLFInfo (DBName, NumOfVLFs, ActiveVLFs, RecoveryMode, LogSizeMB)
SELECT DBName, NumOfVLFs, ActiveVLFs, RecoveryMode, LogSizeMB 
FROM #VLFSummary
 
DROP TABLE #VLFSummary  
