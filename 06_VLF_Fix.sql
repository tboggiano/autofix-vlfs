CREATE PROCEDURE [dbo].[VLF_Fix]
(
	@DBName sysname,
	@StopTimeSecs INT = 600,
	@DelayIncrementSecs INT = 1,
	@TargetLogSizeMB INT,
	@IncrementSizeMB FLOAT = 8192,
	@LogBackJobName sysname
) 
AS  
SET NOCOUNT ON

DECLARE @Delay INT
DECLARE @DelayTime DATETIME
DECLARE @CutOffTime DATETIME
DECLARE @sqlcmd NVARCHAR(MAX)
DECLARE @DBCCQuery VARCHAR(99)
DECLARE @LoopCtr INT
DECLARE @StepMB INT
DECLARE @LogName sysname
DECLARE @CurrentLogFileSizeMB INT
DECLARE @VLFCount INT
DECLARE @DBRecoveryModel CHAR(1)
DECLARE @SQLExceptionMsg VARCHAR(MAX)

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
    
-- Set the stop time for the loop
SET @CutOffTime = DATEADD(s,@StopTimeSecs,GETDATE())
SET @Delay = 0
	
-- Get the recovery model for the database
SET @DBRecoveryModel = ( SELECT ( CASE WHEN d.recovery_model = 3 THEN 'S' ELSE 'F' END  ) FROM sys.databases AS d WHERE d.name = @DBName )

-- Build SQL command for shrink loop
SET @sqlcmd = 
(
    SELECT REPLACE('
        USE [{{@DBName}}];
        CHECKPOINT;
        '
        ,'{{@DBName}}',@DBName)
) +   
	+
	REPLACE(REPLACE(
	'
	SELECT 1
	WHILE @@RowCount > 0
	BEGIN
		SELECT	1
		FROM	msdb.dbo.sysjobs_view job
				INNER JOIN msdb.dbo.sysjobactivity activity
					ON job.job_id = activity.job_id
		WHERE	job.name = "{{JobName}}"
				AND start_execution_date IS NOT NULL
				AND stop_execution_date IS NULL
		ORDER BY start_execution_date DESC
	END '
	,'"', '''')
	,'{{JobName}}', @LogBackJobName)
	 + 
	REPLACE(REPLACE('
	EXEC msdb.dbo.sp_start_job "{{JobName}}"'
	,'"', '''')
	,'{{JobName}}', @LogBackJobName)
	+
	REPLACE(REPLACE(
	'
	SELECT 1
	WHILE @@RowCount > 0
	BEGIN
		SELECT	1
		FROM	msdb.dbo.sysjobs_view job
				INNER JOIN msdb.dbo.sysjobactivity activity
					ON job.job_id = activity.job_id
		WHERE	job.name = "{{JobName}}"
				AND start_execution_date IS NOT NULL
				AND stop_execution_date IS NULL
		ORDER BY start_execution_date DESC
	END'
	,'"', '''')
	,'{{JobName}}', @LogBackJobName)
	 +
(
    SELECT REPLACE('
        DBCC SHRINKFILE ({{file_id}} , 0, TRUNCATEONLY) WITH NO_INFOMSGS;
        CHECKPOINT;
		DBCC SHRINKFILE ({{file_id}} , 0) WITH NO_INFOMSGS;
        '
        ,'{{file_id}}', CONVERT(VARCHAR(99), [file_id]))
    FROM master.sys.master_files
    WHERE type_desc = 'log'
		AND DB_NAME(database_id) = @DBName
) + '
CHECKPOINT;
'

-- Set log name and target size to value of parameter supplied, or existing size if no parameter value supplied
SELECT TOP 1 @LogName = name, @TargetLogSizeMB = ROUND(ISNULL(@TargetLogSizeMB,[size]/128.0),0) FROM master.sys.master_files WHERE database_id = DB_ID(@DBName) AND type = 1 ORDER BY size DESC

-- Get VLF info and store in temporary table
SET @DBCCQuery = REPLACE(REPLACE(
		'DBCC loginfo ("{{DatabaseName}}") WITH NO_INFOMSGS, TABLERESULTS'
		,'"','''')
		,'{{DatabaseName}}', @DBName)
IF(@serverVersion >= @sqlServer2012Version)
    BEGIN
INSERT INTO #VLFInfo2012
EXEC (@DBCCQuery)

SELECT @VLFCount = COUNT(*) 
FROM #VLFInfo2012

SELECT TOP 1 @CurrentLogFileSizeMB = ROUND([size]/128.0,0) 
FROM master.sys.master_files 
WHERE database_id = DB_ID(@DBName) 
	AND type = 1 
ORDER BY size DESC

-- Run the shrinking loop
WHILE ( (GETDATE()<@CutOffTime) AND ((@CurrentLogFileSizeMB > 100) OR (@VLFCount > 8)) AND (@VLFCount > 2))
BEGIN
	-- Run the shrink command only if the most recent log VLF is not active      
	IF ( (SELECT TOP 1 Status FROM #VLFInfo2012 ORDER BY StartOffset DESC)<>2 )
		BEGIN
			EXEC sys.sp_executesql @sqlcmd
		END
	-- Reset values          
	TRUNCATE TABLE #VLFInfo2012 
	     
	INSERT INTO #VLFInfo2012
	EXEC (@DBCCQuery)

	SELECT @VLFCount = COUNT(*) 
	FROM #VLFInfo2012

	SELECT TOP 1 @CurrentLogFileSizeMB = ROUND([size]/128.0,0) 
	FROM master.sys.master_files 
	WHERE database_id = DB_ID(@DBName) 
		AND type = 1 
	ORDER BY size DESC
	
	SET @Delay = @Delay + @DelayIncrementSecs
	SET @DelayTime = DATEADD(s,@Delay,GETDATE())
	
	PRINT 'Waiting for ' + CONVERT(VARCHAR(99),@Delay) + ' seconds ...'
	PRINT 'Current log file size is ' + CONVERT(VARCHAR(99),@CurrentLogFileSizeMB) + 'MB'
	PRINT 'Current VLF count is ' + CONVERT(VARCHAR(99),@VLFCount)

	WAITFOR TIME @DelayTime			     
END

SET @sqlcmd = '-- Target size in MB is ' + ISNULL(CONVERT(VARCHAR(99),@TargetLogSizeMB),'Unknown') + CHAR(13) + CHAR(10)
SET @sqlcmd = @sqlcmd + '-- LogFile name is ' + @LogName + CHAR(13) + CHAR(10)
SET @sqlcmd = @sqlcmd + '-- Ideal increment size is ' + @LogName + CHAR(13) + CHAR(10)

-- Set increment size as close to ideal size as possible (this works better if a target size is supplied that is a multiple of the increment size obviously)
SELECT @StepMB = ROUND(@TargetLogSizeMB / CEILING(@TargetLogSizeMB / @IncrementSizeMB),0) 
		,@LoopCtr = CEILING(@TargetLogSizeMB / @IncrementSizeMB)     
		,@TargetLogSizeMB = @StepMB

WHILE (@LoopCtr > 0)
BEGIN
	SELECT @sqlcmd = @sqlcmd + 'ALTER DATABASE [' + @DBName + '] MODIFY FILE (NAME = N''' + @LogName + ''', SIZE = ' + CONVERT(VARCHAR(9),@TargetLogSizeMB) + 'MB);' + CHAR(13) + CHAR(10)
	SELECT @TargetLogSizeMB = @TargetLogSizeMB + @StepMB,@LoopCtr = @LoopCtr - 1
END

IF ( ((GETDATE()<@CutOffTime) AND ((@CurrentLogFileSizeMB <= 100) OR (@VLFCount = 2))))
BEGIN  
	EXEC sys.sp_executesql @sqlcmd
END  
ELSE
BEGIN
	PRINT 'Unable to reduce VLFs sufficiently within the specified time period ...' 
        
	PRINT 'Please try again'
	SET @SQLExceptionMsg = 'Current log file size is ' + CONVERT(VARCHAR(99),@CurrentLogFileSizeMB) + 'MB'
		+ 'Current VLF count is ' + CONVERT(VARCHAR(99),@VLFCount)
	RAISERROR(@SQLExceptionMsg, 16, 1)
END  

DROP TABLE #VLFInfo2012
END
ELSE
BEGIN
INSERT INTO #VLFInfo2008
EXEC (@DBCCQuery)

SELECT @VLFCount = COUNT(*) 
FROM #VLFInfo2008

SELECT TOP 1 @CurrentLogFileSizeMB = ROUND([size]/128.0,0) 
FROM master.sys.master_files 
WHERE database_id = DB_ID(@DBName) 
	AND type = 1 
ORDER BY size DESC

-- Run the shrinking loop
WHILE ( (GETDATE()<@CutOffTime) AND ((@CurrentLogFileSizeMB > 100) OR (@VLFCount > 8)) AND (@VLFCount > 2))
BEGIN
	-- Run the shrink command only if the most recent log VLF is not active      
	IF ( (SELECT TOP 1 Status FROM #VLFInfo2008 ORDER BY StartOffset DESC)<>2 )
		BEGIN
			EXEC sys.sp_executesql @sqlcmd
		END
	-- Reset values          
	TRUNCATE TABLE #VLFInfo2008 
	     
	INSERT INTO #VLFInfo2008
	EXEC (@DBCCQuery)

	SELECT @VLFCount = COUNT(*) 
	FROM #VLFInfo2008

	SELECT TOP 1 @CurrentLogFileSizeMB = ROUND([size]/128.0,0) 
	FROM master.sys.master_files 
	WHERE database_id = DB_ID(@DBName) 
		AND type = 1 
	ORDER BY size DESC
	
	SET @Delay = @Delay + @DelayIncrementSecs
	SET @DelayTime = DATEADD(s,@Delay,GETDATE())
	
	PRINT 'Waiting for ' + CONVERT(VARCHAR(99),@Delay) + ' seconds ...'
	PRINT 'Current log file size is ' + CONVERT(VARCHAR(99),@CurrentLogFileSizeMB) + 'MB'
	PRINT 'Current VLF count is ' + CONVERT(VARCHAR(99),@VLFCount)

	WAITFOR TIME @DelayTime			     
END

SET @sqlcmd = '-- Target size in MB is ' + ISNULL(CONVERT(VARCHAR(99),@TargetLogSizeMB),'Unknown') + CHAR(13) + CHAR(10)
SET @sqlcmd = @sqlcmd + '-- LogFile name is ' + @LogName + CHAR(13) + CHAR(10)
SET @sqlcmd = @sqlcmd + '-- Ideal increment size is ' + @LogName + CHAR(13) + CHAR(10)

-- Set increment size as close to ideal size as possible (this works better if a target size is supplied that is a multiple of the increment size obviously)
SELECT @StepMB = ROUND(@TargetLogSizeMB / CEILING(@TargetLogSizeMB / @IncrementSizeMB),0) 
		,@LoopCtr = CEILING(@TargetLogSizeMB / @IncrementSizeMB)     
		,@TargetLogSizeMB = @StepMB

WHILE (@LoopCtr > 0)
BEGIN
	SELECT @sqlcmd = @sqlcmd + 'ALTER DATABASE [' + @DBName + '] MODIFY FILE (NAME = N''' + @LogName + ''', SIZE = ' + CONVERT(VARCHAR(9),@TargetLogSizeMB) + 'MB);' + CHAR(13) + CHAR(10)
	SELECT @TargetLogSizeMB = @TargetLogSizeMB + @StepMB,@LoopCtr = @LoopCtr - 1
END

IF ( ((GETDATE()<@CutOffTime) AND ((@CurrentLogFileSizeMB <= 100) OR (@VLFCount = 2))))
BEGIN  
	EXEC sys.sp_executesql @sqlcmd
END  
ELSE
BEGIN
	PRINT 'Unable to reduce VLFs sufficiently within the specified time period ...' 
        
	PRINT 'Please try again'
	SET @SQLExceptionMsg = 'Current log file size is ' + CONVERT(VARCHAR(99),@CurrentLogFileSizeMB) + 'MB'
		+ 'Current VLF count is ' + CONVERT(VARCHAR(99),@VLFCount)
	RAISERROR(@SQLExceptionMsg, 16, 1)
END  

DROP TABLE #VLFInfo2008
END