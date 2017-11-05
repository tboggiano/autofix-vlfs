----------------------------------------------------------------------------------
-- Procedure Name: VLF_AutoFix
--
-- Desc: Runs VLF_Fix if Ideal VLFCount is over 100 and VLFCount over IdealCount + @VLFIdealOver 
--
--
-- Notes:  Recommendations are to have each VLF be no more than 512 MBs 
--	http://www.sqlskills.com/blogs/kimberly/transaction-log-vlfs-too-many-or-too-few/
--	chunks less than 64MB and up to 64MB = 4 VLFs
--	chunks larger than 64MB and up to 1GB = 8 VLFs
--	chunks larger than 1GB = 16 VLFs
--	ideal size for a VLF 512 MBs, 20 to 30 VLfs , 50 high mark
--  https://www.sqlskills.com/blogs/paul/important-change-vlf-creation-algorithm-sql-server-2014/
--  Algorithms update for 2014 and up
--  Is the growth size less than 1/8 the size of the current log size?
--  Yes: create 1 new VLF equal to the growth size
--  No: use the formula above
----------------------------------------------------------------------------------
CREATE PROCEDURE dbo.VLF_AutoFix
(
	@LookBackTime INT = 60 , -- Number of minutes to look back in the XE to see if things have changed
	@VLFIdealOver INT = 20 , -- Number of over the Ideal number of VLFs is OK
	@VLFCountMin INT = 100 , -- The minimum of VLFs the log has to have to try to fix it
	@VLFIdealSize INT = 512 , -- Ideal size of each VLF
	@HoursSinceLastFix INT = 6 , -- Number of hours since the last time it tried to fix it
	@MaxIncrementSizeMB INT = 8192, -- Size to increase by, in 2012 and below this 8192 gives is our 512
	@LogBackJobName sysname --Name of log backup job on server
)
AS
SET NOCOUNT ON

CREATE TABLE #VLFInfo
(
	RecoveryUnitID INT ,
	FileID INT ,
	FileSize BIGINT ,
	StartOffset BIGINT ,
	FSeqNo BIGINT ,
	[Status] BIGINT ,
	Parity BIGINT ,
	CreateLSN NUMERIC(38)
);
	 
CREATE TABLE #VLFCountResults
	(
		DatabaseName SYSNAME ,
		VLFCount INT ,
		LogFileSize BIGINT
	);

CREATE TABLE #Events ( DatabaseName SYSNAME );
CREATE TABLE #LogFileSize ( LogFileSizeMB INT );

DECLARE @DBName SYSNAME ,
	@LogFileSize INT ,
	@IncrementSizeMB INT ,
	@VLFCount INT ,
	@SQL NVARCHAR(MAX),
	@return_status int,
	@CurrentLogFileSize INT;

DECLARE vlfcursor CURSOR READ_ONLY
FOR
	SELECT  DBName ,
			NumOfVLFs ,
			LogSizeMB 
	FROM    dbo.VLFInfo 
	WHERE   NumOfVLFs > @VLFCountMin
			AND NumOfVLFs - ( LogSizeMB / @VLFIdealSize ) >= @VLFIdealOver  ;
OPEN vlfcursor;

FETCH NEXT FROM vlfcursor INTO @DBName, @VLFCount, @LogFileSize ;
WHILE ( @@fetch_status <> -1 )
	BEGIN
		IF ( @@fetch_status <> -2 )
			BEGIN
				--Query  to see if log files has been grown in the last @LookBackTime Minutes
				WITH    Data
							AS ( SELECT   CAST(target_data AS XML) AS TargetData
								FROM     sys.dm_xe_session_targets dt
										INNER JOIN sys.dm_xe_sessions ds ON ds.address = dt.event_session_address
								WHERE    dt.target_name = N'ring_buffer'
										AND ds.Name = N'XE_DatabaseSizeChangeEvents'
								)
				INSERT INTO #Events
				SELECT  XEventData.XEvent.value('(action[@name="database_name"]/value)[1]', 'SYSNAME') AS DatabaseName
				FROM    Data d
						CROSS APPLY TargetData.nodes('RingBufferTarget/event[@name=''database_file_size_change'']')
						AS XEventData ( XEvent )
				WHERE   XEventData.XEvent.value('(@timestamp)[1]', 'datetime2') > CONVERT(DATETIME2, DATEADD(MINUTE, -1 * @LookBackTime, GETDATE()))
						AND XEventData.XEvent.value('(data[@name="file_type"]/text)[1]', 'NVARCHAR(120)') = N'Log file'
						AND XEventData.XEvent.value('(action[@name="database_name"]/value)[1]', 'SYSNAME') = @DBName;

				--If no growths in last @LookBackTime * -1 minutes then VLF and this process has not been run on this DB in the last @HoursSinceLastFix
				IF @@ROWCOUNT = 0 AND NOT EXISTS (SELECT 1 FROM dbo.VLFAutoFix WHERE DBName = @DBName AND LogDate>= DATEADD(HOUR, @HoursSinceLastFix * -1, GETDATE()))
					BEGIN
						IF @LogFileSize >= @MaxIncrementSizeMB -- 512 MB limit on VLF size, creates 16 VLFs per growth
							SET @IncrementSizeMB = @MaxIncrementSizeMB;
						ELSE
							SET @IncrementSizeMB = @LogFileSize;  -- Else grow back to original size using size as increment value
						
						--Attempt to shrink and regrow log file
						EXEC @return_status = dbo.VLF_Fix @DBName = @DBName,  
							@IncrementSizeMB = @IncrementSizeMB,
							@TargetLogSizeMB = @LogFileSize,
							@LogBackJobName = @LogBackJobName;

						--If previous shrink and regrow was unsuccessful regrow to original size without shrinking
                        SELECT  @CurrentLogFileSize = ( size / 128 )
                        FROM    master.sys.master_files
                        WHERE   type_desc = 'log'
                                AND DB_NAME(database_id) = @DBName

                        IF @LogFileSize > @CurrentLogFileSize
						BEGIN 
                            EXEC dbo.VLF_Fix
                                @DBName = @DBName ,
                                @IncrementSizeMB = @IncrementSizeMB ,
                                @TargetLogSizeMB = @LogFileSize ,
								@LogBackJobName= @LogBackJobName;
						END
							
						--Record the Auto Fix info to a table
						INSERT INTO dbo.VLFAutoFix (DBName, CurrentVLFCount, LogFileSizeMBs)
						VALUES (@DBName, @VLFCount, @LogFileSize);
					END

				TRUNCATE TABLE #Events;
			END
		FETCH NEXT FROM vlfcursor INTO @DBName, @VLFCount, @LogFileSize;
	END

CLOSE vlfcursor;
DEALLOCATE vlfcursor;

DROP TABLE #VLFInfo;
DROP TABLE #VLFCountResults;
DROP TABLE #Events;
GO