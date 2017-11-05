CREATE EVENT SESSION XE_DatabaseSizeChangeEvents
    ON SERVER
    ADD EVENT sqlserver.database_file_size_change
        ( SET collect_database_name = ( 1 ))
    ADD TARGET package0.ring_buffer
    WITH (   MAX_MEMORY = 4096KB ,
             EVENT_RETENTION_MODE = ALLOW_MULTIPLE_EVENT_LOSS ,
             MAX_DISPATCH_LATENCY = 30 SECONDS ,
             MAX_EVENT_SIZE = 0KB ,
             MEMORY_PARTITION_MODE = NONE ,
             TRACK_CAUSALITY = OFF ,
             STARTUP_STATE = ON
         );
GO
 
ALTER EVENT SESSION XE_DatabaseSizeChangeEvents ON SERVER STATE = start; 
GO