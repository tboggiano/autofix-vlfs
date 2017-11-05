# autofix-vlfs
Automatically fix high VLF counts in SQL Server 2012+
For more information how this works see:
http://databasesuperhero.com/archive/2017/09/high-vlf-count-fix/

After creating the objects create a SQL Agent job.  Step one will call procedure VLF_UpdateInfo.  Step two will call VLF_AutoFix with the parameter @LogBackJobName parameter specifying your log back job so it can help mark the VLFs a the end the log file not active.
