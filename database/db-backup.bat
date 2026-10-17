@echo off
SETLOCAL

rem "Usage:"
rem "db-backup backup [path_to_file]"
rem "Example:db-backup backup C:\db.sql"
rem ""
rem "db-backup restore [path_to_file]"
rem "Example:db-backup restore C:\db.sql"
rem ""
rem "db-backup refresh [path_to_file]"
rem "Example:db-backup refresh C:\db.sql"



SET "MYSQL_HOME=C:\Program Files\MySQL\MySQL Server 8.3\bin"
SET "USERNAME=root"
SET "PASSWORD=root"
SET "DATABASE=db_phoenix"

IF "%~1"=="" GOTO USAGE
IF "%~2"=="" GOTO USAGE

IF /I "%~1"=="backup" GOTO BACKUP
IF /I "%~1"=="restore" GOTO RESTORE
IF /I "%~1"=="refresh" GOTO REFRESH_TO_DEV

:USAGE
echo Usage:
echo   %~nx0 backup filename
echo   %~nx0 restore filename
echo   %~nx0 refresh filename
echo.
echo Example:
echo   %~nx0 backup db_phoenix_backup.sql
echo   %~nx0 restore db_phoenix_backup.sql
GOTO END

:BACKUP
echo Backing up database %DATABASE% to %~2 ...
"%MYSQL_HOME%\mysqldump.exe" -u%USERNAME% -p%PASSWORD% --routines --triggers --events %DATABASE% > "%~2"
IF ERRORLEVEL 1 (
    echo ERROR: Database backup failed.
    GOTO END
)
echo Backup completed successfully.
GOTO END

:RESTORE
echo Restoring database %DATABASE% from %~2 ...
"%MYSQL_HOME%\mysql.exe" -u%USERNAME% -p%PASSWORD% %DATABASE% < "%~2"
IF ERRORLEVEL 1 (
    echo ERROR: Database restore failed.
    GOTO END
)
echo Restore completed successfully.
GOTO END


:REFRESH_TO_DEV
SET "DATABASE=dev_db_phoenix"
echo Refreshing DEV database %DATABASE% from %~2 ...
"%MYSQL_HOME%\mysql.exe" -u%USERNAME% -p%PASSWORD% -e "DROP DATABASE IF EXISTS %DATABASE%; CREATE DATABASE %DATABASE%;"
"%MYSQL_HOME%\mysql.exe" -u%USERNAME% -p%PASSWORD% %DATABASE% < "%~2"
IF ERRORLEVEL 1 (
    echo ERROR: Database refresh failed.
    GOTO END
)
echo Database refresh completed successfully.
GOTO END

:END
ENDLOCAL
@echo on