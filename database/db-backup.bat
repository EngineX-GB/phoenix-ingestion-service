@echo off
SET MYSQL_HOME="C:\Program Files\MySQL\MySQL Server 8.3\bin"
%MYSQL_HOME%\mysqldump -uroot -proot --routines --triggers --events db_phoenix > db_phoenix_backup_260226.sql
@echo on