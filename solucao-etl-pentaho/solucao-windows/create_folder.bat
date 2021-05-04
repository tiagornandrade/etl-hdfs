@echo off
cls
SET YEAR=%date:~-4%
SET MONTH=%date:~3,2%
SET DAY=%date:~0,2%

cd C:\hadoop-2.8.0\bin
hadoop fs -mkdir -p /user/datalake/raw/%YEAR%/%MONTH%/%DAY%