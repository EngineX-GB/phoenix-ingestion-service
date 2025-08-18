@echo off
rem build the app as a windows executable

rem set the path for python interpreter
SET PYTHON_HOME=%USERPROFILE%\AppData\Local\Programs\Python\Python310
SET PYTHON_EXE=%USERPROFILE%\AppData\Local\Programs\Python\Python310\python.exe
SET GIT_HOME="C:\Program Files\Git"
SET PATH=%PATH%;%USERPROFILE%\AppData\Local\Programs\Python\Python310;%GIT_HOME%\bin

rem download dependencies from pip (if required)
pip install fastapi[standard]
pip install mysql-connector-python
pip install fastapi uvicorn python-multipart
pip install pyinstaller

@echo on

rem build the executable
pyinstaller --onefile --add-data="version.json;." --hidden-import mysql.connector --hidden-import=uvicorn --hidden-import=fastapi --hidden-import=main  DataIngestionRunner.py
