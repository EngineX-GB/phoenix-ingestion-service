Run pip commands:

`pip install "fastapi[standard]"` and/or `pip install fastapi uvicorn`

`pip install mysql-connector-python`
`pip install pyinstaller`
`C:\Users\Dell\AppData\Local\Programs\Python\Python310\python.exe -m pip install --upgrade pip`

use pyinstaller:
`pyinstaller --onefile --hidden-import mysql.connector --hidden-import=uvicorn --hidden-import=fastapi --hidden-import=main  DataIngestionRunner.py`

Run the server:

`fastapi dev main.py`

Launch on:

`http://localhost:8000`

OpenAPI Docs:

`http://localhost:8000/docs`

