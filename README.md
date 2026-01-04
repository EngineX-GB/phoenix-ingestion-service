Run pip commands:

`pip install "fastapi[standard]"` and/or `pip install fastapi uvicorn`

`pip install mysql-connector-python`
`pip install pyinstaller`
`pip install fastapi uvicorn python-multipart`
`C:\Users\Dell\AppData\Local\Programs\Python\Python310\python.exe -m pip install --upgrade pip`


use pyinstaller:
`pyinstaller --onefile --hidden-import mysql.connector --hidden-import=uvicorn --hidden-import=fastapi --hidden-import=main  DataIngestionRunner.py`

Run the server:

`fastapi dev main.py`

Launch on:

`http://localhost:8000`

OpenAPI Docs:

`http://localhost:8000/docs`

Here is an example of a query:

DataIngestionRunner.exe --cmd --dynamic "C:\Users\Dell\Documents\phoenix-feed-prod\feeds\2023" "C:\Users\Dell\PycharmProjects\data-ingestion\configs\ingestion-config-28-col.json,C:\Users\Dell\PycharmProjects\data-ingestion\configs\ingestion-config-29-col.json,C:\Users\Dell\PycharmProjects\data-ingestion\configs\ingestion-config-30-col.json,C:\Users\Dell\PycharmProjects\data-ingestion\configs\ingestion-config-31-col.json,C:\Users\Dell\PycharmProjects\data-ingestion\configs\ingestion-config-32-col.json" True