import io
import zipfile

from fastapi import FastAPI, UploadFile, File
import fastapi.responses
from starlette.responses import JSONResponse

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello" : "World"}


#
#   To upload the feed file, requestor must ensure that
#   the following fields are set in the multi-part form

#   key = "file"    value = <uploaded_file>
#
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    if not file.filename.endswith(".zip"):
        return JSONResponse(status_code=400, content={"error": "Only ZIP files are supported."})

        # Read the uploaded file into memory
    zip_bytes = await file.read()
    zip_stream = io.BytesIO(zip_bytes)

    results = {}

    with zipfile.ZipFile(zip_stream, "r") as zip_ref:
        all_files = zip_ref.namelist()
        text_files = [f for f in all_files if f.endswith(".txt") and "/" in f]
        text_files.sort(key=lambda x: x.split("/")[0])  # Sort by subdirectory

        for file_name in text_files:
            with zip_ref.open(file_name) as f:
                with io.TextIOWrapper(f, encoding="utf-8") as text_file:
                    content = text_file.read()
                    results[file_name] = content

    return {"files": results}
