import io
import zipfile

from fastapi import FastAPI, UploadFile, File
from starlette.responses import JSONResponse
from controller.DataIngestionImpl import DataIngestionImpl
from controller.PropertyManager import PropertyManager

app = FastAPI()
property_manager = PropertyManager()
data_ingestion = DataIngestionImpl(property_manager)


@app.get("/")
def read_root():
    return {"Hello": "World"}


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

    # create a dictionary, where the key is the subfolder (i.e. the date such as '2025-08-08')
    # and the values are the list of files that are in the subfolder

    files_dict = {}

    with zipfile.ZipFile(zip_stream, "r") as zip_ref:
        all_files = zip_ref.namelist()
        text_files = [f for f in all_files if f.endswith(".txt") and "/" in f]
        text_files.sort(key=lambda x: x.split("/")[0])  # Sort by subdirectory

        for file_name in text_files:
            date = file_name.split("/")[0]
            if files_dict.get(date) is None:
                files_dict.update({date: [file_name]})
            else:
                list = files_dict.get(date)
                list.append(file_name)
                files_dict.update({date: list})

        # sort the keys in the map (by date)
        sorted(files_dict.keys())

        for k, v in files_dict.items():
            # get the list of files for the selected (dated) subdirectory
            # and pass this into the loader
            content_object_list = []
            for file_name in v:
                with zip_ref.open(file_name) as f:
                    with io.TextIOWrapper(f, encoding="utf-8") as text_file:
                        content_object_list.append(text_file)
                        data_ingestion.load_feed_data_via_text_wrapper(content_object_list)
            content_object_list.clear()
    return {"files": results}
