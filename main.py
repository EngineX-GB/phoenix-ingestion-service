from fastapi import FastAPI, UploadFile, File

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
    contents = await file.read()
    return {"filename": file.filename, "content_type": file.content_type, "size": len(contents)}