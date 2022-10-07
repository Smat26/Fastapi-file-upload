"""
Run with `uvicorn main:app`

Then run `python client.py`
"""

from fastapi import FastAPI, Form, UploadFile, HTTPException
import glob
import os
import logging

SESSION = {}

app = FastAPI()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s",
)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)


@app.get("/purge")
def purge():
    cwd = os.getcwd()
    files = glob.glob(f"{cwd}/{DATA_DIR}/*")
    for f in files:
        os.remove(f)


@app.post("/upload")
async def upload(
    file: UploadFile,
    file_name: str = Form(...),
    chunk_index: int = Form(...),
    chunk_byte_offset: int = Form(...),
    total_chunks: int = Form(...),
    file_size: int = Form(...),
):
    #
    if file_name not in SESSION:
        SESSION.update({file_name: 0})
    else:
        SESSION[file_name] += 1
    #
    # log.info("sessionD %r", sessionD)
    #
    save_path = os.path.join(DATA_DIR, file_name)
    if os.path.exists(save_path) and SESSION[file_name] == 0:
        log.error("File already exist. Remove it from the upload directory")
        raise HTTPException(status_code=500, detail="File already exists. ")
    #
    try:
        with open(save_path, "wb") as f:
            # log.info(f"Seek position writing: {chunk_byte_offset}")
            # log.info(f"Chunk number should be: {chunk_byte_offset /(8*1024*1024)}")
            f.seek(chunk_byte_offset)
            f.write(file.file.read())
    except OSError:
        log.exception("Could not write to file")
        raise HTTPException(status_code=500, detail="Could not write to file")
    
    log.info("chunk_index %r, total_chunks %r", chunk_index, total_chunks)
    log.info("save_path_size %r, file_size %r", os.path.getsize(save_path), file_size)
    
    if SESSION[file_name] + 1 == total_chunks:
        if os.path.getsize(save_path) != file_size:
            log.error(
                f"File {file_name} was completed, "
                f"but has a size mismatch."
                f"Was {os.path.getsize(save_path)} but we"
                f" expected {file_size} "
            )
            raise HTTPException(status_code=500, detail="size mismatch")
        else:
            log.info(f"File {file_name} has been uploaded successfully")
            del SESSION[file_name]
    else:
        log.debug(
            f"Chunk {chunk_index + 1} of {total_chunks} "
            f"for file {file_name} complete"
        )
    return {"message": f"Chunk #{chunk_index} upload successful for {file_name} "}
