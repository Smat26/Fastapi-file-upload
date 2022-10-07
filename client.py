import concurrent.futures
from concurrent.futures import wait, ThreadPoolExecutor, as_completed, FIRST_COMPLETED
import requests
import os
import io
import time
from copy import deepcopy

chunk_size = 1024 * 1024 * 8
DEBUG = False
UPLOAD_URL = "http://127.0.0.1:8000/upload"
PURGE_URL = "http://127.0.0.1:8000/purge"
FILES_TO_UPLOAD = [
    # "/Users/ahsan/Downloads/GeForceNOW-release.dmg",
    "/Users/ahsan/Downloads/options_installer.zip",
    # '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.14gb',
    # '/Users/dennis/rcsb/py-rcsb_app_file/rcsb/app/tests-file/test-data/bigFile.txt.30gb',
]


def serial_test():
    print(f"Uploading {len(FILES_TO_UPLOAD)} files serially")
    results = []
    for file in FILES_TO_UPLOAD:
        method = upload_async_file if ASYNC else upload_file
        results.append(method(file))
    print("Serial Uploading Result")
    for result in results:
        print(result)


def concurrent_test():
    print(f"Uploading {len(FILES_TO_UPLOAD)} files with threadpool")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(upload_async_file if ASYNC else upload_file, file): file
            for file in FILES_TO_UPLOAD
        }
        results = []

        for future in as_completed(futures):
            results.append(future.result())
        print("Multi-threaded multiple files result")
        for result in results:
            print(result)


def human_friendly(bites):
    unit = "B"
    if bites > 1024:
        bites = bites / 1024
        unit = "KB"
    if bites > 1024:
        bites = bites / 1024
        unit = "MB"
    if bites > 1024:
        bites = bites / 1024
        unit = "GB"
    return f"{bites} {unit}"


def testing_stats(func):
    """For calculating time for a given function"""

    def get_statistics(*args, **kwargs):
        timings = {}
        file_path = args[0]
        size = os.path.getsize(file_path)
        timings["File_size"] = human_friendly(size)
        start = time.time()
        r = func(*args, **kwargs)
        if isinstance(r, concurrent.futures.Future):
            r = r.result()
        if r.status_code == 200:
            timings["Duration"] = time.time() - start
            return timings
        else:
            print("No Stats. Request Failed")
            return None

    return get_statistics


def async_request(req_body, tmp):
    return requests.post(UPLOAD_URL, data=req_body, files={"file": tmp})


@testing_stats
def upload_file(file_path):
    response = None
    total_chunks = 0
    file_size = os.path.getsize(file_path)

    if chunk_size < file_size:
        total_chunks = file_size // chunk_size
        if file_size % chunk_size:
            total_chunks = total_chunks + 1
    else:
        total_chunks = 1

    req_body = {
        "file_name": file_path.split("/")[-1],
        "chunk_index": 0,
        "chunk_byte_offset": 0,
        "total_chunks": total_chunks,
        "file_size": file_size,
    }

    tmp = io.BytesIO()
    with open(file_path, "rb") as to_upload:
        for i in range(0, req_body["total_chunks"]):
            packet_size = min(
                req_body["file_size"] - (req_body["chunk_index"] * chunk_size),
                chunk_size,
            )
            tmp.truncate(packet_size)
            tmp.seek(0)
            tmp.write(to_upload.read(packet_size))
            tmp.seek(0)

            response = requests.post(UPLOAD_URL, data=req_body, files={"file": tmp})
            if DEBUG:
                print(str(response.content))
            if response.status_code != 200:
                break
            req_body["chunk_index"] = req_body["chunk_index"] + 1
            req_body["chunk_byte_offset"] = req_body["chunk_index"] * chunk_size
    return response


@testing_stats
def upload_async_file(file_path):
    response = None
    total_chunks = 0
    file_size = os.path.getsize(file_path)
    send_queue = []

    if chunk_size < file_size:
        total_chunks = file_size // chunk_size
        if file_size % chunk_size:
            total_chunks = total_chunks + 1
    else:
        total_chunks = 1

    req_body = {
        "file_name": file_path.split("/")[-1],
        "chunk_index": 0,
        "chunk_byte_offset": 0,
        "total_chunks": total_chunks,
        "file_size": file_size,
    }

    pool = ThreadPoolExecutor(max_workers=2)

    tmp = io.BytesIO()
    with open(file_path, "rb") as to_upload:
        for i in range(0, req_body["total_chunks"]):
            packet_size = min(
                req_body["file_size"] - (req_body["chunk_index"] * chunk_size),
                chunk_size,
            )
            tmp.truncate(packet_size)
            tmp.seek(0)
            tmp.write(to_upload.read(packet_size))
            tmp.seek(0)
            if len(send_queue) >= 2:
                done, not_done = wait(send_queue, return_when=FIRST_COMPLETED)
                completed_future = done.pop()
                to_remove = send_queue.index(completed_future)
                send_queue.pop(to_remove)

                response = completed_future.result()
                if response.status_code != 200:
                    print(response.content, response.request)

            send_queue.append(
                pool.submit(async_request, deepcopy(req_body), deepcopy(tmp))
            )

            req_body["chunk_index"] = req_body["chunk_index"] + 1
            req_body["chunk_byte_offset"] = req_body["chunk_index"] * chunk_size
    return response


if __name__ == "__main__":
    print("Without ASYNC")
    # ASYNC = False
    # response = requests.get(PURGE_URL)
    # serial_test()  # Each file is uploaded one after another
    # response = requests.get(PURGE_URL)
    # concurrent_test()  # 10 files being uploaded concurrently

    print("With ASYNC")
    ASYNC = True  # Same file can have upto 10 chunks sent concurrently
    # response = requests.get(PURGE_URL)
    # serial_test()
    response = requests.get(PURGE_URL)
    concurrent_test()
