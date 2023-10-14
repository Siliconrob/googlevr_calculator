import argparse
import io
import os
import zipfile

import uvicorn
from fastapi import FastAPI, UploadFile, HTTPException
from starlette.responses import FileResponse, Response, StreamingResponse

from DataStore import load_db, get_dsn, DB_NAME
from price_calculator.ComputeFeed import compute_feed_price

# parser = argparse.ArgumentParser()
# parser.add_argument("--input_path", action="store", default="C:/test/gvr_inputs")
# parser.add_argument("--load_db", action="store_true", default=True)
# parser.add_argument("--start", action="store", default="")
# parser.add_argument("--end", action="store", default="")
# parser.add_argument("--book_date", action="store", default="")
# parser.add_argument("--external_id", action="store", default="")
# parser.add_argument("--web_ui", action="store_true", default=False)

app = FastAPI()


def iterfile():  #
    if not os.path.exists(DB_NAME):
        yield from ()
    else:
        with open(DB_NAME, mode="rb") as db_file:
            yield from db_file


@app.get("/datasource")
async def current_db():
    return StreamingResponse(content=iterfile(), media_type="application/octet")

@app.post("/feed")
async def feed_price(upload_file: UploadFile, external_id: str, start_date: str, end_date: str, booked_date: str):
    with zipfile.ZipFile(io.BytesIO(upload_file.file.read()), "r") as messages_zip_file:
        # if not zipfile.is_zipfile(messages_zip_file):
        #     raise HTTPException(status_code=405, detail="Messages file input is not a zip file")
        dsn = get_dsn(DB_NAME)
        db_load_results = load_db(messages_zip_file, dsn)
    calculated_feed_price = compute_feed_price(external_id, start_date, end_date, booked_date, dsn)
    return calculated_feed_price


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8900)
    # args = parser.parse_args()
    # dsn = get_dsn(DB_NAME)
    # if args.load_db:
    #     db_load_results = load_db(os.path.abspath(args.input_path), dsn)
    #
    # if args.web_ui is False:
    #     start = args.start if len(args.start) > 0 else "2024-02-21"
    #     end = args.end if len(args.end) > 0 else "2024-02-26"
    #     external_id = args.external_id if len(args.external_id) > 0 else "orp5b45c10x"
    #     book_date = args.book_date if len(args.book_date) > 0 else "2023-10-12"
    #     calculated_feed_price = compute_feed_price(external_id, start, end, book_date, dsn)
    #     print(calculated_feed_price)
    # else:
    #     app = FastAPI()
    #
    #
    #     @app.get("/feed_price")
    #     async def feed_price(external_id: str, start_date: str, end_date: str, booked_date: str):
    #         calculated_feed_price = compute_feed_price(external_id, start_date, end_date, booked_date, dsn)
    #         return calculated_feed_price
    #
    #
    #     uvicorn.run(app, host="0.0.0.0", port=8900)
