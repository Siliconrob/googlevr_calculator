import io
import os
import zipfile

import pendulum
from fastapi import FastAPI, UploadFile, HTTPException
from starlette.responses import FileResponse, Response, StreamingResponse
from DataStore import load_db, get_dsn, DB_NAME
from price_calculator.ComputeFeed import compute_feed_price

app = FastAPI(title="Google Vacation Rentals Calculator",
              description="Calculates a rental price based on ARI XML messages",
              version="0.0.1",
              terms_of_service="Strength is irrelevant. Resistance is futile. We wish to improve ourselves. We will add your biological and technological distinctiveness to our own.",
              contact={
                  "url": "https://siliconheaven.info",
                  "email": "siliconrob@siliconheaven.net",
              },
              license_info={
                  "name": "MIT License",
                  "url": "https://opensource.org/license/mit/",
              })


def iter_file():  #
    if not os.path.exists(DB_NAME):
        yield from ()
    else:
        with open(DB_NAME, mode="rb") as db_file:
            yield from db_file


@app.get("/echo")
async def echo(response: str):
    return {"message": f'Sent {response}'}


@app.get("/ping")
async def ping():
    return f'pong {pendulum.now().to_iso8601_string()}'


@app.get("/datasource")
async def current_db():
    return StreamingResponse(content=iter_file(), media_type="application/octet")


@app.post("/feed")
async def feed_price(upload_file: UploadFile, external_id: str, start_date: str, end_date: str, booked_date: str):
    with zipfile.ZipFile(io.BytesIO(upload_file.file.read()), "r") as messages_zip_file:
        dsn = get_dsn(DB_NAME)
        db_load_results = load_db(messages_zip_file, dsn)
    calculated_feed_price = compute_feed_price(external_id, start_date, end_date, booked_date, dsn)
    return calculated_feed_price
