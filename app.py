import io
import os
import zipfile
from datetime import date
from typing import Annotated

import pendulum
from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from starlette.responses import FileResponse, Response, StreamingResponse
from DataStore import load_db, get_dsn, DB_NAME, clear_db
from price_calculator.ComputeFeed import compute_feed_price

tags_metadata = [
    {"name": "Calculator", "description": "For performing calculation"},
    {"name": "Maintenance", "description": "For application maintenance"},
    {"name": "Test", "description": "Testing availability/active"},
]

app = FastAPI(title="Google Vacation Rentals Calculator",
              description="Calculates a rental price based on ARI XML messages",
              version="0.0.1",
              terms_of_service="Strength is irrelevant. Resistance is futile. We wish to improve ourselves. We will add your biological and technological distinctiveness to our own.",
              contact={
                  "url": "https://siliconheaven.info",
                  "email": "siliconrob@siliconheaven.net",
              },
              openapi_tags=tags_metadata,
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


@app.post("/feed", tags=["Calculator"])
async def feed_price(upload_file: UploadFile = File(...),
                     external_id: str = 'orp12345x',
                     start_date: Annotated[date, "Start"] = pendulum.now().add(months=1).to_date_string(),
                     end_date: Annotated[date, "End"] = pendulum.now().add(months=1, weeks=1).to_date_string(),
                     booked_date: Annotated[date, "Booked"] = pendulum.now().to_date_string()):
    if upload_file.content_type not in ["application/zip", "application/octet-stream", "application/x-zip-compressed"]:
        raise HTTPException(400, detail="File must be a zip file")

    with zipfile.ZipFile(io.BytesIO(upload_file.file.read()), "r") as messages_zip_file:
        dsn = get_dsn(DB_NAME)
        db_load_results = load_db(messages_zip_file, dsn)
    calculated_feed_price = compute_feed_price(external_id, start_date, end_date, booked_date, dsn)
    return calculated_feed_price


@app.get("/ping", tags=["Test"], include_in_schema=False)
async def ping():
    return f'pong {pendulum.now().to_iso8601_string()}'


@app.delete("/reset", tags=["Maintenance"], include_in_schema=False)
async def reset():
    clear_db(get_dsn(DB_NAME))


@app.get("/datasource", tags=["Maintenance"], include_in_schema=False)
async def current_db():
    return StreamingResponse(content=iter_file(), media_type="application/octet")
