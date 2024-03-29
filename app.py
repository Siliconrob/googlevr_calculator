import io
import os
import re
import zipfile
from dataclasses import dataclass
import stamina
from datetime import date
from typing import Annotated
from fastapi.middleware.cors import CORSMiddleware
import pendulum
from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from starlette.responses import FileResponse, Response, StreamingResponse, RedirectResponse
from DataStore import load_db, get_dsn, DB_NAME, clear_db, load_db_files, save_cache_item, get_cache_item, read_details
from price_calculator.ComputeFeed import compute_feed_price, FeedPrice
from icecream import ic
from middlewares.exceptionhandler import ExceptionHandlerMiddleware

ic.configureOutput(prefix='|> ')

tags_metadata = [
    {"name": "Calculator", "description": "For performing calculation"},
    {"name": "Maintenance", "description": "For application maintenance"},
    {"name": "Test", "description": "Testing availability/active"},
]

app = FastAPI(title="Google Vacation Rentals Calculator",
              description="Calculates a rental price based on ARI XML messages",
              version="0.0.3",
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

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(ExceptionHandlerMiddleware)


def iter_file():  #
    if not os.path.exists(DB_NAME):
        yield from ()
    else:
        with open(DB_NAME, mode="rb") as db_file:
            yield from db_file


@dataclass
class FeedFromZipArgs:
    upload_file: UploadFile
    external_id: str
    start_date: str
    end_date: str
    booked_date: str


@stamina.retry(on=Exception, attempts=3)
def get_zip_feed_price(feed_args: FeedFromZipArgs) -> FeedPrice:
    with zipfile.ZipFile(io.BytesIO(feed_args.upload_file.file.read()), "r") as messages_zip_file:
        dsn = get_dsn(DB_NAME)
        db_load_results = load_db(messages_zip_file, dsn)
    calculated_feed_price = compute_feed_price(feed_args.external_id,
                                               feed_args.start_date,
                                               feed_args.end_date,
                                               feed_args.booked_date,
                                               dsn)
    return calculated_feed_price


@app.post("/extract_details", tags=["Calculator"], include_in_schema=False)
async def extract_inventory(upload_file: UploadFile = File(...)):
    if upload_file.content_type not in ["application/zip", "application/octet-stream", "application/x-zip-compressed"]:
        raise HTTPException(400, detail="File must be a zip file")
    with zipfile.ZipFile(io.BytesIO(upload_file.file.read()), "r") as messages_zip_file:
        current_files = read_details(messages_zip_file)
    return current_files


@app.post("/feed", tags=["Calculator"])
async def feed_price_zip(upload_file: UploadFile = File(...),
                         external_id: str = 'orp12345x',
                         start_date: Annotated[date, "Start"] = pendulum.now().add(months=1).to_date_string(),
                         end_date: Annotated[date, "End"] = pendulum.now().add(months=1, weeks=1).to_date_string(),
                         booked_date: Annotated[date, "Booked"] = pendulum.now().to_date_string()):
    if upload_file.content_type not in ["application/zip", "application/octet-stream", "application/x-zip-compressed"]:
        raise HTTPException(400, detail="File must be a zip file")
    file_inputs = FeedFromZipArgs(upload_file, external_id, start_date, end_date, booked_date)
    return get_zip_feed_price(file_inputs)


@app.post("/feed_from_xml", tags=["Calculator"], include_in_schema=False)
async def feed_price_xml(upload_files: list[UploadFile] = list[File(...)],
                         external_id: str = 'orp12345x',
                         start_date: Annotated[date, "Start"] = pendulum.now().add(months=1).to_date_string(),
                         end_date: Annotated[date, "End"] = pendulum.now().add(months=1, weeks=1).to_date_string(),
                         booked_date: Annotated[date, "Booked"] = pendulum.now().to_date_string()):
    xml_files = {}
    for upload_file in upload_files:
        if upload_file.content_type not in ["application/xml", "application/octet-stream"]:
            raise HTTPException(400, detail=f"File {upload_file.filename} must be an XML file")
        xml_files[upload_file.filename] = upload_file.file.read()

    dsn = get_dsn(DB_NAME)
    db_load_results = load_db_files(xml_files, dsn)
    calculated_feed_price = compute_feed_price(external_id, start_date, end_date, booked_date, dsn)
    return calculated_feed_price


@app.get("/ping", tags=["Test"], include_in_schema=False)
async def ping():
    return ic(f'pong {pendulum.now().to_iso8601_string()}')


@app.delete("/reset", tags=["Maintenance"], include_in_schema=False)
async def reset():
    ic(clear_db(get_dsn(DB_NAME)))
    return None


@app.post("/save_property_file", tags=["Calculator"], include_in_schema=False)
async def save_property_file(upload_file: UploadFile = File(...), external_id: str = 'orp12345x'):
    if upload_file.content_type not in ["application/zip", "application/octet-stream", "application/x-zip-compressed"]:
        raise HTTPException(400, detail="File must be a zip file")
    data = upload_file.file.read()
    save_result = ic(save_cache_item(external_id, data, get_dsn(DB_NAME)))
    return save_result


@app.get("/get_property_file", tags=["Calculator"], include_in_schema=False)
async def get_property_file(external_id: str = 'orp12345x'):
    saved_item = ic(get_cache_item(external_id, get_dsn(DB_NAME)))
    if saved_item is None:
        return ic(None)

    timestamp = re.sub("[^0-9]", "", saved_item.timestamp)
    return Response(saved_item.contents,
                    headers={'Content-Disposition': f'attachment; filename="{external_id}_{timestamp}.zip"'})


@app.get("/datasource", tags=["Maintenance"], include_in_schema=False)
async def current_db():
    return StreamingResponse(content=iter_file(), media_type="application/octet")


@app.get("/", tags=["Maintenance"], include_in_schema=False)
async def to_docs():
    return RedirectResponse("/docs")
