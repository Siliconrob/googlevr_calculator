import asyncio

import pendulum
import uvicorn
from fastapi import FastAPI
from pathlib import Path
import xmltodict
import os
from pyexpat import ExpatError
import data_messages.OTA_HotelRateAmountNotifRQ
from data_messages import DataHandlers
from data_messages.TaxesAndFees import TaxOrFee
from data_messages.Promotions import Promotion
from data_messages.RateModifications import RateModifications
from data_messages.OTA_HotelAvailNotifRQ import OTAHotelAvailNotifRQ
from data_messages.ExtraGuestCharges import ExtraGuestCharges
import argparse

from price_calculator.ComputeFeed import compute_feed_price


def get_dsn(db_name: str) -> str:
    return f'sqlite+sqlite3://{db_name}'


def read_folder(target_folder: str, db_name: str) -> set:
    files = [current_file for current_file in os.listdir(target_folder) if
             os.path.isfile(os.path.join(target_folder, current_file))]

    return read_files(db_name, files, target_folder)


def read_files(db_name, files, target_folder) -> set:
    results = {}
    for file in files:
        data, contents = read_file_contents(file, target_folder)
        if data is None:
            continue
        args = DataHandlers.DataFileArgs(data,
                                         file,
                                         get_dsn(db_name),
                                         contents)
        record_counts = read_file_into_db(args)
        results = set(results) & set(record_counts)
    return record_counts


def read_file_contents(input_file: str, input_folder: str) -> (dict, str):
    try:
        current_file = Path(os.path.join(input_folder, input_file))
        file_contents = current_file.read_text()
        return xmltodict.parse(file_contents), file_contents
    except UnicodeDecodeError:
        print(f'Unable to read {input_file} as text')
    except ExpatError:
        print(f'Unable to parse {input_file} into XML')
    return None


def read_file_into_db(file_args: DataHandlers.DataFileArgs) -> dict:
    record_counts = {}
    if file_args is None:
        return record_counts
    record_counts["rates"] = data_messages.OTA_HotelRateAmountNotifRQ.insert_records(file_args)
    record_counts["rate_modifications"] = data_messages.RateModifications.insert_records(file_args)
    record_counts["hotel_availability"] = data_messages.OTA_HotelAvailNotifRQ.insert_records(file_args)
    record_counts["taxes_and_fees"] = data_messages.TaxesAndFees.insert_records(file_args)
    record_counts["promotions"] = data_messages.Promotions.insert_records(file_args)
    record_counts["extra_guest_charges"] = data_messages.ExtraGuestCharges.insert_records(file_args)
    return record_counts

DB_NAME = "googlevr.db"


def load_db(xml_files_path: str, db_name: str) -> set:
    if xml_files_path is None:
        return None
    return read_folder(xml_files_path, db_name)


app = FastAPI()


@app.get("/feed_price")
async def feed_price(external_id: str, start_date_text: str, end_date_text: str, book_date_text: str):
    start_date = pendulum.parse(start_date_text)
    end_date = pendulum.parse(end_date_text)
    booked_date = pendulum.parse(book_date_text)
    calculated_feed_price = compute_feed_price(external_id, start_date, end_date, booked_date, get_dsn(DB_NAME))
    return calculated_feed_price

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", action="store", default="c:/test/gvr_inputs")
parser.add_argument("--save_db", action="store_true", default=True)
parser.add_argument("--start", action="store", default="")
parser.add_argument("--end", action="store", default="")
parser.add_argument("--book_date", action="store", default="")
parser.add_argument("--external_id", action="store", default="")
parser.add_argument("--web_ui", action="store_true", default=False)

if __name__ == "__main__":
    args = parser.parse_args()
    start = args.start if len(args.start) > 0 else "2024-02-21"
    end = args.end if len(args.end) > 0 else "2024-03-01"
    external_id = args.external_id if len(args.external_id) > 0 else "orp5b45c10x"
    book_date = args.book_date if len(args.book_date) > 0 else "2023-10-06"
    # results = load_db(args.input_path, db_name)

    if args.web_ui:
        uvicorn.run(app, host="0.0.0.0", port=8900)
    else:
        calculated_feed_price = compute_feed_price(external_id, start, end, book_date, get_dsn(DB_NAME))
