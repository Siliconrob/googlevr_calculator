from pathlib import Path
import xmltodict
import os
from pyexpat import ExpatError
import pendulum
from decimal import Decimal

from glom import glom
from pydapper import connect, connect_async

import data_messages.OTA_HotelRateAmountNotifRQ
from data_messages.OTA_HotelRateAmountNotifRQ import OTAHotelRateAmountNotifRQ
from data_messages.RateModifications import RateModifications
from data_messages.OTA_HotelAvailNotifRQ import OTAHotelAvailNotifRQ


import sqlite3
import asyncio




def read_ratemodifications(file_input: dict) -> list[RateModifications]:
    root_key = RateModifications.data_type_key()
    if root_key not in file_input or len(file_input.keys()) > 1:
        return None

    messages_key = RateModifications.rate_amount_messages_key()
    hotel_key = RateModifications.hotel_id_key()
    message_key = RateModifications.rate_amount_message_key()
    externalId = file_input[root_key][messages_key][hotel_key]
    rates = []
    for rate_amount_message in file_input[root_key][messages_key][message_key]:
        base_by_amount = rate_amount_message["Rates"]["Rate"]["BaseByGuestAmts"]["BaseByGuestAmt"]
        durations = rate_amount_message["StatusApplicationControl"]
        start, end = pendulum.parse(durations["@Start"]), pendulum.parse(durations["@End"])
        new_rate = RateModifications(externalId, start, end, Decimal(base_by_amount["@AmountBeforeTax"]),
                                             int(base_by_amount["@NumberOfGuests"]))
        rates.append(new_rate)

    return rates

def get_dsn(db_name: str) -> str:
    return f'sqlite+sqlite3://{db_name}'






def load_ratemodifications(rates: list[RateModifications], db_name: str) -> int:
    insert_rates = []
    for rate in rates:
        insert_rates.append({
            "internalId": rate.internalId,
            "externalId": rate.externalId,
            "start": rate.start.isoformat(),
            "end": rate.end.isoformat(),
            "baseAmount": float(rate.baseAmount),
            "guestCount": rate.guestCount
        })

    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists {OTAHotelRateAmountNotifRQ.__name__} (internalId INT, externalId varchar(20), start TEXT, end TEXT, baseAmount DECIMAL(18,6), guestCount int, PRIMARY KEY(externalId, start, end))")
        rowcount = commands.execute(
            f"INSERT INTO {OTAHotelRateAmountNotifRQ.__name__} (internalId, externalId, start, end, baseAmount, guestCount) values (?internalId?, ?externalId?, ?start?, ?end?, ?baseAmount?, ?guestCount?) ON CONFLICT (internalId, start, end) DO UPDATE SET baseAmount = ?baseAmount?, guestCount = ?guestCount?",
            param=insert_rates,
        )
        return rowcount


def read_folder(target_folder: str, db_name: str) -> dict:
    files = [current_file for current_file in os.listdir(target_folder) if os.path.isfile(os.path.join(target_folder, current_file))]
    record_counts = {}

    dsn = get_dsn(db_name)
    for file in files:
        try:
            formatted_data = xmltodict.parse(Path(os.path.join(target_folder, file)).read_text())
            #record_counts["rates"] = data_messages.OTA_HotelRateAmountNotifRQ.insert_records(formatted_data, dsn)
            #record_counts["ratemodifications"] = data_messages.RateModifications.insert_records(formatted_data, dsn)
            record_counts["ratemodifications"] = data_messages.OTA_HotelAvailNotifRQ.insert_records(formatted_data, dsn)
            # record_counts["rates"] = load_rates(read_rates(xmltodict.parse(file_contents)), db_name)
            # record_counts["rates"] = load_rates(read_rates(xmltodict.parse(file_contents)), db_name)
        except UnicodeDecodeError:
            print(f'Unable to read {file} as text')
        except ExpatError:
            print(f'Unable to parse {file} into XML')
    return record_counts



# Press the green button in the gutter to run the script.
# def create_db(db_name: str) -> str:
#     return db_name
#
#     with sqlite3.connect(db_name) as conn:
#         conn.execute(
#             f"create table if not exists {OTAHotelRateAmountNotifRQ.__name__} (internalId INT, externalId varchar(20), start TEXT, end TEXT, baseAmount DECIMAL(18,6), guestCount int)")
#     return db_name


# async def main():
#     await read_folder_async('c:/test/gvr_inputs', get_dsn('googlevr.db'))

if __name__ == "__main__":
    #asyncio.run(main())
    results = read_folder('c:/test/gvr_inputs', 'googlevr.db')
    print(results)

