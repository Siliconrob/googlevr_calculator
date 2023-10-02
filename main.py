from pathlib import Path
import xmltodict
import os
from pyexpat import ExpatError
import data_messages.OTA_HotelRateAmountNotifRQ
from data_messages.Promotions import Promotion
from data_messages.OTA_HotelRateAmountNotifRQ import OTAHotelRateAmountNotifRQ
from data_messages.RateModifications import RateModifications
from data_messages.OTA_HotelAvailNotifRQ import OTAHotelAvailNotifRQ


def get_dsn(db_name: str) -> str:
    return f'sqlite+sqlite3://{db_name}'


def read_folder(target_folder: str, db_name: str) -> dict:
    files = [current_file for current_file in os.listdir(target_folder) if
             os.path.isfile(os.path.join(target_folder, current_file))]
    record_counts = {}

    dsn = get_dsn(db_name)
    for file in files:
        try:
            formatted_data = xmltodict.parse(Path(os.path.join(target_folder, file)).read_text())
            # record_counts["rates"] = data_messages.OTA_HotelRateAmountNotifRQ.insert_records(formatted_data, dsn)
            # record_counts["ratemodifications"] = data_messages.RateModifications.insert_records(formatted_data, dsn)
            # record_counts["ratemodifications"] = data_messages.OTA_HotelAvailNotifRQ.insert_records(formatted_data, dsn)
            # record_counts["taxesandfees"] = data_messages.TaxesAndFees.insert_records(formatted_data, dsn)
            record_counts["promotions"] = data_messages.Promotions.insert_records(formatted_data, dsn)
            # record_counts["extraguestcharges"] = data_messages.ExtraGuestCharges.insert_records(formatted_data, dsn)
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
    # asyncio.run(main())
    results = read_folder('c:/test/gvr_inputs', 'googlevr.db')
    print(results)
