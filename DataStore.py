import os
from pathlib import Path
from pyexpat import ExpatError
import xmltodict
import data_messages.OTA_HotelRateAmountNotifRQ
from data_messages import DataHandlers
from data_messages.ExtraGuestCharges import ExtraGuestCharges
from data_messages.OTA_HotelAvailNotifRQ import OTAHotelAvailNotifRQ
from data_messages.Promotions import Promotion
from data_messages.RateModifications import RateModifications
from data_messages.TaxesAndFees import TaxOrFee
from data_messages.PropertyData import PropertyData


def get_dsn(db_name: str) -> str:
    return f'sqlite+sqlite3://{db_name}'


def read_folder(target_folder: str, dsn: str) -> set:
    files = [current_file for current_file in os.listdir(target_folder) if
             os.path.isfile(os.path.join(target_folder, current_file))]

    return read_files(dsn, files, target_folder)


def read_files(dsn: str, files: list[str], target_folder: str) -> set:
    results = {}
    for file in files:
        data, contents = read_file_contents(file, target_folder)
        if data is None:
            continue
        args = DataHandlers.DataFileArgs(data,
                                         file,
                                         dsn,
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
    record_counts["property_data"] = data_messages.PropertyData.insert_records(file_args)
    return record_counts


def load_db(xml_files_path: str, dsn: str) -> set:
    if xml_files_path is None:
        return None
    return read_folder(xml_files_path, dsn)


DB_NAME = "googlevr.db"
