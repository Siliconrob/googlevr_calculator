import decimal
from dataclasses import dataclass
from datetime import datetime
from glom import glom

from data_messages import FileInfo, DataHandlers


@dataclass
class PropertyData:
    external_id: str
    start: datetime.date
    end: datetime.date
    base_amount: decimal
    guest_count: int


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    property_data, file_info = read_property_data(file_args)
    return load_rates(property_data, file_info, file_args)


def create_tables(dsn: str):
    pass


def load_rates(property_data: PropertyData,
               file_info: FileInfo.FileInfo,
               file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if property_data is None:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    file_info.records = 1
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_property_data(file_args: DataHandlers.DataFileArgs) -> (PropertyData, FileInfo.FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'Transaction', default=None) is None):
        return None, None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data,'Transaction.@timestamp'))
    results.external_id = glom(file_args.formatted_data, 'Transaction.PropertyDataSet.Property')
    property_data_sets = None
    if property_data_sets is None:
        return None, None
    return property_data_sets, results
