from dataclasses import dataclass
from datetime import datetime
import pendulum

from data_messages.FileInfo import FileInfo


@dataclass
class DataFileArgs:
    formatted_data: dict
    file_name: str
    dsn: str
    created: datetime = pendulum.UTC


@dataclass
class RecordCounts:
    rates: FileInfo = None
    rate_modifications: FileInfo = None
    hotel_availability: FileInfo = None
    taxes_and_fees: FileInfo = None
    promotions: FileInfo = None
    extra_guest_charges: FileInfo = None
    created: datetime = pendulum.UTC
