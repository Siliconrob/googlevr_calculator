import decimal
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import pendulum
from glom import glom
from pydapper import connect
from data_messages import FileInfo, DataHandlers


@dataclass
class OTAHotelRateAmountNotifRQ:
    external_id: str
    start: datetime.date
    end: datetime.date
    base_amount: decimal
    guest_count: int


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    rates, file_info = read_rates(file_args)
    return load_rates(rates, file_info, file_args)


def create_tables(dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
            create table if not exists OTAHotelRateAmountNotifRQ
            (
                external_id varchar(20),
                start TEXT,
                end TEXT,
                base_amount DECIMAL(18,6),
                guest_count int,
                file_id int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                PRIMARY KEY(external_id, file_id, start, end)
            )""")


def load_rates(rates: list[OTAHotelRateAmountNotifRQ],
               file_info: FileInfo.FileInfo,
               file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if len(rates) == 0:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    rowcount = {}
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            delete from OTAHotelRateAmountNotifRQ
            where file_id != ?file_id?
            """,
            param={"file_id": new_id})
        rowcount["rates"] = commands.execute(f"""
            INSERT INTO OTAHotelRateAmountNotifRQ
            (
                external_id,
                file_id,
                start,
                end,
                base_amount,
                guest_count
            )
            values
            (
                ?external_id?,
                ?file_id?,
                ?start?,
                ?end?,
                ?base_amount?, 
                ?guest_count?
            )
            ON CONFLICT (external_id, file_id, start, end)
            DO UPDATE SET base_amount = ?base_amount?,
                guest_count = ?guest_count?
            """,
            param=[{
                "external_id": rate.external_id,
                "file_id": new_id,
                "start": rate.start.isoformat(),
                "end": rate.end.isoformat(),
                "base_amount": float(rate.base_amount),
                "guest_count": rate.guest_count
            } for rate in rates],
        )
    file_info.records = len(rates)
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_rates(file_args: DataHandlers.DataFileArgs) -> (list[OTAHotelRateAmountNotifRQ], FileInfo.FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'OTA_HotelRateAmountNotifRQ', default=None) is None):
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data,'OTA_HotelRateAmountNotifRQ.@TimeStamp'))
    results.external_id = glom(file_args.formatted_data, 'OTA_HotelRateAmountNotifRQ.RateAmountMessages.@HotelCode')
    rates = []
    file_rates = glom(file_args.formatted_data, '**.RateAmountMessage')
    if len(file_rates) == 0:
        return [], None
    for rate_amount_message in file_rates.pop():
        base_by_amount = glom(rate_amount_message, 'Rates.Rate.BaseByGuestAmts.BaseByGuestAmt', default=None)
        status_application_control = glom(rate_amount_message, 'StatusApplicationControl', default=None)
        if base_by_amount is None or status_application_control is None:
            continue
        start, end = pendulum.parse(status_application_control["@Start"]), pendulum.parse(
            status_application_control["@End"])
        new_rate = OTAHotelRateAmountNotifRQ(results.external_id,
                                             start,
                                             end,
                                             Decimal(base_by_amount["@AmountBeforeTax"]),
                                             int(base_by_amount["@NumberOfGuests"]))
        rates.append(new_rate)
    return rates, results
