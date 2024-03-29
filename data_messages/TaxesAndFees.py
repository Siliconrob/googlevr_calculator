import decimal
from dataclasses import dataclass
from decimal import Decimal

import xmltodict
from glom import glom
from pydapper import connect

from data_messages import DateRange, LengthOfStay, FileInfo
from fileset import DataHandlers
from fileset.DataHandlers import get_safe_list
from data_messages.LastId import LastId


@dataclass
class TaxOrFee:
    external_id: str
    booking_dates: list[DateRange]
    checkin_dates: list[DateRange]
    checkout_dates: list[DateRange]
    length_of_stay: LengthOfStay
    type: str
    basis: str
    period: str
    currency: str
    amount: decimal
    extract_type: str
    xml_contents: str


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables("Tax", file_args.dsn)
    create_tables("Fee", file_args.dsn)
    taxes_and_fees, file_info = read_taxes_fees(file_args)
    return load_taxes_fees(taxes_and_fees, file_info, file_args)


def load_taxes_fees(taxes_and_fees: dict, file_info: FileInfo.FileInfo,
                    file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if taxes_and_fees is None:
        return None

    taxes = taxes_and_fees.get('Tax', [])
    fees = taxes_and_fees.get('Fee', [])

    if len(taxes) == 0 and len(fees) == 0:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    taxes_inserted = insert_tax_fee_records(file_args.dsn, taxes, 'Tax', new_id)
    fees_inserted = insert_tax_fee_records(file_args.dsn, fees, 'Fee', new_id)
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def create_tables(table_prefix: str, dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
            create table if not exists {table_prefix} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id varchar(20),
            calc_type varchar(30),
            basis varchar(30),
            period varchar(30),
            currency varchar(5),
            amount DECIMAL(18,6),
            xml_contents TEXT,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE)
            """)
        commands.execute(f"""
            create table if not exists {table_prefix}_BookingDates
            (external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,            
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES {table_prefix}(id) ON DELETE CASCADE,
            PRIMARY KEY(external_id, start, end, days_of_week, file_id, parent_id))
            """)
        commands.execute(f"""
            create table if not exists {table_prefix}_CheckinDates
            (external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,            
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES {table_prefix}(id) ON DELETE CASCADE,            
            PRIMARY KEY(external_id, start, end, days_of_week, file_id, parent_id))
            """)
        commands.execute(f"""
            create table if not exists {table_prefix}_CheckoutDates
            (external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,            
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES {table_prefix}(id) ON DELETE CASCADE,             
            PRIMARY KEY(external_id, start, end, days_of_week, file_id, parent_id))
            """)
        commands.execute(f"""
            create table if not exists {table_prefix}_LengthOfStay
            (external_id varchar(20),
            min int,
            max int,
            file_id int,
            parent_id int,            
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES {table_prefix}(id) ON DELETE CASCADE,              
            PRIMARY KEY(external_id, file_id, parent_id))
            """)


def insert_tax_fee_records(db_name: str, records: list[TaxOrFee], table_prefix: str, file_id: int):
    rowcounts = {}
    with connect(db_name) as commands:
        commands.execute(f"""
            delete from {table_prefix}
            where file_id != ?file_id?
            """,
                         param={"file_id": file_id})
        for record in records:
            rowcounts[table_prefix] = commands.execute(f"""
                INSERT INTO {table_prefix}
                (
                    external_id,
                    calc_type,
                    basis,
                    period,
                    currency,
                    amount,
                    xml_contents,
                    file_id
                )
                values
                (
                    ?external_id?,
                    ?calc_type?,
                    ?basis?,
                    ?period?,
                    ?currency?,
                    ?amount?,
                    ?xml_contents?,
                    ?file_id?
                )
                """,
                                                       param={
                                                           "external_id": record.external_id,
                                                           "calc_type": record.type,
                                                           "basis": record.basis,
                                                           "period": record.period,
                                                           "currency": record.currency,
                                                           "amount": None if record.amount is None else float(
                                                               record.amount),
                                                           "xml_contents": record.xml_contents,
                                                           "file_id": file_id
                                                       })

            last_id = commands.query_first_or_default(f"""
                select seq
                from sqlite_sequence
                WHERE name = ?table_name?
                """,
                                                      param={"table_name": table_prefix}, model=LastId,
                                                      default=LastId())

            if len(record.booking_dates) > 0:
                rowcounts['bookingDates'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_BookingDates (external_id, start, end, days_of_week, file_id, parent_id)
                        values (?external_id?, ?start?, ?end?, ?days_of_week?, ?file_id?, ?parent_id?)
                        ON CONFLICT (external_id, start, end, days_of_week, file_id, parent_id) DO NOTHING
                        """,
                    param=[{
                        "external_id": record.external_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat(),
                        "days_of_week": date_range.days_of_week,
                        "file_id": file_id,
                        "parent_id": last_id.seq
                    } for date_range in record.booking_dates]),
            if len(record.checkin_dates) > 0:
                rowcounts['checkinDates'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_CheckinDates (external_id, start, end, days_of_week, file_id, parent_id)
                        values (?external_id?, ?start?, ?end?, ?days_of_week?, ?file_id?, ?parent_id?)
                        ON CONFLICT (external_id, start, end, days_of_week, file_id, parent_id) DO NOTHING
                        """,
                    param=[{
                        "external_id": record.external_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat(),
                        "days_of_week": date_range.days_of_week,
                        "file_id": file_id,
                        "parent_id": last_id.seq
                    } for date_range in record.checkin_dates]),
            if len(record.checkout_dates) > 0:
                rowcounts['checkoutDates'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_CheckoutDates (external_id, start, end, days_of_week, file_id, parent_id)
                        values (?external_id?, ?start?, ?end?, ?days_of_week?, ?file_id?, ?parent_id?)
                        ON CONFLICT (external_id, start, end, days_of_week, file_id, parent_id) DO NOTHING
                        """,
                    param=[{
                        "external_id": record.external_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat(),
                        "days_of_week": date_range.days_of_week,
                        "file_id": file_id,
                        "parent_id": last_id.seq
                    } for date_range in record.checkout_dates]),
            if record.length_of_stay is not None:
                rowcounts['lengthOfStay'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_LengthOfStay (external_id, min, max, file_id, parent_id)
                        values (?external_id?, ?min?, ?max?, ?file_id?, ?parent_id?)
                        ON CONFLICT (external_id, file_id, parent_id) DO NOTHING
                        """,
                    param={
                        "external_id": record.external_id,
                        "min": None if record.length_of_stay.min is None else record.length_of_stay.min,
                        "max": None if record.length_of_stay.max is None else record.length_of_stay.max,
                        "file_id": file_id,
                        "parent_id": last_id.seq
                    }),
    return rowcounts


def read_taxes_fees(file_args: DataHandlers.DataFileArgs) -> (dict, FileInfo.FileInfo):
    results = FileInfo.FileInfo()
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'TaxFeeInfo', default=None) is None):
        return {}, results

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data, 'TaxFeeInfo.@timestamp'))
    results.external_id = glom(file_args.formatted_data, 'TaxFeeInfo.Property.ID')
    taxes_and_fees = {
        'Tax': read_tax_fee_data(results.external_id, file_args.formatted_data, '**.Tax', 'Tax'),
        'Fee': read_tax_fee_data(results.external_id, file_args.formatted_data, '**.Fee', 'Fee')
    }
    results.records = len(taxes_and_fees['Tax']) + len(taxes_and_fees['Fee'])
    return taxes_and_fees, results


def read_tax_fee_data(external_id, file_input, spec, extract_type) -> list[TaxOrFee]:
    taxes_or_fees = []

    taxes_or_fees_promotions = glom(file_input, spec)
    if len(taxes_or_fees_promotions) == 0:
        return []

    for tax_or_fee in get_safe_list(taxes_or_fees_promotions):
        extracted_amount = glom(tax_or_fee, 'Amount', default=None)
        taxes_or_fees.append(TaxOrFee(external_id,
                                      DateRange.parse_ranges(glom(tax_or_fee, 'BookingDates.DateRange', default=[])),
                                      DateRange.parse_ranges(glom(tax_or_fee, 'CheckinDates.DateRange', default=[])),
                                      DateRange.parse_ranges(glom(tax_or_fee, 'CheckoutDates.DateRange', default=[])),
                                      LengthOfStay.parse_range(glom(tax_or_fee, 'LengthOfStay', default=None)),
                                      glom(tax_or_fee, 'Type', default=None),
                                      glom(tax_or_fee, 'Basis', default=None),
                                      glom(tax_or_fee, 'Period', default=None),
                                      glom(tax_or_fee, 'Currency', default=None),
                                      None if extracted_amount is None else Decimal(extracted_amount),
                                      extract_type,
                                      xmltodict.unparse({extract_type: tax_or_fee})))
    return taxes_or_fees
