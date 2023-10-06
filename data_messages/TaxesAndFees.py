import decimal
from dataclasses import dataclass
from decimal import Decimal
from glom import glom
from pydapper import connect
from data_messages import DateRange, LengthOfStay, FileInfo, DataHandlers


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
    extractType: str


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    taxes_and_fees, file_info = read_taxes_fees(file_args)
    return load_taxes_fees(taxes_and_fees, file_info, file_args.dsn)


def load_taxes_fees(taxes_and_fees: dict, file_info: FileInfo.FileInfo, db_name: str) -> FileInfo.FileInfo:
    if taxes_and_fees is None:
        return None

    taxes = taxes_and_fees.get('Tax', [])
    fees = taxes_and_fees.get('Fee', [])

    if len(taxes) == 0 and len(fees) == 0:
        return None

    new_id = FileInfo.load_file(file_info.file_name, db_name)
    taxes_inserted = insert_tax_fee_records(db_name, taxes, 'Tax', new_id)
    fees_inserted = insert_tax_fee_records(db_name, fees, 'Fee', new_id)
    file_info.records = len(taxes) + len(fees)
    return FileInfo.update_file(file_info, db_name)


def insert_tax_fee_records(db_name: str, records: list[TaxOrFee], table_prefix: str, file_id: int):
    rowcounts = {}
    with connect(db_name) as commands:
        commands.execute(f"""
            create table if not exists {table_prefix} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id varchar(20),
            calc_type varchar(30),
            basis varchar(30),
            period varchar(30),
            currency varchar(5),
            amount DECIMAL(18,6),
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE)
            """)
        commands.execute(f"""
            delete from {table_prefix}
            where file_id != ?file_id?
            """,
            param={"file_id": file_id})
        commands.execute(f"""
            create table if not exists {table_prefix}_BookingDates
            (external_id varchar(20),
            start TEXT,
            end TEXT,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE, 
            PRIMARY KEY(external_id, start, end, file_id))
            """)
        commands.execute(f"delete from {table_prefix}_BookingDates")
        commands.execute(f"""
            create table if not exists {table_prefix}_CheckinDates
            (external_id varchar(20),
            start TEXT,
            end TEXT,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            PRIMARY KEY(external_id, start, end, file_id))
            """)
        commands.execute(f"delete from {table_prefix}_CheckinDates")
        commands.execute(f"""
            create table if not exists {table_prefix}_CheckoutDates
            (external_id varchar(20),
            start TEXT,
            end TEXT,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            PRIMARY KEY(external_id, start, end, file_id))
            """)
        commands.execute(f"delete from {table_prefix}_CheckoutDates")
        commands.execute(f"""
            create table if not exists {table_prefix}_LengthOfStay
            (external_id varchar(20),
            min int,
            max int,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            PRIMARY KEY(external_id, file_id))
            """)
        commands.execute(f"delete from {table_prefix}_LengthOfStay")

        for record in records:
            rowcounts[table_prefix] = commands.execute(f"""
                INSERT INTO {table_prefix} (external_id, calc_type, basis, period, currency, amount, file_id)
                values (?external_id?, ?calc_type?, ?basis?, ?period?, ?currency?, ?amount?, ?file_id?)
                """,
                param={
                    "external_id": record.external_id,
                    "calc_type": record.type,
                    "basis": record.basis,
                    "period": record.period,
                    "currency": record.currency,
                    "amount": None if record.amount is None else float(record.amount),
                    "file_id": file_id
                })

            if len(record.booking_dates) > 0:
                rowcounts['bookingDates'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_BookingDates (external_id, start, end, file_id)
                        values (?external_id?, ?start?, ?end?, ?file_id?)
                        ON CONFLICT (external_id, start, end, file_id) DO NOTHING
                        """,
                    param=[{
                        "external_id": record.external_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat(),
                        "file_id": file_id,
                    } for date_range in record.booking_dates]),
            if len(record.checkin_dates) > 0:
                rowcounts['checkinDates'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_CheckinDates (external_id, start, end, file_id)
                        values (?external_id?, ?start?, ?end?, ?file_id?)
                        ON CONFLICT (external_id, start, end, file_id) DO NOTHING
                        """,
                    param=[{
                        "external_id": record.external_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat(),
                        "file_id": file_id,
                    } for date_range in record.checkin_dates]),
            if len(record.checkout_dates) > 0:
                rowcounts['checkoutDates'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_CheckoutDates (external_id, start, end, file_id)
                        values (?external_id?, ?start?, ?end?, ?file_id?)
                        ON CONFLICT (external_id, start, end, file_id) DO NOTHING
                        """,
                    param=[{
                        "external_id": record.external_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat(),
                        "file_id": file_id,
                    } for date_range in record.checkout_dates]),
            if record.length_of_stay is not None:
                rowcounts['lengthOfStay'] = commands.execute(
                    f"""
                        INSERT INTO {table_prefix}_LengthOfStay (external_id, min, max, file_id)
                        values (?external_id?, ?min?, ?max?, ?file_id?)
                        ON CONFLICT (external_id, file_id) DO NOTHING
                        """,
                    param={
                        "external_id": record.external_id,
                        "min": None if record.length_of_stay.min is None else record.length_of_stay.min,
                        "max": None if record.length_of_stay.max is None else record.length_of_stay.max,
                        "file_id": file_id
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
    for tax_or_fee in glom(file_input, spec).pop():
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
                                      extract_type))
    return taxes_or_fees
