import datetime
import decimal
from dataclasses import dataclass
from decimal import Decimal
from glom import glom
from pydapper import connect
from data_messages import DateRange, LengthOfStay, FileInfo


@dataclass
class TaxOrFee:
    externalId: str
    bookingDates: list[DateRange]
    checkinDates: list[DateRange]
    checkoutDates: list[DateRange]
    lengthOfStay: LengthOfStay
    type: str
    basis: str
    period: str
    currency: str
    amount: decimal
    extractType: str


def insert_records(file_input: dict, file_name: str, db_name: str) -> int:
    taxes_and_fees, file_info = read_taxes_fees(file_input)
    load_taxes_fees(taxes_and_fees, file_name, file_info, db_name)


def load_taxes_fees(taxes_and_fees: dict, file_name: str, file_info: FileInfo.FileInfo, db_name: str) -> int:
    if taxes_and_fees is None:
        return

    taxes = taxes_and_fees.get('Tax', [])
    fees = taxes_and_fees.get('Fee', [])

    if len(taxes) == 0 and len(fees) == 0:
        return

    file_info.name = file_name
    new_id = FileInfo.load_file(file_name, db_name)
    taxes_inserted = insert_tax_fee_records(db_name, taxes, 'Tax', new_id)
    fees_inserted = insert_tax_fee_records(db_name, fees, 'Fee', new_id)
    FileInfo.update_file(file_info, db_name)


def insert_tax_fee_records(db_name: str, records: list[TaxOrFee], table_prefix: str, file_id: int):
    rowcounts = {}
    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists {table_prefix} (id INTEGER PRIMARY KEY AUTOINCREMENT, externalId varchar(20), calcType varchar(30), basis varchar(30), period varchar(30), currency varchar(5), amount DECIMAL(18,6), parentId int, FOREIGN KEY (parentId) REFERENCES FileInfo(id) ON UPDATE CASCADE ON DELETE CASCADE)")
        commands.execute(f"delete from {table_prefix} where parentId != ?fileId?", param={"fileId": file_id})
        commands.execute(
            f"create table if not exists {table_prefix}_BookingDates (externalId varchar(20), start TEXT, end TEXT, parentId int, FOREIGN KEY (parentId) REFERENCES {table_prefix}(id) ON UPDATE CASCADE ON DELETE CASCADE, PRIMARY KEY(externalId, start, end, parentId))")
        commands.execute(f"delete from {table_prefix}_BookingDates")
        commands.execute(
            f"create table if not exists {table_prefix}_CheckinDates (externalId varchar(20), start TEXT, end TEXT, parentId int, FOREIGN KEY (parentId) REFERENCES {table_prefix}(id) ON UPDATE CASCADE ON DELETE CASCADE, PRIMARY KEY(externalId, start, end, parentId))")
        commands.execute(f"delete from {table_prefix}_CheckinDates")
        commands.execute(
            f"create table if not exists {table_prefix}_CheckoutDates (externalId varchar(20), start TEXT, end TEXT, parentId int, FOREIGN KEY (parentId) REFERENCES {table_prefix}(id) ON UPDATE CASCADE ON DELETE CASCADE, PRIMARY KEY(externalId, start, end, parentId))")
        commands.execute(f"delete from {table_prefix}_CheckoutDates")
        commands.execute(
            f"create table if not exists {table_prefix}_LengthOfStay (externalId varchar(20), min int, max int, parentId int, FOREIGN KEY (parentId) REFERENCES {table_prefix}(id) ON UPDATE CASCADE ON DELETE CASCADE, PRIMARY KEY(externalId, parentId))")
        commands.execute(f"delete from {table_prefix}_LengthOfStay")

        for record in records:
            rowcounts[table_prefix] = commands.execute(
                f"INSERT INTO {table_prefix} (externalId, calcType, basis, period, currency, amount, parentId) values (?externalId?, ?calcType?, ?basis?, ?period?, ?currency?, ?amount?, ?parentId?)",
                param={
                    "externalId": record.externalId,
                    "calcType": record.type,
                    "basis": record.basis,
                    "period": record.period,
                    "currency": record.currency,
                    "amount": None if record.amount is None else float(record.amount),
                    "parentId": file_id
                })
            last_id = commands.query_single(f"select seq from sqlite_sequence WHERE name = ?table_name?",
                                            param={"table_name": table_prefix})

            if len(record.bookingDates) > 0:
                rowcounts['bookingDates'] = commands.execute(
                    f"INSERT INTO {table_prefix}_BookingDates (externalId, start, end, parentId) values (?externalId?, ?start?, ?end?, ?parentId?) ON CONFLICT (externalId, start, end, parentId) DO NOTHING",
                    param=[{
                        "externalId": record.externalId,
                        "start": None if dateRange.start is None else dateRange.start.isoformat(),
                        "end": None if dateRange.end is None else dateRange.end.isoformat(),
                        "parentId": last_id['seq'],
                    } for dateRange in record.bookingDates]),
            if len(record.checkinDates) > 0:
                rowcounts['checkinDates'] = commands.execute(
                    f"INSERT INTO {table_prefix}_CheckinDates (externalId, start, end, parentId) values (?externalId?, ?start?, ?end?, ?parentId?) ON CONFLICT (externalId, start, end, parentId) DO NOTHING",
                    param=[{
                        "externalId": record.externalId,
                        "start": None if dateRange.start is None else dateRange.start.isoformat(),
                        "end": None if dateRange.end is None else dateRange.end.isoformat(),
                        "parentId": last_id['seq'],
                    } for dateRange in record.checkinDates]),
            if len(record.checkoutDates) > 0:
                rowcounts['checkoutDates'] = commands.execute(
                    f"INSERT INTO {table_prefix}_CheckoutDates (externalId, start, end, parentId) values (?externalId?, ?start?, ?end?, ?parentId?) ON CONFLICT (externalId, start, end, parentId) DO NOTHING",
                    param=[{
                        "externalId": record.externalId,
                        "start": None if dateRange.start is None else dateRange.start.isoformat(),
                        "end": None if dateRange.end is None else dateRange.end.isoformat(),
                        "parentId": last_id['seq'],
                    } for dateRange in record.checkoutDates]),
            if record.lengthOfStay is not None:
                rowcounts['lengthOfStay'] = commands.execute(
                    f"INSERT INTO {table_prefix}_LengthOfStay (externalId, min, max, parentId) values (?externalId?, ?min?, ?max?, ?parentId?) ON CONFLICT (externalId, parentId) DO NOTHING",
                    param={
                        "externalId": record.externalId,
                        "min": None if record.lengthOfStay.min is None else record.lengthOfStay.min,
                        "max": None if record.lengthOfStay.max is None else record.lengthOfStay.max,
                        "parentId": last_id['seq']
                    }),
    return rowcounts


def read_taxes_fees(file_input: dict) -> (dict, FileInfo.FileInfo):
    results = FileInfo.FileInfo()
    if len(file_input.keys()) > 1 or glom(file_input, 'TaxFeeInfo', default=None) is None:
        return {}, results

    results = FileInfo.FileInfo()
    results.timestamp = FileInfo.get_timestamp(glom(file_input, 'TaxFeeInfo.@timestamp'))
    results.externalId = glom(file_input, 'TaxFeeInfo.Property.ID')
    taxes_and_fees = {
        'Tax': read_tax_fee_data(results.externalId, file_input, '**.Tax', 'Tax'),
        'Fee': read_tax_fee_data(results.externalId, file_input, '**.Fee', 'Fee')
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
