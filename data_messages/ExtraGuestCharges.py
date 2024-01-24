import decimal
from dataclasses import dataclass
from decimal import Decimal

import xmltodict
from glom import glom
from pydapper import connect

from data_messages import DateRange
from data_messages import FileInfo
from fileset import DataHandlers
from data_messages.LastId import LastId
from fileset.DataHandlers import get_safe_list


@dataclass
class ExtraGuestCharges:
    external_id: str
    stay_dates: list[DateRange]
    adult_charges: decimal
    xml_contents: str


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    extra_charges, file_info = read_extra_charges(file_args)
    return load_extra_charges(extra_charges, file_info, file_args)


def create_tables(dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
        create table if not exists ExtraGuestCharges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id varchar(20),
            adult_charges DECIMAL(18,6),
            xml_contents TEXT,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE)
            """)
        commands.execute(f"""
            create table if not exists ExtraGuestCharges_StayDates
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id varchar(20),
                start TEXT,
                end TEXT,
                days_of_week TEXT,              
                file_id int,
                parent_id int,            
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES ExtraGuestCharges(id) ON DELETE CASCADE
            )
            """)


def load_extra_charges(extra_guest_charges: ExtraGuestCharges,
                       file_info: FileInfo.FileInfo,
                       file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if extra_guest_charges is None:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    rowcounts = {}
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            delete from ExtraGuestCharges
            where file_id != ?file_id?
            """,
                         param={"file_id": new_id})
        commands.execute(f"""
            delete from ExtraGuestCharges_StayDates
            where file_id != ?file_id?
            """,
                         param={"file_id": new_id})
        rowcounts["ExtraGuestCharges"] = commands.execute(f"""
            INSERT INTO ExtraGuestCharges
            (
                external_id,
                adult_charges,
                xml_contents,
                file_id
            )
            values
            (
                ?external_id?,
                ?adult_charges?,
                ?xml_contents?,
                ?file_id?
            )
            """,
                                                          param={
                                                              "external_id": extra_guest_charges.external_id,
                                                              "adult_charges": None if extra_guest_charges.adult_charges is None else float(
                                                                  extra_guest_charges.adult_charges),
                                                              "xml_contents": extra_guest_charges.xml_contents,
                                                              "file_id": new_id
                                                          })
        last_id = commands.query_first_or_default(f"""
            select seq
            from sqlite_sequence
            WHERE name = ?table_name?
            """,
                                                  param={"table_name": "ExtraGuestCharges"}, model=LastId,
                                                  default=LastId())
        rowcounts['stay_dates'] = commands.execute(
            f"""
                INSERT INTO ExtraGuestCharges_StayDates (external_id, start, end, days_of_week, file_id, parent_id)
                values (?external_id?, ?start?, ?end?, ?days_of_week?, ?file_id?, ?parent_id?)
                ON CONFLICT DO NOTHING
                """,
            param=[{
                "external_id": extra_guest_charges.external_id,
                "start": None if date_range.start is None else date_range.start.isoformat(),
                "end": None if date_range.end is None else date_range.end.isoformat(),
                "days_of_week": date_range.days_of_week,
                "file_id": new_id,
                "parent_id": last_id.seq
            } for date_range in extra_guest_charges.stay_dates]),

    file_info.records = 1
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_extra_charges(file_args: DataHandlers.DataFileArgs) -> (ExtraGuestCharges, FileInfo.FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'ExtraGuestCharges', default=None) is None):
        return None, None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data, 'ExtraGuestCharges.@timestamp'))
    results.external_id = get_safe_list(glom(file_args.formatted_data, '**.@hotel_id'))
    extra_guest_charges = glom(file_args.formatted_data, '**.ExtraGuestCharge')
    if len(extra_guest_charges) == 0:
        return None, None
    extra_guest_charges = extra_guest_charges.pop()
    extra_adult_charge = glom(extra_guest_charges, 'AgeBrackets.AdultCharge', default=None)
    if extra_adult_charge is None:
        None, None
    charge_amount = Decimal(extra_adult_charge["@amount"])
    stay_dates = DateRange.parse_ranges(glom(extra_guest_charges, 'StayDates.DateRange', default=[]))
    charges_dict = glom(file_args.formatted_data, 'ExtraGuestCharges.HotelExtraGuestCharges')
    extras = ExtraGuestCharges(results.external_id,
                               stay_dates,
                               charge_amount,
                               xmltodict.unparse({'HotelExtraGuestCharges': charges_dict}))

    return extras, results
