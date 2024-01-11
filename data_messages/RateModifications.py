import decimal
from dataclasses import dataclass
from decimal import Decimal

import xmltodict
from glom import glom
from pydapper import connect

from data_messages import DateRange, LengthOfStay, BookingWindow
from data_messages import FileInfo
from fileset import DataHandlers
from data_messages.BookingWindow import BookingWindowInt
from fileset.DataHandlers import get_safe_list
from data_messages.LastId import LastId


@dataclass
class RateModifications:
    external_id: str
    booking_dates: list[DateRange]
    checkin_dates: list[DateRange]
    checkout_dates: list[DateRange]
    price_adjustment: decimal
    length_of_stay: LengthOfStay
    booking_window: BookingWindowInt
    xml_contents: str


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    rate_modifiers, file_info = read_rate_modifications(file_args)
    return load_rate_modifications(rate_modifiers, file_info, file_args)


def create_tables(dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
            create table if not exists RateModifications
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id varchar(20),
                multiplier DECIMAL(18,6),
                xml_contents TEXT,
                file_id int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,                
                UNIQUE(external_id, file_id, multiplier)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_BookingDates
            (
                external_id varchar(20),
                file_id int,
                parent_id int,
                start TEXT,
                end TEXT,
                days_of_week TEXT,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES RateModifications(id) ON DELETE CASCADE,                                 
                UNIQUE(external_id, file_id, start, end, days_of_week)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_CheckinDates
            (
                external_id varchar(20),
                file_id int,
                parent_id int,
                start TEXT,
                end TEXT,
                days_of_week TEXT,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES RateModifications(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, start, end, days_of_week)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_CheckoutDates
            (
                external_id varchar(20),
                file_id int,
                parent_id int,
                start TEXT,
                end TEXT,
                days_of_week TEXT,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES RateModifications(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, start, end, days_of_week)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_LengthOfStay
            (
                external_id varchar(20),
                file_id int,
                parent_id int,
                min int,
                max int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES RateModifications(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, min, max)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_BookingWindow
            (
                external_id varchar(20),
                file_id int,
                parent_id int,
                min int,
                max int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES RateModifications(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, min, max)
            )""")


def load_rate_modifications(rate_modifiers: list[RateModifications],
                            file_info: FileInfo.FileInfo,
                            file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if len(rate_modifiers) == 0:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    rowcount = {}
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            delete from RateModifications
            where file_id != ?file_id?
            """,
                         param={"file_id": new_id})
        for rate_modifier in rate_modifiers:
            rowcount['ratemodifiers'] = commands.execute(f"""
                INSERT INTO RateModifications
                (
                    external_id,
                    file_id,
                    multiplier,
                    xml_contents
                )
                values
                (
                    ?external_id?,
                    ?file_id?,
                    ?multiplier?,
                    ?xml_contents?
                ) ON CONFLICT (external_id, file_id, multiplier)
                DO NOTHING
                """,
                                                         param={
                                                             "external_id": rate_modifier.external_id,
                                                             "file_id": new_id,
                                                             "multiplier": float(rate_modifier.price_adjustment),
                                                             "xml_contents": rate_modifier.xml_contents
                                                         })
            last_id = commands.query_first_or_default(f"""
                select seq
                from sqlite_sequence
                WHERE name = ?table_name?
                """,
                                                      param={"table_name": "RateModifications"}, model=LastId,
                                                      default=LastId())

            if len(rate_modifier.booking_dates) > 0:
                rowcount['bookingDates'] = commands.execute(f"""
                    INSERT INTO RateModifications_BookingDates
                    (
                        external_id,
                        file_id,
                        parent_id,
                        start,
                        end,
                        days_of_week
                    )
                    values
                    (
                        ?external_id?,
                        ?file_id?,
                        ?parent_id?,
                        ?start?,
                        ?end?,
                        ?days_of_week?
                    ) ON CONFLICT (external_id, file_id, start, end, days_of_week) DO NOTHING
                    """,
                                                            param=[{
                                                                "external_id": rate_modifier.external_id,
                                                                "file_id": new_id,
                                                                "parent_id": last_id.seq,
                                                                "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                "days_of_week": date_range.days_of_week
                                                            } for date_range in rate_modifier.booking_dates]),
            if len(rate_modifier.checkin_dates) > 0:
                rowcount['checkinDates'] = commands.execute(f"""
                    INSERT INTO RateModifications_CheckinDates
                    (
                        external_id,
                        file_id,
                        parent_id,
                        start,
                        end,
                        days_of_week
                    )
                    values
                    (
                        ?external_id?,
                        ?file_id?,
                        ?parent_id?,
                        ?start?,
                        ?end?,
                        ?days_of_week?
                    ) ON CONFLICT (external_id, file_id, start, end, days_of_week)
                    DO NOTHING
                    """,
                                                            param=[{
                                                                "external_id": rate_modifier.external_id,
                                                                "file_id": new_id,
                                                                "parent_id": last_id.seq,
                                                                "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                "days_of_week": date_range.days_of_week
                                                            } for date_range in rate_modifier.checkin_dates]),
            if len(rate_modifier.checkout_dates) > 0:
                rowcount['checkoutDates'] = commands.execute(f"""
                    INSERT INTO RateModifications_CheckoutDates
                    (
                        external_id,
                        file_id,
                        parent_id,
                        start,
                        end,
                        days_of_week
                    )
                    values
                    (
                        ?external_id?,
                        ?file_id?,
                        ?parent_id?,
                        ?start?,
                        ?end?,
                        ?days_of_week?
                    ) ON CONFLICT (external_id, file_id, start, end, days_of_week)
                    DO NOTHING""",
                                                             param=[{
                                                                 "external_id": rate_modifier.external_id,
                                                                 "file_id": new_id,
                                                                 "parent_id": last_id.seq,
                                                                 "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                 "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                 "days_of_week": date_range.days_of_week
                                                             } for date_range in rate_modifier.checkout_dates]),
            if rate_modifier.length_of_stay is not None:
                rowcount['lengthOfStay'] = commands.execute(f"""
                    INSERT INTO RateModifications_LengthOfStay
                    (
                        external_id,
                        file_id,
                        parent_id,
                        min,
                        max
                    )
                    values
                    (
                        ?external_id?,
                        ?file_id?,
                        ?parent_id?,
                        ?min?,
                        ?max?
                    ) ON CONFLICT (external_id, file_id, min, max) DO NOTHING""",
                                                            param={
                                                                "external_id": rate_modifier.external_id,
                                                                "file_id": new_id,
                                                                "parent_id": last_id.seq,
                                                                "min": rate_modifier.length_of_stay.min,
                                                                "max": rate_modifier.length_of_stay.max
                                                            })
            if rate_modifier.booking_window is not None:
                rowcount['bookingWindow'] = commands.execute(f"""
                    INSERT INTO RateModifications_BookingWindow
                    (
                        external_id,
                        file_id,
                        parent_id,
                        min,
                        max
                    )
                    values
                    (
                        ?external_id?,
                        ?file_id?,
                        ?parent_id?,
                        ?min?,
                        ?max?
                    ) ON CONFLICT (external_id, file_id, min, max) DO NOTHING""",
                                                             param={
                                                                 "external_id": rate_modifier.external_id,
                                                                 "file_id": new_id,
                                                                 "parent_id": last_id.seq,
                                                                 "min": rate_modifier.booking_window.min,
                                                                 "max": rate_modifier.booking_window.max
                                                             })
    file_info.records = len(rate_modifiers)
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_rate_modifications(file_args: DataHandlers.DataFileArgs) -> (list[RateModifications], FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'RateModifications', default=None) is None):
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data, 'RateModifications.@timestamp'))
    results.external_id = glom(file_args.formatted_data, 'RateModifications.HotelRateModifications.@hotel_id')

    itinerary = glom(file_args.formatted_data, '**.ItineraryRateModification')
    if len(itinerary) == 0:
        return [], None

    new_modifiers = []

    for itinerary in get_safe_list(itinerary):
        multiplier = glom(itinerary, 'ModificationActions.PriceAdjustment', default=None)
        if multiplier is None:
            continue
        booking_dates = DateRange.parse_ranges(glom(itinerary, 'BookingDates.DateRange', default=[]))
        checkin_dates = DateRange.parse_ranges(glom(itinerary, 'CheckinDates.DateRange', default=[]))
        checkout_dates = DateRange.parse_ranges(glom(itinerary, 'CheckoutDates.DateRange', default=[]))
        stay_requires = LengthOfStay.parse_range(glom(itinerary, 'LengthOfStay', default=None))
        booking_window = BookingWindow.parse_range_int(glom(itinerary, 'BookingWindow', default=None))
        new_modifiers.append(RateModifications(results.external_id,
                                               booking_dates,
                                               checkin_dates,
                                               checkout_dates,
                                               Decimal(multiplier["@multiplier"]),
                                               stay_requires,
                                               booking_window,
                                               xmltodict.unparse({"ItineraryRateModification": itinerary})))
    return new_modifiers, results
