import decimal
from dataclasses import dataclass
from decimal import Decimal
from glom import glom
from pydapper import connect
from data_messages import DateRange, LengthOfStay
from data_messages import FileInfo, DataHandlers

@dataclass
class RateModifications:
    external_id: str
    booking_dates: list[DateRange]
    checkin_dates: list[DateRange]
    checkout_dates: list[DateRange]
    price_adjustment: decimal
    length_of_stay: LengthOfStay


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    rate_modifier, file_info = read_rate_modifications(file_args)
    return load_rate_modifications(rate_modifier, file_info, file_args)


def load_rate_modifications(rate_modifier: RateModifications,
                            file_info: FileInfo.FileInfo,
                            file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if rate_modifier is None:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            create table if not exists RateModifications
            (
                external_id varchar(20),
                multiplier DECIMAL(18,6),
                file_id int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,                
                PRIMARY KEY(external_id, file_id, multiplier)
            )""")
        commands.execute(f"""
            delete from RateModifications
            where file_id != ?file_id?
            """,
            param={"file_id": new_id})
        commands.execute(f"""
            create table if not exists RateModifications_BookingDates
            (
                external_id varchar(20),
                file_id int,
                start TEXT,
                end TEXT,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, start, end)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_CheckinDates
            (
                external_id varchar(20),
                file_id int,
                start TEXT,
                end TEXT,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, start, end)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_CheckoutDates
            (
                external_id varchar(20),
                file_id int,
                start TEXT,
                end TEXT,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, start, end)
            )""")
        commands.execute(f"""
            create table if not exists RateModifications_LengthOfStay
            (
                external_id varchar(20),
                file_id int,
                min int,
                max int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, min, max)
            )""")

        rowcounts = {}
        rowcounts['ratemodifiers'] = commands.execute(f"""
            INSERT INTO RateModifications
            (
                external_id,
                file_id,
                multiplier
            )
            values
            (
                ?external_id?,
                ?file_id?,
                ?multiplier?
            ) ON CONFLICT (external_id, file_id, multiplier)
            DO NOTHING
            """,
            param={
                "external_id": rate_modifier.external_id,
                "file_id": new_id,
                "multiplier": float(rate_modifier.price_adjustment)
            })

        if len(rate_modifier.booking_dates) > 0:
            rowcounts['bookingDates'] = commands.execute(f"""
                INSERT INTO RateModifications_BookingDates
                (
                    external_id,
                    file_id,
                    start,
                    end
                )
                values
                (
                    ?external_id?,
                    ?file_id?,
                    ?start?,
                    ?end?
                ) ON CONFLICT (external_id, file_id, start, end) DO NOTHING
                """,
                param=[{
                    "external_id": rate_modifier.external_id,
                    "file_id": new_id,
                    "start": date_range.start.isoformat(),
                    "end": date_range.end.isoformat()
                } for date_range in rate_modifier.booking_dates]),
        if len(rate_modifier.checkin_dates) > 0:
            rowcounts['checkinDates'] = commands.execute(f"""
                INSERT INTO RateModifications_CheckinDates
                (
                    external_id,
                    file_id,
                    start,
                    end
                )
                values
                (
                    ?external_id?,
                    ?file_id?,
                    ?start?,
                    ?end?
                ) ON CONFLICT (external_id, file_id, start, end)
                DO NOTHING
                """,
                param=[{
                    "external_id": rate_modifier.external_id,
                    "file_id": new_id,
                    "start": dateRange.start.isoformat(),
                    "end": dateRange.end.isoformat()
                } for dateRange in rate_modifier.checkin_dates]),
        if len(rate_modifier.checkout_dates) > 0:
            rowcounts['checkoutDates'] = commands.execute(f"""
                INSERT INTO RateModifications_CheckoutDates
                (
                    external_id,
                    file_id,
                    start,
                    end
                )
                values
                (
                    ?external_id?,
                    ?file_id?,
                    ?start?,
                    ?end?
                ) ON CONFLICT (external_id, file_id, start, end)
                DO NOTHING""",
                param=[{
                    "external_id": rate_modifier.external_id,
                    "file_id": new_id,
                    "start": dateRange.start.isoformat(),
                    "end": dateRange.end.isoformat()
                } for dateRange in rate_modifier.checkout_dates]),
        if rate_modifier.length_of_stay is not None:
            rowcounts['lengthOfStay'] = commands.execute(f"""
                INSERT INTO RateModifications_LengthOfStay
                (
                    external_id,
                    file_id,
                    min,
                    max
                )
                values
                (
                    ?external_id?,
                    ?file_id?,
                    ?min?,
                    ?max?
                ) ON CONFLICT (external_id, file_id, min, max) DO NOTHING""",
                param={
                    "external_id": rate_modifier.external_id,
                    "file_id": new_id,
                    "min": rate_modifier.length_of_stay.min,
                    "max": rate_modifier.length_of_stay.max
                }),
    file_info.records = 1
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_rate_modifications(file_args: DataHandlers.DataFileArgs) -> (RateModifications, FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'RateModifications', default=None) is None):
        return None, None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data, 'RateModifications.@timestamp'))
    results.external_id = glom(file_args.formatted_data, 'RateModifications.HotelRateModifications.@hotel_id')
    itinerary = glom(file_args.formatted_data, '**.ItineraryRateModification').pop()
    multiplier = glom(itinerary, 'ModificationActions.PriceAdjustment', default=None)
    if multiplier is None:
        return None, None
    booking_dates = DateRange.parse_ranges(glom(itinerary, 'BookingDates.DateRange', default=[]))
    checkin_dates = DateRange.parse_ranges(glom(itinerary, 'CheckinDates.DateRange', default=[]))
    checkout_dates = DateRange.parse_ranges(glom(itinerary, 'CheckoutDates.DateRange', default=[]))
    stay_requires = LengthOfStay.parse_range(glom(itinerary, 'LengthOfStay', default=None))
    new_modifier = RateModifications(results.external_id,
                                     booking_dates,
                                     checkin_dates,
                                     checkout_dates,
                                     Decimal(multiplier["@multiplier"]),
                                     stay_requires)
    return new_modifier, results
