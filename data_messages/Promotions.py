import decimal
from dataclasses import dataclass

import xmltodict
from glom import glom
from pydapper import connect

from data_messages import DateRange, LengthOfStay, FileInfo, BookingWindow
from fileset import DataHandlers
from data_messages.BookingWindow import BookingWindowTimeSpan
from fileset.DataHandlers import get_safe_list
from data_messages.LastId import LastId


@dataclass
class Promotion:
    external_id: str
    id: str
    booking_dates: list[DateRange]
    checkin_dates: list[DateRange]
    checkout_dates: list[DateRange]
    stay_dates: list[DateRange]
    length_of_stay: LengthOfStay
    booking_window: BookingWindowTimeSpan
    percentage: decimal
    fixed_amount: decimal
    fixed_amount_per_night: decimal
    fixed_price: decimal
    stacking: str
    xml_contents: str


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    promotions, file_info = read_promotions(file_args)
    return load_promotions(promotions, file_info, file_args)


def create_tables(dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
            create table if not exists Promotion (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_id varchar(20),
            promotion_id varchar(100),
            percentage DECIMAL(18,6),
            fixed_amount DECIMAL(18,6),
            fixed_amount_per_night DECIMAL(18,6),
            fixed_price DECIMAL(18,6),
            xml_contents TEXT,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,              
            UNIQUE(external_id, promotion_id, file_id))
            """)
        commands.execute(f"""
            create table if not exists Promotion_BookingDates (
            external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,            
            UNIQUE(external_id, file_id, parent_id, start, end, days_of_week))
            """)
        commands.execute(f"""
            create table if not exists Promotion_CheckinDates (
            external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,            
            UNIQUE(external_id, file_id, parent_id, start, end, days_of_week))
            """)
        commands.execute(f"""
            create table if not exists Promotion_CheckoutDates (
            external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,
            UNIQUE(external_id, file_id, parent_id, start, end, days_of_week))
            """)
        commands.execute(f"""
            create table if not exists Promotion_StayDates (
            external_id varchar(20),
            start TEXT,
            end TEXT,
            days_of_week TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,
            UNIQUE(external_id, file_id, parent_id, start, end, days_of_week))
            """)
        commands.execute(f"""
            create table if not exists Promotion_LengthOfStay (
            external_id varchar(20),
            min int,
            max int,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,                                     
            UNIQUE(external_id, file_id, parent_id, min, max))
            """)
        commands.execute(f"""
            create table if not exists Promotion_BookingWindow
            (
                external_id varchar(20),
                file_id int,
                parent_id int,
                min int,
                max int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,                 
                PRIMARY KEY(external_id, file_id, parent_id, min, max)
            )""")


def load_promotions(promotions: list[Promotion], file_info: FileInfo.FileInfo,
                    file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if len(promotions) == 0:
        return None
    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    rowcount = {}
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            delete from Promotion
            where file_id != ?file_id?
            """,
                         param={"file_id": new_id})
        for promotion in promotions:
            rowcount['promotion'] = commands.execute(
                f"""
                INSERT INTO Promotion (
                    external_id,
                    promotion_id,
                    file_id,
                    percentage,
                    fixed_amount,
                    fixed_amount_per_night,
                    fixed_price,
                    xml_contents
                )
                values (
                    ?external_id?,
                    ?promotion_id?,
                    ?file_id?,
                    ?percentage?,
                    ?fixed_amount?,
                    ?fixed_amount_per_night?,
                    ?fixed_price?,
                    ?xml_contents?
                )
                ON CONFLICT (external_id, promotion_id, file_id)
                DO UPDATE
                    SET percentage = ?percentage?,
                    fixed_amount = ?fixed_amount?,
                    fixed_amount_per_night = ?fixed_amount_per_night?,
                    fixed_price = ?fixed_price?,
                    xml_contents = ?xml_contents?
                    """,
                param={
                    "external_id": promotion.external_id,
                    "promotion_id": promotion.id,
                    "file_id": new_id,
                    "percentage": None if promotion.percentage is None else float(promotion.percentage),
                    "fixed_amount": None if promotion.fixed_amount is None else float(promotion.fixed_amount),
                    "fixed_amount_per_night": None if promotion.fixed_amount_per_night is None else float(
                        promotion.fixed_amount_per_night),
                    "fixed_price": None if promotion.fixed_price is None else float(promotion.fixed_price),
                    "xml_contents": promotion.xml_contents
                })
            last_id = commands.query_first_or_default(f"""
                select seq
                from sqlite_sequence
                WHERE name = ?table_name?
                """,
                                                      param={"table_name": "Promotion"}, model=LastId, default=LastId())

            if len(promotion.booking_dates) > 0:
                rowcount['booking_dates'] = commands.execute(f"""
                INSERT INTO Promotion_BookingDates
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
                )
                ON CONFLICT (external_id, file_id, parent_id, start, end, days_of_week)
                DO NOTHING
                """,
                                                             param=[{
                                                                 "external_id": promotion.external_id,
                                                                 "file_id": new_id,
                                                                 "parent_id": last_id.seq,
                                                                 "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                 "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                 "days_of_week": date_range.days_of_week
                                                             } for date_range in promotion.booking_dates]),
            if len(promotion.checkin_dates) > 0:
                rowcount['checkin_dates'] = commands.execute(f"""
                INSERT INTO Promotion_CheckinDates
                (
                    external_id,
                    file_id,
                    parent_id,
                    start,
                    end,
                    days_of_week
                )
                values (
                    ?external_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?start?,
                    ?end?,
                    ?days_of_week?
                )
                ON CONFLICT (external_id, file_id, parent_id, start, end, days_of_week)
                DO NOTHING
                """,
                                                             param=[{
                                                                 "external_id": promotion.external_id,
                                                                 "file_id": new_id,
                                                                 "parent_id": last_id.seq,
                                                                 "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                 "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                 "days_of_week": date_range.days_of_week
                                                             } for date_range in promotion.checkin_dates])
            if len(promotion.checkout_dates) > 0:
                rowcount['checkout_dates'] = commands.execute(f"""
                INSERT INTO Promotion_CheckoutDates
                (
                    external_id,
                    file_id,
                    parent_id,
                    start,
                    end,
                    days_of_week
                )
                values (
                    ?external_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?start?,
                    ?end?,
                    ?days_of_week?
                    )
                ON CONFLICT (external_id, file_id, parent_id, start, end, days_of_week)
                DO NOTHING
                """,
                                                              param=[{
                                                                  "external_id": promotion.external_id,
                                                                  "file_id": new_id,
                                                                  "parent_id": last_id.seq,
                                                                  "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                  "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                  "days_of_week": date_range.days_of_week
                                                              } for date_range in promotion.checkout_dates])
            if len(promotion.stay_dates) > 0:
                rowcount['stay_dates'] = commands.execute(f"""
                INSERT INTO Promotion_StayDates
                (
                    external_id,
                    file_id,
                    parent_id,
                    start,
                    end,
                    days_of_week
                )
                values (
                    ?external_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?start?,
                    ?end?,
                    ?days_of_week?
                    )
                ON CONFLICT (external_id, file_id, parent_id, start, end, days_of_week)
                DO NOTHING
                """,
                                                              param=[{
                                                                  "external_id": promotion.external_id,
                                                                  "file_id": new_id,
                                                                  "parent_id": last_id.seq,
                                                                  "start": None if date_range.start is None else date_range.start.isoformat(),
                                                                  "end": None if date_range.end is None else date_range.end.isoformat(),
                                                                  "days_of_week": date_range.days_of_week
                                                              } for date_range in promotion.stay_dates])
            if promotion.length_of_stay is not None:
                rowcount['length_of_stay'] = commands.execute(f"""
                INSERT INTO Promotion_LengthOfStay
                (
                    external_id,
                    file_id,
                    parent_id,
                    min,
                    max
                )
                values (
                    ?external_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?min?,
                    ?max?
                ) ON CONFLICT (external_id, file_id, parent_id, min, max)
                DO NOTHING
                """,
                                                              param={
                                                                  "external_id": promotion.external_id,
                                                                  "file_id": new_id,
                                                                  "parent_id": last_id.seq,
                                                                  "min": None if promotion.length_of_stay.min is None else promotion.length_of_stay.min,
                                                                  "max": None if promotion.length_of_stay.max is None else promotion.length_of_stay.max
                                                              })
            if promotion.booking_window is not None:
                rowcount['bookingWindow'] = commands.execute(f"""
                    INSERT INTO Promotion_BookingWindow
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
                                                                 "external_id": promotion.external_id,
                                                                 "file_id": new_id,
                                                                 "parent_id": last_id.seq,
                                                                 "min": None if promotion.booking_window.min is None else promotion.booking_window.min.days,
                                                                 "max": None if promotion.booking_window.max is None else promotion.booking_window.max.days
                                                             })

    file_info.records = len(promotions)
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_promotions(file_args: DataHandlers.DataFileArgs) -> (list[Promotion], FileInfo.FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'Promotions', default=None) is None):
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data, 'Promotions.@timestamp'))
    results.external_id = glom(file_args.formatted_data, 'Promotions.HotelPromotions.@hotel_id')
    promotions = []
    file_promotions = glom(file_args.formatted_data, '**.Promotion')
    if len(file_promotions) == 0:
        return [], None

    for promotion in get_safe_list(file_promotions):
        discount = glom(promotion, 'Discount', default=None)
        if discount is None:
            continue
        stacks = glom(promotion, 'Stacking')
        stacking_type = stacks.get("@type") if stacks is not None else None

        promotions.append(Promotion(results.external_id,
                                    promotion.get("@id"),
                                    DateRange.parse_ranges(glom(promotion, 'BookingDates.DateRange', default=[])),
                                    DateRange.parse_ranges(glom(promotion, 'CheckinDates.DateRange', default=[])),
                                    DateRange.parse_ranges(glom(promotion, 'CheckoutDates.DateRange', default=[])),
                                    DateRange.parse_ranges(glom(promotion, 'StayDates.DateRange', default=[])),
                                    LengthOfStay.parse_range(glom(promotion, 'LengthOfStay', default=None)),
                                    BookingWindow.parse_range_timespans(glom(promotion, 'BookingWindow', default=None)),
                                    discount.get("@percentage"),
                                    discount.get("@fixed_amount"),
                                    discount.get("@fixed_amount_per_night"),
                                    discount.get("@fixed_price"),
                                    stacking_type,
                                    xmltodict.unparse({"Promotion": promotion})))
    return promotions, results
