import decimal
from dataclasses import dataclass
from glom import glom
from pydapper import connect
from data_messages import DateRange, LengthOfStay, FileInfo, DataHandlers
from data_messages.LastId import LastId


@dataclass
class Promotion:
    external_id: str
    id: str
    booking_dates: list[DateRange]
    checkin_dates: list[DateRange]
    checkout_dates: list[DateRange]
    length_of_stay: LengthOfStay
    percentage: decimal
    fixed_amount: decimal
    fixed_amount_per_night: decimal
    fixed_price: decimal
    stacking: str


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
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,              
            UNIQUE(external_id, promotion_id, file_id))
            """)
        commands.execute(f"""
            create table if not exists Promotion_BookingDates (
            external_id varchar(20),
            promotion_id varchar(100),
            start TEXT,
            end TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,            
            UNIQUE(external_id, promotion_id, file_id, start, end))
            """)
        commands.execute(f"""
            create table if not exists Promotion_CheckinDates (
            external_id varchar(20),
            promotion_id varchar(100),
            start TEXT,
            end TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,            
            UNIQUE(external_id, promotion_id, file_id, start, end))
            """)
        commands.execute(f"""
            create table if not exists Promotion_CheckoutDates (
            external_id varchar(20),
            promotion_id varchar(100),
            start TEXT,
            end TEXT,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,
            UNIQUE(external_id, promotion_id, file_id, start, end))
            """)
        commands.execute(f"""
            create table if not exists Promotion_LengthOfStay (
            external_id varchar(20),
            promotion_id varchar(100),
            min int,
            max int,
            file_id int,
            parent_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            FOREIGN KEY (parent_id) REFERENCES Promotion(id) ON DELETE CASCADE,                                     
            UNIQUE(external_id, promotion_id, file_id))
            """)


def load_promotions(promotions: list[Promotion], file_info: FileInfo.FileInfo, file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
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
                    fixed_price
                )
                values (
                    ?external_id?,
                    ?promotion_id?,
                    ?file_id?,
                    ?percentage?,
                    ?fixed_amount?,
                    ?fixed_amount_per_night?,
                    ?fixed_price?
                )
                ON CONFLICT (external_id, promotion_id, file_id)
                DO UPDATE
                    SET percentage = ?percentage?,
                    fixed_amount = ?fixed_amount?,
                    fixed_amount_per_night = ?fixed_amount_per_night?,
                    fixed_price = ?fixed_price?
                    """,
                param={
                    "external_id": promotion.external_id,
                    "promotion_id": promotion.id,
                    "file_id": new_id,
                    "percentage": None if promotion.percentage is None else float(promotion.percentage),
                    "fixed_amount": None if promotion.fixed_amount is None else float(promotion.fixed_amount),
                    "fixed_amount_per_night": None if promotion.fixed_amount_per_night is None else float(promotion.fixed_amount_per_night),
                    "fixed_price": None if promotion.fixed_price is None else float(promotion.fixed_price),
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
                    promotion_id,
                    file_id,
                    parent_id,
                    start,
                    end
                )
                values
                (
                    ?external_id?,
                    ?promotion_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?start?,
                    ?end?
                )
                ON CONFLICT (external_id, promotion_id, file_id, start, end)
                DO NOTHING
                """,
                    param=[{
                        "external_id": promotion.external_id,
                        "promotion_id": promotion.id,
                        "file_id": new_id,
                        "parent_id": last_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat()
                    } for date_range in promotion.booking_dates]),
            if len(promotion.checkin_dates) > 0:
                rowcount['checkin_dates'] = commands.execute(f"""
                INSERT INTO Promotion_CheckinDates
                (
                    external_id,
                    promotion_id,
                    file_id,
                    parent_id,
                    start,
                    end
                )
                values (
                    ?external_id?,
                    ?promotion_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?start?,
                    ?end?
                )
                ON CONFLICT (external_id, promotion_id, file_id, start, end)
                DO NOTHING
                """,
                    param=[{
                        "external_id": promotion.external_id,
                        "promotion_id": promotion.id,
                        "file_id": new_id,
                        "parent_id": last_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat()
                    } for date_range in promotion.checkin_dates]),
            if len(promotion.checkout_dates) > 0:
                rowcount['checkout_dates'] = commands.execute(f"""
                INSERT INTO Promotion_CheckoutDates
                (
                    external_id,
                    promotion_id,
                    file_id,
                    parent_id,
                    start,
                    end
                )
                values (
                    ?external_id?,
                    ?promotion_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?start?,
                    ?end?
                    )
                ON CONFLICT (external_id, promotion_id, file_id, start, end)
                DO NOTHING
                """,
                    param=[{
                        "external_id": promotion.external_id,
                        "promotion_id": promotion.id,
                        "file_id": new_id,
                        "parent_id": last_id,
                        "start": None if date_range.start is None else date_range.start.isoformat(),
                        "end": None if date_range.end is None else date_range.end.isoformat()
                    } for date_range in promotion.checkout_dates]),
            if promotion.length_of_stay is not None:
                rowcount['length_of_stay'] = commands.execute(f"""
                INSERT INTO Promotion_LengthOfStay
                (
                    external_id,
                    promotion_id,
                    file_id,
                    parent_id,
                    min,
                    max
                )
                values (
                    ?external_id?,
                    ?promotion_id?,
                    ?file_id?,
                    ?parent_id?,
                    ?min?,
                    ?max?
                ) ON CONFLICT (external_id, promotion_id, file_id)
                DO NOTHING
                """,
                    param={
                        "external_id": promotion.external_id,
                        "promotion_id": promotion.id,
                        "file_id": new_id,
                        "parent_id": last_id,
                        "min": None if promotion.length_of_stay.min is None else promotion.length_of_stay.min,
                        "max": None if promotion.length_of_stay.max is None else promotion.length_of_stay.max
                    }),

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
    for promotion in file_promotions.pop():
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
                                    LengthOfStay.parse_range(glom(promotion, 'LengthOfStay', default=None)),
                                    discount.get("@percentage"),
                                    discount.get("@fixed_amount"),
                                    discount.get("@fixed_amount_per_night"),
                                    discount.get("@fixed_price"),
                                    stacking_type))
    return promotions, results
