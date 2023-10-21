import decimal
from dataclasses import dataclass
from datetime import datetime
from pydapper import connect


@dataclass
class Rate:
    external_id: str
    current_day: datetime.date
    start: datetime.date
    end: datetime.date
    base_amount: decimal
    guest_count: int


def get_rates(external_id: str, start: datetime, end: datetime, dsn: str) -> list[Rate]:
    with connect(dsn) as commands:
        return commands.query(f"""
                select o.external_id,
                       date(aa.td, '-1 day') current_day,
                       o.start,
                       o.end,
                       o.base_amount,
                       o.guest_count
                from OTAHotelRateAmountNotifRQ o
                inner join
                (
                    WITH RECURSIVE cnt(x) AS
                    (
                        SELECT 1
                        UNION ALL
                        SELECT x + 1
                        FROM cnt
                        LIMIT (SELECT (JULIANDAY(?end_date?) - JULIANDAY(?start_date?)))
                    )
                    SELECT date(JULIANDAY(?start_date?), '+' || x || ' days') as td
                    FROM cnt
                ) aa
                on aa.td between o.start and o.end
                where o.external_id = ?external_id?
                """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                              }, model=Rate)

# Recursive date range in sqlite stash for need
# alternate approach
# WITH date_range(myDate, nights) as
# (
# 	SELECT DATE(?start_date?, '+1 day') as myDate, 1 as nights
# 	UNION ALL
# 	SELECT DATE(myDate, '+1 day'), nights + 1 as nights
# 	FROM date_range
# 	WHERE nights < (SELECT CAST(JULIANDAY(?end_date?) - JULIANDAY(?start_date?) as INTEGER))
# )
# SELECT *
# FROM date_range;