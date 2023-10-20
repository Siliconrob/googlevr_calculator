import decimal
from dataclasses import dataclass
from datetime import datetime
from pydapper import connect


@dataclass
class Rate:
    external_id: str
    start: datetime.date
    end: datetime.date
    base_amount: decimal
    guest_count: int


def get_rates(external_id: str, start: datetime, end: datetime, dsn: str) -> list[Rate]:
    with connect(dsn) as commands:
        return commands.query(f"""
                select o.external_id,
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
                """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                              }, model=Rate)

# Recursive date range in sqlite stash for need
# WITH date_range(myDate, level) as
# (
# 	SELECT '2023-10-01' as myDate, 0 as nights
#    UNION ALL
#    SELECT DATE(myDate, '+1 day'), nights + 1
#    FROM date_range
#    WHERE nights < 8
# )
# SELECT *
# FROM date_range
