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
    day_multiplier: int


def get_rates(external_id: str, start: datetime, end: datetime, dsn: str) -> list[Rate]:
    with connect(dsn) as commands:
        return commands.query(f"""
                select external_id,
                    start,
                    end,
                    base_amount,
                    guest_count,
                    (
                        CAST(JULIANDAY(end) - JULIANDAY(start) as int) -
                        CASE WHEN end > ?end_date? THEN CAST(JULIANDAY(end) - JULIANDAY(?end_date?) as int) ELSE 0 END -
                        CASE WHEN start < ?start_date? THEN CAST(JULIANDAY(start) - JULIANDAY(?start_date?) as int) ELSE 0 END
                    ) as day_multiplier
                from OTAHotelRateAmountNotifRQ
                where external_id = ?external_id? and start between ?start_date? and ?end_date?
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
