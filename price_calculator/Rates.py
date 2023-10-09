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
                select external_id, start, end, base_amount, guest_count
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
