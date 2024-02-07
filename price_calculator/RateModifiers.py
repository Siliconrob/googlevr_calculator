import decimal
from dataclasses import dataclass
from datetime import datetime

from pydapper import connect


@dataclass
class RateModifier:
    external_id: str
    rate_id: str
    multiplier: decimal
    xml_contents: str


def get_rate_modifiers(external_id: str,
                       start: datetime,
                       end: datetime,
                       nights: int,
                       book_date: datetime,
                       dsn: str) -> list[RateModifier]:
    with connect(dsn) as commands:
        return commands.query(f"""
            select r.external_id, r.multiplier, r.rate_id, r.xml_contents
            from RateModifications r
            left join RateModifications_BookingWindow rbw
            on r.id = rbw.parent_id
            left join RateModifications_BookingDates rbd
            on r.id = rbd.parent_id
            left join RateModifications_CheckinDates rcid
            on r.id = rcid.parent_id            
            left join RateModifications_CheckoutDates rcod
            on r.id = rcod.parent_id            
            left join RateModifications_LengthOfStay rlos
            on r.id = rlos.parent_id
            WHERE r.external_id = ?external_id?
            and (?nights? between COALESCE(rlos.min, ?nights?) and COALESCE(rlos.max, ?nights?))
            and ((JULIANDAY(?start_date?) - JULIANDAY(?book_date?)) between COALESCE(rbw.min, 0) and  COALESCE(rbw.max, 1000))
            and ?book_date? between COALESCE(rbd.start, DATE(?book_date?, '-1 day')) and COALESCE(rbd.end, DATE(?book_date?, '+1 day'))
            and ?start_date? between COALESCE(rcid.start, DATE(?start_date?, '-1 day')) and COALESCE(rcid.end, DATE(?start_date?, '+1 day'))
            and ?end_date? between COALESCE(rcod.start, DATE(?end_date?, '-1 day')) and COALESCE(rcod.end, DATE(?end_date?, '+1 day'))            
            and EXISTS
            (
                select day_id
                from DayOfTheWeek dw
                where instr((select upper(rbd.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', ?book_date?)
                UNION
                select day_id from DayOfTheWeek dw WHERE rbd.days_of_week IS NULL
            )
            and EXISTS
            (
                select day_id
                from DayOfTheWeek dw
                where instr((select upper(rcid.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', ?start_date?)
                UNION
                select day_id from DayOfTheWeek dw WHERE rcid.days_of_week IS NULL
            )
            and EXISTS
            (
                select day_id
                from DayOfTheWeek dw
                where instr((select upper(rcod.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', ?end_date?)
                UNION
                select day_id from DayOfTheWeek dw WHERE rcod.days_of_week IS NULL
            )            
            """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                                  "book_date": book_date.isoformat(),
                                  "nights": nights
                              }, model=RateModifier)

# WITH the_dates as (
#     WITH RECURSIVE cnt(x) AS
#     (
#         SELECT 0
#         UNION ALL
#         SELECT x + 1
#         FROM cnt
#         LIMIT (SELECT (JULIANDAY(?end_date?) - JULIANDAY(?start_date?)))
#     )
#     SELECT date(JULIANDAY(?start_date?), '+' || x || ' days') as gen_date
#     FROM cnt
# )
# select rm.id, td.gen_date, rm.multiplier, rm.rate_id, rm.xml_contents
# FROM RateModifications rm
# left join RateModifications_StayDates rsd
# on rm.id = rsd.parent_id
# left join the_dates td
# where td.gen_date between DATE(rsd.start) and DATE(rsd.end);