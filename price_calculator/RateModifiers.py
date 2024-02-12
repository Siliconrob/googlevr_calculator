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

# WITH reservation_dates as (
#     WITH RECURSIVE cnt(x) AS
#     (
#         SELECT 0
#         UNION ALL
#         SELECT x + 1
#         FROM cnt LIMIT (SELECT (JULIANDAY('2024-03-16') - JULIANDAY('2024-03-01')))
#     )
#     SELECT date(JULIANDAY('2024-03-01'), '+' || x || ' days') as gen_date, upper(dw.google_code) google_day_id
#     FROM cnt
#     INNER JOIN DayOfTheWeek dw
#     on dw.day_id = strftime('%w', date(JULIANDAY('2024-03-01'), '+' || x || ' days'))
# ),
# check_in_dates as (
#     select rm.id check_in_id
#     FROM RateModifications rm
#     LEFT JOIN RateModifications_CheckinDates rcid
#     ON rm.id = rcid.parent_id
#     WHERE '2024-03-01' between DATE(COALESCE(rcid.start, '2024-03-01')) and DATE(COALESCE(rcid.end, '2024-03-01'))
#     AND EXISTS
#     (
#         select day_id from DayOfTheWeek dw where instr((select upper(rcid.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', '2024-03-01')
#         UNION
#         select day_id from DayOfTheWeek dw WHERE rcid.days_of_week IS NULL
#     )
# ),
# check_out_dates as (
#     select rm.id check_out_id
#     FROM RateModifications rm
#     LEFT JOIN RateModifications_CheckoutDates rcod
#     ON rm.id = rcod.parent_id
#     WHERE '2024-03-16' between DATE(COALESCE(rcod.start, '2024-03-16')) and DATE(COALESCE(rcod.end, '2024-03-16'))
#     AND EXISTS
#     (
#         select day_id from DayOfTheWeek dw where instr((select upper(rcod.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', '2024-03-16')
#         UNION
#         select day_id from DayOfTheWeek dw WHERE rcod.days_of_week IS NULL
#     )
# ),
# book_dates as (
#     select rm.id book_id
#     FROM RateModifications rm
#     LEFT JOIN RateModifications_BookingDates rbd
#     ON rm.id = rbd.parent_id
#     WHERE '2024-02-11' between DATE(COALESCE(rbd.start, '2024-02-11')) and DATE(COALESCE(rbd.end, '2024-02-11'))
#     AND EXISTS
#     (
#         select day_id from DayOfTheWeek dw where instr((select upper(rbd.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', '2024-2-11')
#         UNION
#         select day_id from DayOfTheWeek dw WHERE rbd.days_of_week IS NULL
#     )
# )
# SELECT rd.gen_date, rm.id, rm.multiplier, rm.external_id, rm.xml_contents
# --        cid.check_in_id,
# --        cod.check_out_id,
# --        bd.book_id,
# --         (
# --             SELECT rd.gen_date between COALESCE(rsd.start, rd.gen_date) and COALESCE(rsd.end, rd.gen_date)
# --             AND EXISTS
# --             (
# --                 select day_id from DayOfTheWeek dw where instr((select upper(rsd.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', rd.gen_date)
# --                 UNION
# --                 select day_id from DayOfTheWeek dw WHERE rsd.days_of_week IS NULL
# --             )
# --         ) stay_id
# FROM reservation_dates rd
# LEFT JOIN RateModifications rm
# LEFT JOIN RateModifications_BookingWindow rbw
#     on rm.id = rbw.parent_id
# LEFT JOIN RateModifications_LengthOfStay rlos
#     on rm.id = rlos.parent_id
# LEFT JOIN check_in_dates cid
#     ON rm.id = cid.check_in_id
# LEFT JOIN check_out_dates cod
#     ON rm.id = cod.check_out_id
# LEFT JOIN book_dates bd
#     on rm.id = bd.book_id
# LEFT JOIN RateModifications_StayDates rsd
#     ON rm.id = rsd.parent_id
# WHERE EXISTS
# (
#     SELECT rd.gen_date between COALESCE(rsd.start, rd.gen_date) and COALESCE(rsd.end, rd.gen_date)
#     AND EXISTS
#     (
#         select day_id
#         from DayOfTheWeek dw
#         where instr((select upper(rsd.days_of_week)), upper(dw.google_code)) > 0
#          and dw.day_id = strftime('%w', rd.gen_date)
#         UNION
#         select day_id
#         from DayOfTheWeek dw
#         WHERE rsd.days_of_week IS NULL
#     )
# )
# AND
# (
#     cid.check_in_id > 0
#     AND cod.check_out_id > 0
#     AND bd.book_id > 0
# );