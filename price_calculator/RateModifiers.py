import decimal
from dataclasses import dataclass
from datetime import datetime

from pydapper import connect


@dataclass
class RateModifier:
    external_id: str
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
            select r.external_id, r.multiplier, r.xml_contents
            from RateModifications r
            left join RateModifications_BookingWindow rbw
            on r.id = rbw.parent_id
            left join RateModifications_BookingDates rbd
            on r.id = rbd.parent_id
            and ?book_date? between COALESCE(rbd.start, DATE(?book_date?, '-1 day')) and COALESCE(rbd.end, DATE(?book_date?, '+1 day'))
            left join RateModifications_CheckinDates rcid
            on r.id = rcid.parent_id
            and ?start_date? between COALESCE(rcid.start, DATE(?start_date?, '-1 day')) and COALESCE(rcid.end, DATE(?start_date?, '+1 day'))
            left join RateModifications_CheckoutDates rcod
            on r.id = rcod.parent_id
            and ?end_date? between COALESCE(rcod.start, DATE(?end_date?, '-1 day')) and COALESCE(rcod.end, DATE(?end_date?, '+1 day'))
            left join RateModifications_LengthOfStay rlos
            on r.id = rlos.parent_id
            WHERE r.external_id = ?external_id?
            and (?nights? between COALESCE(rlos.min, ?nights?) and COALESCE(rlos.max, ?nights?))
            and ((JULIANDAY(?start_date?) - JULIANDAY(?book_date?)) between COALESCE(rbw.min, 0) and  COALESCE(rbw.max, 1000))
            """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                                  "book_date": book_date.isoformat(),
                                  "nights": nights
                              }, model=RateModifier)
