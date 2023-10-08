import decimal
from dataclasses import dataclass
from datetime import datetime
from pydapper import connect


@dataclass
class Promotion:
    external_id: str
    promotion_id: str
    percentage: decimal
    fixed_amount: decimal
    fixed_amount_per_night: decimal
    fixed_price: decimal


def get_promotions(external_id: str,
                   start: datetime,
                   end: datetime,
                   nights: int,
                   book_date: datetime,
                   dsn: str) -> list[Promotion]:
    with connect(dsn) as commands:
        return commands.query(f"""
            select p.external_id, p.promotion_id, p.percentage, p.fixed_amount, p.fixed_amount_per_night, p.fixed_price
            from Promotion p
            left join Promotion_BookingDates pbd
            on p.promotion_id = pbd.promotion_id
            and p.external_id = pbd.external_id
            and ?book_date? between COALESCE(pbd.start, DATE(?book_date?, '-1 day')) and COALESCE(pbd.end, DATE(?book_date?, '+1 day'))
            left join Promotion_CheckinDates pcid
            on p.promotion_id = pcid.promotion_id
            and p.external_id = pcid.external_id
            and ?start_date? between COALESCE(pcid.start, DATE(?start_date?, '-1 day')) and COALESCE(pcid.end, DATE(?start_date?, '+1 day'))
            left join Promotion_CheckoutDates pcod
            on p.promotion_id = pcod.promotion_id
            and p.external_id = pcod.external_id
            and ?end_date? between COALESCE(pcod.start, DATE(?end_date?, '-1 day')) and COALESCE(pcod.end, DATE(?end_date?, '+1 day'))
            left join Promotion_LengthOfStay plos
            on p.promotion_id = plos.promotion_id
            and p.external_id = plos.external_id
            and COALESCE(plos.min, ?nights?) <= ?nights? and COALESCE(plos.max, ?nights?) >= ?nights?        
            WHERE p.external_id = ?external_id?
            """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                                  "book_date": book_date.isoformat(),
                                  "nights": nights
                              }, model=Promotion)
