import decimal
from dataclasses import dataclass
from datetime import datetime

from pydapper import connect


@dataclass
class TaxOrFee:
    external_id: str
    calc_type: str
    period: str
    amount: decimal
    xml_contents: str


def tax_or_fee_base_query(table_prefix: str):
    return f"""
        select tf.external_id, tf.calc_type, tf.period, tf.amount, tf.xml_contents
        from {table_prefix} tf
        left join {table_prefix}_BookingDates tfbd
        on tf.id = tfbd.parent_id
        and ?book_date? between COALESCE(tfbd.start, DATE(?book_date?, '-1 day')) and COALESCE(tfbd.end, DATE(?book_date?, '+1 day'))
        left join {table_prefix}_CheckinDates tfcid
        on tf.id = tfcid.parent_id
        and ?start_date? between COALESCE(tfcid.start, DATE(?start_date?, '-1 day')) and COALESCE(tfcid.end, DATE(?start_date?, '+1 day'))
        left join {table_prefix}_CheckoutDates tfcod
        on tf.id = tfcod.parent_id
        and ?end_date? between COALESCE(tfcod.start, DATE(?end_date?, '-1 day')) and COALESCE(tfcod.end, DATE(?end_date?, '+1 day'))
        left join {table_prefix}_LengthOfStay tflos
        on tf.id = tflos.parent_id
        and COALESCE(tflos.min, ?nights?) <= ?nights? and COALESCE(tflos.max, ?nights?) >= ?nights?
        WHERE tf.external_id = ?external_id?
        and EXISTS
        (
            select day_id
            from DayOfTheWeek dw
            where instr((select upper(tfbd.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', ?book_date?)
            UNION
            select day_id from DayOfTheWeek dw WHERE tfbd.days_of_week IS NULL
        )
        and EXISTS
        (
            select day_id
            from DayOfTheWeek dw
            where instr((select upper(tfcid.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', ?start_date?)
            UNION
            select day_id from DayOfTheWeek dw WHERE tfcid.days_of_week IS NULL
        )
        and EXISTS
        (
            select day_id
            from DayOfTheWeek dw
            where instr((select upper(tfcod.days_of_week)), upper(dw.google_code)) > 0 and dw.day_id = strftime('%w', ?end_date?)
            UNION
            select day_id from DayOfTheWeek dw WHERE tfcod.days_of_week IS NULL
        )        
        """


def get_taxes_or_fees(table_prefix: str, external_id: str, start: datetime, end: datetime, nights: int,
                      book_date: datetime, dsn: str) -> list[TaxOrFee]:
    with connect(dsn) as commands:
        return commands.query(tax_or_fee_base_query(table_prefix),
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                                  "book_date": book_date.isoformat(),
                                  "nights": nights
                              }, model=TaxOrFee)


def get_taxes(external_id: str, start: datetime, end: datetime, nights: int, book_date: datetime, dsn: str) -> list[
    TaxOrFee]:
    return get_taxes_or_fees("Tax", external_id, start, end, nights, book_date, dsn)


def get_fees(external_id: str, start: datetime, end: datetime, nights: int, book_date: datetime, dsn: str) -> list[
    TaxOrFee]:
    return get_taxes_or_fees("Fee", external_id, start, end, nights, book_date, dsn)
