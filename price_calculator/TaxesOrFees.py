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


def tax_or_fee_base_query(table_prefix: str):
    return f"""
        select tf.external_id, tf.calc_type, tf.period, tf.amount
        from {table_prefix} tf
        left join {table_prefix}_BookingDates bd
        on tf.id = bd.parent_id
        and bd.start between ?start_date? and ?end_date?
        left join {table_prefix}_CheckinDates cid
        on tf.id = cid.parent_id
        and cid.start between ?start_date? and ?end_date?
        left join {table_prefix}_CheckoutDates cod
        on tf.id = cod.parent_id
        and cod.start between ?start_date? and ?end_date?
        left join {table_prefix}_LengthOfStay tflos
        on tf.id = tflos.parent_id
        and tflos.min <= ?nights? and tflos.max > ?nights?
        where tf.external_id = ?external_id?
        """


def get_taxes_or_fees(table_prefix: str, external_id: str, start: datetime, end: datetime, nights: int, dsn: str) -> \
list[TaxOrFee]:
    with connect(dsn) as commands:
        return commands.query(tax_or_fee_base_query(table_prefix),
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                                  "nights": nights
                              }, model=TaxOrFee)


def get_taxes(external_id: str, start: datetime, end: datetime, nights: int, dsn: str) -> list[TaxOrFee]:
    return get_taxes_or_fees("Tax", external_id, start, end, nights, dsn)


def get_fees(external_id: str, start: datetime, end: datetime, nights: int, dsn: str) -> list[TaxOrFee]:
    return get_taxes_or_fees("Fee", external_id, start, end, nights, dsn)
