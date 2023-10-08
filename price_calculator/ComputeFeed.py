import decimal
from dataclasses import dataclass, field
from datetime import datetime
import pendulum
from price_calculator.Rates import get_rates
from price_calculator.TaxesOrFees import get_taxes, get_fees


@dataclass
class ChargeDetails:
    start_date: datetime
    end_date: datetime
    duration: int = 0
    rent: list[decimal] = field(default_factory=list)
    taxes: list[decimal] = field(default_factory=list)
    fees: list[decimal] = field(default_factory=list)
    number_adults: int = 2
    number_children: int = 0


def compute_feed_price(external_id, start: str, end: str, dsn: str) -> ChargeDetails:
    start_date = pendulum.parse(start)
    end_date = pendulum.parse(end)
    duration = pendulum.period(start_date, end_date)
    print(f'Nights: {duration.days}')
    rate_records = get_rates(external_id, start_date, end_date, dsn)
    tax_records = get_taxes(external_id, start_date, end_date, duration.days, dsn)
    fee_records = get_fees(external_id, start_date, end_date, duration.days, dsn)

    return ChargeDetails(start_date, end_date)
