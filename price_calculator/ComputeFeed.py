import decimal
from dataclasses import dataclass, field
from datetime import datetime
import pendulum

from price_calculator.ExtraGuestCharges import get_extra_guest_charges, ExtraGuestCharge
from price_calculator.Promotions import get_promotions, Promotion
from price_calculator.RateModifiers import get_rate_modifiers, RateModifier
from price_calculator.Rates import get_rates, Rate
from price_calculator.TaxesOrFees import get_taxes, get_fees, TaxOrFee


@dataclass
class ChargeDetails:
    start_date: datetime
    end_date: datetime
    book_date: datetime
    duration: int = 0
    rent: list[Rate] = field(default_factory=list)
    taxes: list[TaxOrFee] = field(default_factory=list)
    fees: list[TaxOrFee] = field(default_factory=list)
    promotions: list[Promotion] = field(default_factory=list)
    extra_guest_charges: list[ExtraGuestCharge] = field(default_factory=list)
    rate_modifiers: list[RateModifier] = field(default_factory=list)
    number_adults: int = 2
    number_children: int = 0


def total_base_rent(charges: ChargeDetails) -> decimal:
    total_base_rent = 0
    current_day = charges.start_date




    # for rent_rate in rent_records:


    return total_base_rent

def compute_feed_price(external_id, start_date_text: str, end_date_text: str, book_date_text: str, dsn: str) -> ChargeDetails:
    start_date = pendulum.parse(start_date_text)
    end_date = pendulum.parse(end_date_text)
    book_date = pendulum.parse(book_date_text)
    duration = pendulum.period(start_date, end_date)

    details = ChargeDetails(
        start_date,
        end_date,
        book_date,
        duration,
        get_rates(external_id, start_date, end_date, dsn),
        get_taxes(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_fees(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_promotions(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_extra_guest_charges(external_id, start_date, end_date, dsn),
        get_rate_modifiers(external_id, start_date, end_date, duration.days, book_date, dsn)
    )

    print(f'Total: {total_base_rent(details)}')

    return details
