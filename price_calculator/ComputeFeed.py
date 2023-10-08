import decimal
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
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
    nights: int = 0
    rent: list[Rate] = field(default_factory=list)
    taxes: list[TaxOrFee] = field(default_factory=list)
    fees: list[TaxOrFee] = field(default_factory=list)
    promotions: list[Promotion] = field(default_factory=list)
    extra_guest_charges: list[ExtraGuestCharge] = field(default_factory=list)
    rate_modifiers: list[RateModifier] = field(default_factory=list)
    number_adults: int = 2
    number_children: int = 0


def total_base_rent(charges: ChargeDetails) -> decimal:
    total = 0
    for current_day in pendulum.period(charges.start_date, charges.end_date.subtract(days=1)).range('days'):
        for rent_record in charges.rent:
            if current_day == pendulum.parse(rent_record.start):
                total += rent_record.base_amount
            continue
    return total


def promotions_adjustment(rent_total: decimal, charges: ChargeDetails) -> decimal:
    total = 0
    for promotion in charges.promotions:
        percent = None if promotion.percentage is None else Decimal(promotion.percentage)
        if percent is not None:
            total += rent_total * percent
            continue
        fixed_amount = None if promotion.fixed_amount is None else Decimal(promotion.fixed_amount)
        if fixed_amount is not None:
            total += fixed_amount
            continue
    return total


def taxes_and_fees(rent_amount: decimal, charges: ChargeDetails) -> decimal:
    fees = tax_or_fee_total(charges.fees, charges.nights, rent_amount)
    taxes = tax_or_fee_total(charges.taxes, charges.nights, rent_amount)
    total = fees + taxes
    return total


def tax_or_fee_total(tax_or_fees: list[TaxOrFee], nights: int, rent_amount: decimal) -> decimal:
    total = 0
    for current_item in tax_or_fees:
        amount = Decimal(current_item.amount)
        if current_item.period == "night":
            if current_item.calc_type == "amount":
                total += nights * amount
            continue
        if current_item.calc_type == "amount":
            total += amount
            continue
        total += (amount / 100) * rent_amount
    return total


def compute_feed_price(external_id, start_date_text: str, end_date_text: str, book_date_text: str, dsn: str) -> ChargeDetails:
    start_date = pendulum.parse(start_date_text)
    end_date = pendulum.parse(end_date_text)
    book_date = pendulum.parse(book_date_text)
    duration = pendulum.period(start_date, end_date)

    details = ChargeDetails(
        start_date,
        end_date,
        book_date,
        duration.days,
        get_rates(external_id, start_date, end_date, dsn),
        get_taxes(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_fees(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_promotions(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_extra_guest_charges(external_id, start_date, end_date, dsn),
        get_rate_modifiers(external_id, start_date, end_date, duration.days, book_date, dsn)
    )

    total_rent = total_base_rent(details)
    print(f'Total Base Rent: {total_rent}')
    total_promotions = promotions_adjustment(total_rent, details)
    print(f'Promotions: {total_promotions}')
    total_taxes_fees = taxes_and_fees(total_rent - total_promotions, details)
    print(f'Taxes And Fees: {total_taxes_fees}')
    total = total_rent + total_taxes_fees
    print(f'Total: {total}')
    return details
