import dataclasses
import decimal
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
import pendulum

from price_calculator.Inventory import get_inventory, Inventory
from price_calculator.ExtraGuestCharges import get_extra_guest_charges, ExtraGuestCharge
from price_calculator.Promotions import get_promotions, Promotion
from price_calculator.RateModifiers import get_rate_modifiers, RateModifier
from price_calculator.Rates import get_rates, Rate
from price_calculator.TaxesOrFees import get_taxes, get_fees, TaxOrFee
from icecream import ic

ic.configureOutput(prefix='|> ')


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
    inventories: list[Inventory] = field(default_factory=list)
    number_adults: int = 2
    number_children: int = 0


@dataclass
class FeedPrice:
    total: decimal = 0
    rent: decimal = 0
    promotions: decimal = 0
    rate_modifiers: decimal = 0
    taxes_and_fees: decimal = 0
    details: ChargeDetails = None

    def to_dict(self):
        return dataclasses.asdict(self)


def total_base_rent(charges: ChargeDetails) -> decimal:
    total = 0
    for rent_record in charges.rent:
        total += ic(rent_record.base_amount)
    return decimal.Decimal(total)


def promotions_adjustment(rent_total: decimal, nights: int, charges: ChargeDetails) -> decimal:
    total = 0
    for promotion in charges.promotions:
        percent = None if promotion.percentage is None else Decimal(promotion.percentage)
        if percent is not None:
            total += ic(rent_total * (percent / 100))
            continue
        fixed_amount = None if promotion.fixed_amount is None else Decimal(promotion.fixed_amount)
        if fixed_amount is not None:
            total += ic(fixed_amount)
            continue
        fixed_amount_per_night = None if promotion.fixed_amount_per_night is None else Decimal(
            promotion.fixed_amount_per_night)
        if fixed_amount_per_night is not None:
            total += ic(fixed_amount_per_night * nights)
            continue
    return decimal.Decimal(total)


def rate_modifiers_adjustment(rent_total: decimal, charges: ChargeDetails) -> decimal:
    total = 0
    for rate_modifier in charges.rate_modifiers:
        percent = None if rate_modifier.multiplier is None else Decimal(rate_modifier.multiplier)
        if percent is not None:
            total += ic(rent_total * (1 - percent))
            continue
    return decimal.Decimal(total)


def taxes_and_fees(rent_amount: decimal, charges: ChargeDetails) -> decimal:
    fees = ic(tax_or_fee_total(charges.fees, charges.nights, rent_amount))
    taxes = ic(tax_or_fee_total(charges.taxes, charges.nights, rent_amount))
    total = ic(fees + taxes)
    return decimal.Decimal(total)


def tax_or_fee_total(tax_or_fees: list[TaxOrFee], nights: int, rent_amount: decimal) -> decimal:
    total = 0
    for current_item in tax_or_fees:
        amount = Decimal(current_item.amount)
        if current_item.period == "night":
            if current_item.calc_type == "amount":
                total += ic(nights * amount)
            continue
        if current_item.calc_type == "amount":
            total += ic(amount)
            continue
        total += ic((amount / 100) * rent_amount)
    return decimal.Decimal(total)


def compute_feed_price(external_id, start_date: date, end_date: date, book_date: date, dsn: str) -> FeedPrice:
    start_date = ic(pendulum.datetime(start_date.year, start_date.month, start_date.day))
    end_date = ic(pendulum.datetime(end_date.year, end_date.month, end_date.day))

    if start_date > end_date:
        raise Exception(ic(f'{start_date} is greater than {end_date}'))

    book_date = ic(pendulum.datetime(book_date.year, book_date.month, book_date.day))
    duration = ic(end_date.diff(start_date))

    details = ic(ChargeDetails(
        start_date,
        end_date,
        book_date,
        duration.days,
        get_rates(external_id, start_date, end_date, dsn),
        get_taxes(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_fees(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_promotions(external_id, start_date, end_date, duration.in_days(), book_date, dsn),
        get_extra_guest_charges(external_id, start_date, end_date, dsn),
        get_rate_modifiers(external_id, start_date, end_date, duration.days, book_date, dsn),
        get_inventory(external_id, start_date, end_date, dsn)
    ))

    total_rent = total_base_rent(details)
    ic(f'Total Base Rent: {total_rent}')

    total_rate_modifiers = rate_modifiers_adjustment(total_rent, details)
    ic(f'Rate Modifiers: {total_rate_modifiers}')
    current_amount = ic(total_rent - total_rate_modifiers)

    total_promotions = promotions_adjustment(current_amount, duration.days, details)
    ic(f'Promotions: {total_promotions}')
    current_amount = ic(current_amount - total_promotions)

    total_taxes_fees = ic(taxes_and_fees(current_amount, details))
    ic(f'Taxes And Fees: {total_taxes_fees}')

    total = current_amount + total_taxes_fees
    ic(f'Total: {total}')
    return FeedPrice(total, total_rent, total_promotions, total_rate_modifiers, total_taxes_fees, details)
