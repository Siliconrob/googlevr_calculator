import decimal
from dataclasses import dataclass
from glom import glom
from pydapper import connect
from data_messages import DateRange, LengthOfStay


@dataclass
class Tax:
    externalId: str
    bookingDates: list[DateRange]
    checkinDates: list[DateRange]
    checkoutDates: list[DateRange]
    lengthOfStay: LengthOfStay
    period: str
    currency: str
    amount: decimal


@dataclass
class Fee:
    externalId: str
    bookingDates: list[DateRange]
    checkinDates: list[DateRange]
    checkoutDates: list[DateRange]
    lengthOfStay: LengthOfStay
    period: str
    currency: str
    amount: decimal


def insert_records(file_input: dict, db_name: str) -> int:
    read_taxes(file_input)
    #load_taxes(read_taxes(file_input), db_name)
    #load_fees(read_fees(file_input), db_name)



def load_taxes(taxes: list[Tax], db_name: str) -> int:
    if len(taxes) == 0:
        return

    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists Tax_BookingDates (externalId varchar(20), promotionId varchar(100), start TEXT, end TEXT, PRIMARY KEY(externalId, promotionId, start, end))")
        commands.execute(
            f"create table if not exists Tax_CheckinDates (externalId varchar(20), promotionId varchar(100), start TEXT, end TEXT, PRIMARY KEY(externalId, promotionId, start, end))")
        commands.execute(
            f"create table if not exists Tax_CheckoutDates (externalId varchar(20), promotionId varchar(100), start TEXT, end TEXT, PRIMARY KEY(externalId, promotionId, start, end))")
        commands.execute(
            f"create table if not exists Tax_LengthOfStay (externalId varchar(20), promotionId varchar(100), min int, max int, PRIMARY KEY(externalId, promotionId))")
        commands.execute(
            f"create table if not exists Tax (externalId varchar(20), promotionId varchar(100), percentage DECIMAL(18,6), fixedAmount DECIMAL(18,6), fixedAmountPerNight DECIMAL(18,6), fixedPrice DECIMAL(18,6), PRIMARY KEY(externalId, promotionId))")

        rowcounts = {}

        for tax in taxes:
            if len(tax.bookingDates) > 0:
                rowcounts['bookingDates'] = commands.execute(
                    f"INSERT INTO Promotion_BookingDates (externalId, promotionId, start, end) values (?externalId?, ?promotionId?, ?start?, ?end?) ON CONFLICT (externalId, promotionId, start, end) DO NOTHING",
                    param=[{
                        "externalId": promotion.externalId,
                        "promotionId": promotion.id,
                        "start": None if dateRange.start is None else dateRange.start.isoformat(),
                        "end": None if dateRange.end is None else dateRange.end.isoformat()
                    } for dateRange in promotion.bookingDates]),
            if len(tax.checkinDates) > 0:
                rowcounts['checkinDates'] = commands.execute(
                    f"INSERT INTO Promotion_CheckinDates (externalId, promotionId, start, end) values (?externalId?, ?promotionId?, ?start?, ?end?) ON CONFLICT (externalId, promotionId, start, end) DO NOTHING",
                    param=[{
                        "externalId": promotion.externalId,
                        "promotionId": promotion.id,
                        "start": None if dateRange.start is None else dateRange.start.isoformat(),
                        "end": None if dateRange.end is None else dateRange.end.isoformat()
                    } for dateRange in promotion.checkinDates]),
            if len(tax.checkoutDates) > 0:
                rowcounts['checkoutDates'] = commands.execute(
                    f"INSERT INTO Promotion_CheckoutDates (externalId, promotionId, start, end) values (?externalId?, ?promotionId?, ?start?, ?end?) ON CONFLICT (externalId, promotionId, start, end) DO NOTHING",
                    param=[{
                        "externalId": promotion.externalId,
                        "promotionId": promotion.id,
                        "start": None if dateRange.start is None else dateRange.start.isoformat(),
                        "end": None if dateRange.end is None else dateRange.end.isoformat()
                    } for dateRange in promotion.checkoutDates]),
            if tax.lengthOfStay is not None:
                rowcounts['lengthOfStay'] = commands.execute(
                    f"INSERT INTO Promotion_LengthOfStay (externalId, promotionId, min, max) values (?externalId?, ?promotionId?, ?min?, ?max?) ON CONFLICT (externalId, promotionId) DO NOTHING",
                    param={
                        "externalId": promotion.externalId,
                        "promotionId": promotion.id,
                        "min": None if promotion.lengthOfStay.min is None else promotion.lengthOfStay.min,
                        "max": None if promotion.lengthOfStay.max is None else promotion.lengthOfStay.max
                    }),

            rowcounts['tax'] = commands.execute(
                f"INSERT INTO Promotion (externalId, promotionId, percentage, fixedAmount, fixedAmountPerNight, fixedPrice) values (?externalId?, ?promotionId?, ?percentage?, ?fixedAmount?, ?fixedAmountPerNight?, ?fixedPrice?) ON CONFLICT (externalId, promotionId)  DO UPDATE SET percentage = ?percentage?, fixedAmount = ?fixedAmount?, fixedAmountPerNight = ?fixedAmountPerNight?, fixedPrice = ?fixedPrice?",
                param={
                    "externalId": promotion.externalId,
                    "promotionId": promotion.id,
                    "percentage": None if promotion.percentage is None else float(promotion.percentage),
                    "fixedAmount": None if promotion.fixed_amount is None else float(promotion.fixed_amount),
                    "fixedAmountPerNight": None if promotion.fixed_amount_per_night is None else float(
                        promotion.fixed_amount_per_night),
                    "fixedPrice": None if promotion.fixed_price is None else float(promotion.fixed_price),
                })
        return rowcounts


def read_taxes(file_input: dict) -> list[Tax]:
    if len(file_input.keys()) > 1 or glom(file_input, 'Promotions', default=None) is None:
        return []

    external_id = glom(file_input, 'Promotions.HotelPromotions.@hotel_id')
    promotions = []
    for promotion in glom(file_input, '**.Promotion').pop():
        discount = glom(promotion, 'Discount', default=None)
        if discount is None:
            continue
        stacks = glom(promotion, 'Stacking')
        stacking_type = stacks.get("@type") if stacks is not None else None

        promotions.append(Promotion(external_id,
                                    promotion.get("@id"),
                                    DateRange.parse_ranges(glom(promotion, 'BookingDates.DateRange', default=[])),
                                    DateRange.parse_ranges(glom(promotion, 'CheckinDates.DateRange', default=[])),
                                    DateRange.parse_ranges(glom(promotion, 'CheckoutDates.DateRange', default=[])),
                                    LengthOfStay.parse_range(glom(promotion, 'LengthOfStay', default=None)),
                                    discount.get("@percentage"),
                                    discount.get("@fixed_amount"),
                                    discount.get("@fixed_amount_per_night"),
                                    discount.get("@fixed_price"),
                                    stacking_type))
    return promotions
