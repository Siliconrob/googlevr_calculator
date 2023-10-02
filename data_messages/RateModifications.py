import decimal
from dataclasses import dataclass, field
from decimal import Decimal
from glom import glom
from pydapper import connect
from data_messages import DateRange, LengthOfStay


@dataclass
class RateModifications:
    @property
    def internal_id(self) -> int:
        return int(self.externalId.replace("orp5b", "").replace("x", ""), 16)

    externalId: str
    bookingDates: list[DateRange]
    checkinDates: list[DateRange]
    checkoutDates: list[DateRange]
    priceAdjustment: decimal
    lengthOfStay: LengthOfStay
    internalId: int = field(init=False, default=internal_id)


def insert_records(file_input: dict, db_name: str) -> int:
    return load_rate_modifications(read_rate_modifications(file_input), db_name)


def load_rate_modifications(ratemodifier: RateModifications, db_name: str) -> int:
    if ratemodifier is None:
        return

    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists RateModifications_BookingDates (externalId varchar(20), start TEXT, end TEXT, PRIMARY KEY(externalId, start, end))")
        commands.execute(
            f"create table if not exists RateModifications_CheckinDates (externalId varchar(20), start TEXT, end TEXT, PRIMARY KEY(externalId, start, end))")
        commands.execute(
            f"create table if not exists RateModifications_CheckoutDates (externalId varchar(20), start TEXT, end TEXT, PRIMARY KEY(externalId, start, end))")
        commands.execute(
            f"create table if not exists RateModifications_LengthOfStay (externalId varchar(20), min int, max int, PRIMARY KEY(externalId, min, max))")
        commands.execute(
            f"create table if not exists RateModifications (externalId varchar(20), multiplier DECIMAL(18,6), PRIMARY KEY(externalId, multiplier))")

        rowcounts = {}
        if len(ratemodifier.bookingDates) > 0:
            rowcounts['bookingDates'] = commands.execute(
                f"INSERT INTO RateModifications_BookingDates (externalId, start, end) values (?externalId?, ?start?, ?end?) ON CONFLICT (externalId, start, end) DO NOTHING",
                param=[{
                    "externalId": ratemodifier.externalId,
                    "start": dateRange.start.isoformat(),
                    "end": dateRange.end.isoformat()
                } for dateRange in ratemodifier.bookingDates]),
        if len(ratemodifier.checkinDates) > 0:
            rowcounts['checkinDates'] = commands.execute(
                f"INSERT INTO RateModifications_CheckinDates (externalId, start, end) values (?externalId?, ?start?, ?end?) ON CONFLICT (externalId, start, end) DO NOTHING",
                param=[{
                    "externalId": ratemodifier.externalId,
                    "start": dateRange.start.isoformat(),
                    "end": dateRange.end.isoformat()
                } for dateRange in ratemodifier.checkinDates]),
        if len(ratemodifier.checkoutDates) > 0:
            rowcounts['checkoutDates'] = commands.execute(
                f"INSERT INTO RateModifications_CheckoutDates (externalId, start, end) values (?externalId?, ?start?, ?end?) ON CONFLICT (externalId, start, end) DO NOTHING",
                param=[{
                    "externalId": ratemodifier.externalId,
                    "start": dateRange.start.isoformat(),
                    "end": dateRange.end.isoformat()
                } for dateRange in ratemodifier.checkoutDates]),
        if ratemodifier.lengthOfStay is not None:
            rowcounts['lengthOfStay'] = commands.execute(
                f"INSERT INTO RateModifications_LengthOfStay (externalId, min, max) values (?externalId?, ?min?, ?max?) ON CONFLICT (externalId, min, max) DO NOTHING",
                param={
                    "externalId": ratemodifier.externalId,
                    "min": ratemodifier.lengthOfStay.min,
                    "max": ratemodifier.lengthOfStay.max
                }),

        rowcounts['ratemodifiers'] = commands.execute(
            f"INSERT INTO RateModifications (externalId, multiplier) values (?externalId?, ?multiplier?) ON CONFLICT (externalId, multiplier) DO NOTHING",
            param={
                "externalId": ratemodifier.externalId,
                "multiplier": float(ratemodifier.priceAdjustment)
            }),
    return rowcounts


def read_rate_modifications(file_input: dict) -> RateModifications:
    if len(file_input.keys()) > 1 or glom(file_input, 'RateModifications', default=None) is None:
        return None

    external_id = glom(file_input, 'RateModifications.HotelRateModifications.@hotel_id')
    itinerary = glom(file_input, '**.ItineraryRateModification').pop()
    multiplier = glom(itinerary, 'ModificationActions.PriceAdjustment', default=None)
    if multiplier is None:
        return None
    booking_dates = DateRange.parse_ranges(glom(itinerary, 'BookingDates.DateRange', default=[]))
    checkin_dates = DateRange.parse_ranges(glom(itinerary, 'CheckinDates.DateRange', default=[]))
    checkout_dates = DateRange.parse_ranges(glom(itinerary, 'CheckoutDates.DateRange', default=[]))
    stay_requires = LengthOfStay.parse_range(glom(itinerary, 'LengthOfStay', default=None))
    new_modifier = RateModifications(external_id, booking_dates, checkin_dates, checkout_dates,
                                     Decimal(multiplier["@multiplier"]), stay_requires)

    return new_modifier
