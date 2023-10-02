import decimal
from dataclasses import dataclass, field
from datetime import datetime
import pendulum
from decimal import Decimal
from glom import glom
from pydapper import connect, connect_async


@dataclass
class OTAHotelRateAmountNotifRQ:
    @property
    def internal_id(self) -> int:
        return int(self.externalId.replace("orp5b", "").replace("x", ""), 16)

    externalId: str
    start: datetime.date
    end: datetime.date
    baseAmount: decimal
    guestCount: int
    internalId: int = field(init=False, default=internal_id)


def insert_records(file_input: dict, db_name: str) -> int:
    return load_rates(read_rates(file_input), db_name)


def load_rates(rates: list[OTAHotelRateAmountNotifRQ], db_name: str) -> int:
    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists OTAHotelRateAmountNotifRQ (internalId INT, externalId varchar(20), start TEXT, end TEXT, baseAmount DECIMAL(18,6), guestCount int, PRIMARY KEY(internalId, start, end))")
        rowcount = commands.execute(
            f"INSERT INTO OTAHotelRateAmountNotifRQ (internalId, externalId, start, end, baseAmount, guestCount) values (?internalId?, ?externalId?, ?start?, ?end?, ?baseAmount?, ?guestCount?) ON CONFLICT (internalId, start, end) DO UPDATE SET baseAmount = ?baseAmount?, guestCount = ?guestCount?",
            param=[{
                "internalId": rate.internalId,
                "externalId": rate.externalId,
                "start": rate.start.isoformat(),
                "end": rate.end.isoformat(),
                "baseAmount": float(rate.baseAmount),
                "guestCount": rate.guestCount
            } for rate in rates],
        )
        return rowcount


def read_rates(file_input: dict) -> list[OTAHotelRateAmountNotifRQ]:
    if len(file_input.keys()) > 1 or glom(file_input, 'OTA_HotelRateAmountNotifRQ', default=None) is None:
        return None

    external_id = glom(file_input, 'OTA_HotelRateAmountNotifRQ.RateAmountMessages.@HotelCode')
    rates = []
    for rate_amount_message in glom(file_input, '**.RateAmountMessage').pop():
        base_by_amount = glom(rate_amount_message, 'Rates.Rate.BaseByGuestAmts.BaseByGuestAmt', default=None)
        status_application_control = glom(rate_amount_message, 'StatusApplicationControl', default=None)
        if base_by_amount is None or status_application_control is None:
            continue
        start, end = pendulum.parse(status_application_control["@Start"]), pendulum.parse(status_application_control["@End"])
        new_rate = OTAHotelRateAmountNotifRQ(external_id, start, end, Decimal(base_by_amount["@AmountBeforeTax"]), int(base_by_amount["@NumberOfGuests"]))
        rates.append(new_rate)
    return rates
