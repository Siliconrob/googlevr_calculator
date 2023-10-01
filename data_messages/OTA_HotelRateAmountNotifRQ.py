import decimal
from dataclasses import dataclass, field
from datetime import datetime

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

    @staticmethod
    def hotel_id_key() -> str:
        return "@HotelCode"

    @staticmethod
    def rate_amount_messages_key() -> str:
        return "RateAmountMessages"

    @staticmethod
    def rate_amount_message_key() -> str:
        return "RateAmountMessage"

    @staticmethod
    def data_type_key() -> str:
        return "OTA_HotelRateAmountNotifRQ"