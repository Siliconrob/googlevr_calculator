import decimal
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class RateModifications:
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
        return "hotel_id"

    @staticmethod
    def hotel_rate_modifications_key() -> str:
        return "HotelRateModifications"

    @staticmethod
    def itinerary_rate_modification_key() -> str:
        return "ItineraryRateModification"

    @staticmethod
    def data_type_key() -> str:
        return "RateModifications"