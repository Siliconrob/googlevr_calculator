import datetime
from dataclasses import dataclass, field

from data_messages import LandingPages
from data_messages.Listings import Listing
from data_messages.OTA_HotelInvCountNotifRQ import OTAHotelInvCountNotifRQ


@dataclass
class ExtractedFiles:
    Inventory: list[OTAHotelInvCountNotifRQ] = field(default_factory=list)
    Listing: list[Listing] = field(default_factory=list)
    LandingPage: list[LandingPages] = field(default_factory=list)
