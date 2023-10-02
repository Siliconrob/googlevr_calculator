from dataclasses import dataclass
from glom import glom
from pydapper import connect
from data_messages.LengthOfStay import TimeLengthOfStay


@dataclass
class OTAHotelAvailNotifRQ:
    externalId: str
    minLengthOfStay: TimeLengthOfStay
    maxLengthOfStay: TimeLengthOfStay
    available: bool


def insert_records(file_input: dict, db_name: str) -> int:
    return load_availability(read_availability(file_input), db_name)


def load_availability(rates: list[OTAHotelAvailNotifRQ], db_name: str) -> int:
    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists OTAHotelAvailNotifRQ (externalId varchar(20), start TEXT, end TEXT, baseAmount DECIMAL(18,6), guestCount int, PRIMARY KEY(internalId, start, end))")
        rowcount = commands.execute(
            f"INSERT INTO OTAHotelAvailNotifRQ (internalId, externalId, start, end, baseAmount, guestCount) values (?internalId?, ?externalId?, ?start?, ?end?, ?baseAmount?, ?guestCount?) ON CONFLICT (internalId, start, end) DO UPDATE SET baseAmount = ?baseAmount?, guestCount = ?guestCount?",
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


def read_availability(file_input: dict) -> list[OTAHotelAvailNotifRQ]:
    if len(file_input.keys()) > 1 or glom(file_input, 'OTA_HotelAvailNotifRQ', default=None) is None:
        return None

    external_id = glom(file_input, 'OTA_HotelAvailNotifRQ.AvailStatusMessages.@HotelCode')
    availabilities = []
    for avail_status_message in glom(file_input, '**.AvailStatusMessage').pop():
        status_application_control = glom(avail_status_message, 'StatusApplicationControl', default=None)
        if status_application_control is None:
            continue
        lengths_of_stay = glom(avail_status_message, 'LengthsOfStay.LengthOfStay', default=None)
        min_time_stay = glom(lengths_of_stay, lambda z: [i for i in z if i["@MinMaxMessageType"] == 'SetMinLOS'], default=[])
        max_time_stay = glom(lengths_of_stay, lambda z: [i for i in z if i["@MinMaxMessageType"] == 'SetMaxLOS'], default=[])
        min_time = min_time_stay.pop() if len(min_time_stay) > 0 else None
        max_time = max_time_stay.pop() if len(max_time_stay) > 0 else None

        parsed_min_time = TimeLengthOfStay(int(min_time["@Time"]), 'SetMinLOS') if min_time is not None else None
        parsed_max_time = TimeLengthOfStay(int(max_time["@Time"]), 'SetMaxLOS') if max_time is not None else None

        restriction = glom(avail_status_message, 'RestrictionStatus', default=None)
        status = True if restriction["@Status"] == 'Open' else False

        availabilities.append(OTAHotelAvailNotifRQ(external_id, parsed_min_time, parsed_max_time, status))
    return availabilities

