import datetime
from dataclasses import dataclass

import pendulum
from glom import glom
from pydapper import connect
from data_messages.LengthOfStay import TimeLengthOfStay


@dataclass
class OTAHotelAvailNotifRQ:
    externalId: str
    start: datetime.date
    end: datetime.date
    minLengthOfStay: int
    maxLengthOfStay: int
    available: bool


def insert_records(file_input: dict, db_name: str) -> int:
    return load_availability(read_availability(file_input), db_name)


def load_availability(availabilities: list[OTAHotelAvailNotifRQ], db_name: str) -> int:
    if len(availabilities) == 0:
        return

    with connect(db_name) as commands:
        commands.execute(
            f"create table if not exists OTAHotelAvailNotifRQ (externalId varchar(20), start TEXT, end TEXT, minLengthOfStay int, maxLengthOfStay int, available bool, PRIMARY KEY(externalId, start, end))")
        rowcount = commands.execute(
            f"INSERT INTO OTAHotelAvailNotifRQ (externalId, start, end, minLengthOfStay, maxLengthOfStay, available) values (?externalId?, ?start?, ?end?, ?minLengthOfStay?, ?maxLengthOfStay?, ?available?) ON CONFLICT (externalId, start, end) DO UPDATE SET minLengthOfStay = ?minLengthOfStay?, maxLengthOfStay = ?maxLengthOfStay?, available = ?available?",
            param=[{
                "externalId": availability.externalId,
                "start": availability.start.isoformat(),
                "end": availability.end.isoformat(),
                "minLengthOfStay": availability.minLengthOfStay,
                "maxLengthOfStay": availability.maxLengthOfStay,
                "available": availability.available,
            } for availability in availabilities],
        )
        return rowcount


def read_availability(file_input: dict) -> list[OTAHotelAvailNotifRQ]:
    if len(file_input.keys()) > 1 or glom(file_input, 'OTA_HotelAvailNotifRQ', default=None) is None:
        return []

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

        parsed_min_time = int(min_time["@Time"]) if min_time is not None else None
        parsed_max_time = int(max_time["@Time"]) if max_time is not None else None

        restriction = glom(avail_status_message, 'RestrictionStatus', default=None)
        status = True if restriction["@Status"] == 'Open' else False
        start, end = pendulum.parse(status_application_control["@Start"]), pendulum.parse(status_application_control["@End"])
        availabilities.append(OTAHotelAvailNotifRQ(external_id, start, end, parsed_min_time, parsed_max_time, status))
    return availabilities

