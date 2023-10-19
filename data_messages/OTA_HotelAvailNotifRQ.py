import datetime
from dataclasses import dataclass

import pendulum
from glom import glom
from pydapper import connect

from data_messages import FileInfo, DataHandlers
from data_messages.DataHandlers import get_safe_list


@dataclass
class OTAHotelAvailNotifRQ:
    external_id: str
    start: datetime.date
    end: datetime.date
    min_length_of_stay: int
    max_length_of_stay: int
    available: bool


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    availabilities, file_info = read_availability(file_args)
    return load_availability(availabilities, file_info, file_args)


def create_tables(dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
        create table if not exists OTAHotelAvailNotifRQ (
            id INTEGER PRIMARY KEY AUTOINCREMENT,        
            external_id varchar(20),            
            start TEXT,
            end TEXT,
            min_length_of_stay int,
            max_length_of_stay int,
            available bool,
            file_id int,
            FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
            UNIQUE(external_id, file_id, start, end))
            """)


def load_availability(availabilities: list[OTAHotelAvailNotifRQ],
                      file_info: FileInfo.FileInfo,
                      file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if len(availabilities) == 0:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            delete from OTAHotelAvailNotifRQ
            where file_id != ?file_id?
            """,
            param={"file_id": new_id})
        rowcount = commands.execute(f"""
        INSERT INTO OTAHotelAvailNotifRQ
        (
            external_id,
            file_id,
            start,
            end,
            min_length_of_stay,
            max_length_of_stay,
            available
        )
        values
        (
            ?external_id?,
            ?file_id?,
            ?start?,
            ?end?,
            ?min_length_of_stay?,
            ?max_length_of_stay?,
            ?available?
        )
        ON CONFLICT (external_id, file_id, start, end)
        DO UPDATE SET min_length_of_stay = ?min_length_of_stay?,
            max_length_of_stay = ?max_length_of_stay?,
            available = ?available?
            """,
            param=[{
                "external_id": availability.external_id,
                "file_id": new_id,
                "start": availability.start.isoformat(),
                "end": availability.end.isoformat(),
                "min_length_of_stay": availability.min_length_of_stay,
                "max_length_of_stay": availability.max_length_of_stay,
                "available": availability.available,
            } for availability in availabilities],
        )
    file_info.records = len(availabilities)
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_availability(file_args: DataHandlers.DataFileArgs) -> (list[OTAHotelAvailNotifRQ], FileInfo.FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'OTA_HotelAvailNotifRQ', default=None) is None):
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data, 'OTA_HotelAvailNotifRQ.@TimeStamp'))
    results.external_id = glom(file_args.formatted_data, 'OTA_HotelAvailNotifRQ.AvailStatusMessages.@HotelCode')
    availabilities = []
    file_availabilities = glom(file_args.formatted_data, '**.AvailStatusMessage')
    if len(file_availabilities) == 0:
        return [], None

    for avail_status_message in get_safe_list(file_availabilities):
        status_application_control = glom(avail_status_message, 'StatusApplicationControl', default=None)
        if status_application_control is None:
            continue
        lengths_of_stay = glom(avail_status_message, 'LengthsOfStay.LengthOfStay', default=None)
        min_time_stay = glom(lengths_of_stay, lambda z: [i for i in z if i["@MinMaxMessageType"] == 'SetMinLOS'],
                             default=[])
        max_time_stay = glom(lengths_of_stay, lambda z: [i for i in z if i["@MinMaxMessageType"] == 'SetMaxLOS'],
                             default=[])
        min_time = min_time_stay.pop() if len(min_time_stay) > 0 else None
        max_time = max_time_stay.pop() if len(max_time_stay) > 0 else None

        parsed_min_time = int(min_time["@Time"]) if min_time is not None else None
        parsed_max_time = int(max_time["@Time"]) if max_time is not None else None

        restriction = glom(avail_status_message, 'RestrictionStatus', default=None)
        status = True if restriction["@Status"] == 'Open' else False
        start, end = (pendulum.parse(status_application_control["@Start"]),
                      pendulum.parse(status_application_control["@End"]))
        availabilities.append(OTAHotelAvailNotifRQ(results.external_id,
                                                   start,
                                                   end,
                                                   parsed_min_time,
                                                   parsed_max_time,
                                                   status))
    return availabilities, results
