from dataclasses import dataclass
from datetime import datetime

import pendulum
from glom import glom
from pydapper import connect

from data_messages import FileInfo
from fileset import DataHandlers
from fileset.DataHandlers import get_safe_list


@dataclass
class OTAHotelInvCountNotifRQ:
    external_id: str
    start: datetime.date
    end: datetime.date
    inventory: int


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    create_tables(file_args.dsn)
    rates, file_info = read_inventories(file_args)
    return load_inventories(rates, file_info, file_args)


def create_tables(dsn: str):
    with connect(dsn) as commands:
        commands.execute(f"""
            create table if not exists OTAHotelInvCountNotifRQ
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id varchar(20),
                start TEXT,
                end TEXT,
                inventory int,
                file_id int,
                FOREIGN KEY (file_id) REFERENCES FileInfo(id) ON DELETE CASCADE,
                UNIQUE(external_id, file_id, start, end)
            )""")


def load_inventories(inventories: list[OTAHotelInvCountNotifRQ],
                     file_info: FileInfo.FileInfo,
                     file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    if len(inventories) == 0:
        return None

    new_id = FileInfo.load_file(file_info.file_name, file_args.dsn)
    rowcount = {}
    with connect(file_args.dsn) as commands:
        commands.execute(f"""
            delete from OTAHotelInvCountNotifRQ
            where file_id != ?file_id?
            """,
            param={"file_id": new_id})
        rowcount["inventories"] = commands.execute(f"""
            INSERT INTO OTAHotelInvCountNotifRQ
            (
                external_id,
                file_id,
                start,
                end,
                inventory
            )
            values
            (
                ?external_id?,
                ?file_id?,
                ?start?,
                ?end?,
                ?inventory? 
            )
            ON CONFLICT (external_id, file_id, start, end)
            DO UPDATE SET inventory = ?inventory?
            """,
            param=[{
                "external_id": inventory.external_id,
                "file_id": new_id,
                "start": inventory.start.isoformat(),
                "end": inventory.end.isoformat(),
                "inventory": inventory.inventory
            } for inventory in inventories],
        )
    file_info.records = len(inventories)
    file_info.xml_contents = file_args.file_contents
    return FileInfo.update_file(file_info, file_args.dsn)


def read_inventories(file_args: DataHandlers.DataFileArgs) -> (list[OTAHotelInvCountNotifRQ], FileInfo.FileInfo):
    if (len(file_args.formatted_data.keys()) > 1
            or glom(file_args.formatted_data, 'OTA_HotelInvCountNotifRQ', default=None) is None):
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    results.timestamp = FileInfo.get_timestamp(glom(file_args.formatted_data,'OTA_HotelInvCountNotifRQ.@TimeStamp'))
    results.external_id = glom(file_args.formatted_data, 'OTA_HotelInvCountNotifRQ.Inventories.@HotelCode')
    new_inventories = []
    file_inventories = glom(file_args.formatted_data, '**.Inventory')
    if len(file_inventories) == 0:
        return [], None

    for inventory_message in get_safe_list(file_inventories):
        inventory_count = glom(inventory_message, 'InvCounts.InvCount', default=None)
        status_application_control = glom(inventory_message, 'StatusApplicationControl', default=None)
        if inventory_count is None or status_application_control is None:
            continue
        start, end = pendulum.parse(status_application_control["@Start"]), pendulum.parse(
            status_application_control["@End"])
        new_inventory = OTAHotelInvCountNotifRQ(results.external_id,
                                             start,
                                             end,
                                             int(inventory_count.get("@Count", 1)))
        new_inventories.append(new_inventory)
    return new_inventories, results
