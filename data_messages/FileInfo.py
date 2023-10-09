import datetime
from dataclasses import dataclass

import pendulum
from pydapper import connect

from data_messages.LastId import get_last_inserted_id


@dataclass
class FileInfo:
    file_name: str = None
    external_id: str = None
    timestamp: datetime.datetime = None
    records: int = 0
    id: int = 0
    xml_contents: str = None


def get_timestamp(input: str) -> datetime.datetime:
    return None if input is None else pendulum.parse(input)


def load_file(file_name: str, db_name: str) -> int:
    with connect(db_name) as commands:
        commands.execute(f"""
            create table if not exists FileInfo
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name varchar(200) UNIQUE, 
                external_id varchar(20),
                timestamp TEXT,
                records int,
                xml_contents TEXT
            )
            """)
        commands.execute(f"delete from FileInfo where file_name = ?file_name?", param={"file_name": file_name})
        commands.execute(f"""
            insert into FileInfo (file_name, records)
            values (?file_name?, 0)
            ON CONFLICT (file_name)
            DO NOTHING
            """,
            param={
                "file_name": file_name
            })

    new_id = get_last_inserted_id(db_name, "FileInfo")
    return new_id


def update_file(info: FileInfo, db_name: str):
    with connect(db_name) as commands:
        commands.execute(f"""
            update FileInfo
            set external_id = ?external_id?,
                timestamp = ?timestamp?,
                records = ?records?,
                xml_contents = ?xml_contents?
            WHERE file_name = ?file_name?
            """,
             param={
                 "file_name": info.file_name,
                 "external_id": info.external_id,
                 "timestamp": None if info.timestamp is None else info.timestamp.isoformat(),
                 "records": info.records,
                 "xml_contents": info.xml_contents
             })
        return commands.query_first_or_default("""
            SELECT *
            FROM FileInfo
            WHERE file_name = ?file_name?
            """,
            param={"file_name": info.file_name},
            model=FileInfo,
            default=FileInfo())
