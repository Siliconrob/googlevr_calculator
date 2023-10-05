import datetime
from dataclasses import dataclass
import pendulum
from pydapper import connect


@dataclass
class FileInfo:
    name: str = None
    externalId: str = None
    timestamp: datetime.datetime = None
    records: int = 0

def get_timestamp(input: str) -> datetime.datetime:
    return None if input is None else pendulum.parse(input)


def load_file(file_name: str, db_name: str) -> int:
    with connect(db_name) as commands:
        commands.execute(f"""
            create table if not exists FileInfo (
            id INTEGER UNIQUE,
            fileName varchar(200) UNIQUE, 
            externalId varchar(20),
            timestamp TEXT,
            records int,
            PRIMARY KEY(id AUTOINCREMENT))
            """)
        commands.execute(f"delete from FileInfo where fileName = ?fileName?", param={"fileName": file_name})
        commands.execute(f"""
            insert into FileInfo (fileName, records)
            values (?fileName?, 0)
            ON CONFLICT (fileName) DO NOTHING""",
            param={"fileName": file_name}
        )
        last_id = commands.query_single(f"select seq from sqlite_sequence WHERE name = ?table_name?",
                                        param={"table_name": "FileInfo"})
        return last_id["seq"]


def update_file(info: FileInfo, db_name: str):
    with connect(db_name) as commands:
        commands.execute(f"""
            update FileInfo
            set externalId = ?externalId?,
                timestamp = ?timestamp?,
                records = ?records?
            WHERE fileName = ?fileName?
            """,
            param={
                "fileName": info.name,
                "externalId": info.externalId,
                "timestamp": None if info.timestamp is None else info.timestamp.isoformat(),
                "records": info.records
            }
        )