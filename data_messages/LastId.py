from dataclasses import dataclass
from pydapper import connect


@dataclass
class LastId:
    seq: int = 0


def get_last_inserted_id(db_name: str, table_name: str) -> int:
    with connect(db_name) as commands:
        last_id = commands.query_first_or_default(f"""
            select seq
            from sqlite_sequence
            WHERE name = ?table_name?
            """,
            param={"table_name": table_name}, model=LastId, default=LastId())
        return last_id.seq
