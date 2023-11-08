import datetime
from dataclasses import dataclass

import pendulum
from icecream import ic
from pydapper import connect
from data_messages.LastId import get_last_inserted_id

ic.configureOutput(prefix='|> ')


@dataclass
class CacheItem:
    cache_key: str = None
    timestamp: datetime.datetime = None
    contents: bytes = None


def save(key: str, data: bytes, db_name: str) -> int:
    remove_expired(db_name)
    insert_datetime = ic(pendulum.now().utcnow())
    with connect(db_name) as commands:
        commands.execute(f"""
            insert into CacheStore (cache_key, contents, timestamp)
            values (?cache_key?, ?contents?, ?timestamp?)
            ON CONFLICT (cache_key)
            DO UPDATE SET contents = ?contents?,
                timestamp = ?timestamp?
            """,
            param={
                "cache_key": key,
                "contents": data,
                "timestamp": insert_datetime.isoformat()
            })

    new_id = get_last_inserted_id(db_name, "CacheStore")
    return new_id


def get(key: str, db_name: str) -> CacheItem:
    remove_expired(db_name)
    with connect(db_name) as commands:
        return commands.query_first_or_default("""
            SELECT cache_key, timestamp, contents
            FROM CacheStore
            WHERE cache_key = ?cache_key?
            """,
            param={"cache_key": key},
            model=CacheItem,
            default=None)


def remove_expired(db_name: str) -> None:
    expiration_datetime = ic(pendulum.now().utcnow().subtract(minutes=5))
    with connect(db_name) as commands:
        commands.execute(f"""
            create table if not exists CacheStore
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key varchar(20) UNIQUE, 
                timestamp TEXT,
                contents BLOB
            )
            """)
        commands.execute(f"delete from CacheStore where timestamp < ?expiration_datetime?",
        param={
            "expiration_datetime": expiration_datetime.isoformat()
        })
