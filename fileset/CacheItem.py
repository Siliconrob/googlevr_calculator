import datetime
from dataclasses import dataclass


@dataclass
class CacheItem:
    cache_key: str = None
    timestamp: datetime.datetime = None
    contents: bytes = None
