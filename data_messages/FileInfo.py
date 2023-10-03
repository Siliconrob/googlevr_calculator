import datetime
from dataclasses import dataclass
import pendulum


@dataclass
class FileInfo:
    name: str
    size: int
    created: datetime.datetime
    timestamp: datetime.datetime = None
    records: int = 0

    def set_timestamp(self, input: str):
        self.timestamp = pendulum.parse(input)
