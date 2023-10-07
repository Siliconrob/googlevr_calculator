from dataclasses import dataclass
from datetime import datetime
import pendulum


@dataclass
class DataFileArgs:
    formatted_data: dict
    file_name: str
    dsn: str
    file_contents: str
    created: datetime = pendulum.UTC
