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


def get_safe_list(input_list: list):
    if len(input_list) == 0:
        return input_list
    input_list = input_list.pop()
    if isinstance(input_list, dict):
        input_list = [input_list]
    return input_list
