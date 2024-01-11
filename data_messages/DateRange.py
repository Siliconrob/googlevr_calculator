import datetime
from dataclasses import dataclass

import pendulum


@dataclass
class DateRange:
    start: datetime.date
    end: datetime.date
    days_of_week: str


def parse_ranges(input: any) -> list[DateRange]:
    parsed = []
    to_parse_list = [input] if isinstance(input, dict) else input
    for current in to_parse_list:
        start = None if current.get("@start") is None else pendulum.parse(current["@start"])
        end = None if current.get("@end") is None else pendulum.parse(current["@end"])
        days_of_week = None if current.get("@days_of_week", "") == "" else current["@days_of_week"]
        parsed.append(DateRange(start, end, days_of_week))
    return parsed
