import datetime
from dataclasses import dataclass, field

import pendulum


@dataclass
class DateRange:
    start: datetime.date
    end: datetime.date


def parse_ranges(input: any) -> list[DateRange]:
    parsed = []
    to_parse_list = [input] if isinstance(input, dict) else input
    for current in to_parse_list:
        start = pendulum.parse(current["@start"])
        end = pendulum.parse(current["@end"])
        parsed.append(DateRange(start, end))
    return parsed
