import datetime
from dataclasses import dataclass


@dataclass
class BookingWindowTimeSpan:
    min: datetime.timedelta
    max: datetime.timedelta


@dataclass
class BookingWindowInt:
    min: int
    max: int


def parse_range_int(input: dict) -> BookingWindowInt:
    if input is None:
        return None
    parsed = BookingWindowInt(None, None)
    start = input.get("@min", None)
    if start is not None:
        parsed.min = int(start)
    end = input.get("@max", None)
    if end is not None:
        parsed.max = int(end)
    return parsed


def get_split_details(s: str, split: str) -> (int, str):
    if split in s:
        n, s = s.split(split)
    else:
        n = 0
    return n, s


def parse_duration(s: str) -> datetime.timedelta:
    # Remove prefix
    s = s.split('P')[-1]
    # Step through letter dividers
    days, s = get_split_details(s, 'D')
    _, s = get_split_details(s, 'T')
    hours, s = get_split_details(s, 'H')
    minutes, s = get_split_details(s, 'M')
    seconds, s = get_split_details(s, 'S')
    return datetime.timedelta(days=int(days), hours=int(hours), minutes=int(minutes), seconds=int(seconds))


def parse_range_timespans(input: dict) -> BookingWindowTimeSpan:
    if input is None:
        return None
    parsed = BookingWindowTimeSpan(None, None)
    start = input.get("@min", None)
    if start is not None:
        parsed.min = parse_duration(start)
    end = input.get("@max", None)
    if end is not None:
        parsed.max = parse_duration(end)
    return parsed
