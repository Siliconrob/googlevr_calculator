from dataclasses import dataclass

@dataclass
class MinMaxLengthOfStay:
    min: int
    max: int


@dataclass
class TimeLengthOfStay:
    time: int
    type: str


def parse_range(input: dict) -> MinMaxLengthOfStay:
    parsed = MinMaxLengthOfStay(None, None)
    start = input.get("@min", None)
    if start is not None:
        parsed.min = int(start)
    end = input.get("@max", None)
    if end is not None:
        parsed.max = int(end)
    return parsed