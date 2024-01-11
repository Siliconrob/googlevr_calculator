from dataclasses import dataclass
from enum import Enum
from pydapper import connect


class WeekDay(Enum):
    Sunday = (0, "U")
    Monday = (1, "M")
    Tuesday = (2, "T")
    Wednesday = (3, "W")
    Thursday = (4, "H")
    Friday = (5, "F")
    Saturday = (6, "S")


@dataclass
class DayOfTheWeek:
    day_id: int
    name: str
    google_code: str


def load(db_name: str):
    with connect(db_name) as commands:
        commands.execute(f"""
            create table if not exists DayOfTheWeek
            (
                day_id INTEGER PRIMARY KEY,
                name varchar(10) UNIQUE,                
                google_code varchar(1) UNIQUE,
                UNIQUE(day_id, google_code, name)
            )
            """)
        commands.execute(f"""
            insert into DayOfTheWeek (day_id, name, google_code)
            values (?day_id?, ?name?, ?google_code?)
            ON CONFLICT (day_id, name, google_code)
            DO NOTHING
            """,
                         param=[{
                             "day_id": new_day.value[0],
                             "name": new_day.name,
                             "google_code": new_day.value[1],
                         } for new_day in WeekDay])
