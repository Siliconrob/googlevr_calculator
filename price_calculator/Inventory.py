from dataclasses import dataclass
from datetime import datetime
from pydapper import connect


@dataclass
class Inventory:
    external_id: str
    current_day: datetime.date
    start: datetime.date
    end: datetime.date
    inventory: int
    xml_contents: str


def get_inventory(external_id: str, start: datetime, end: datetime, dsn: str) -> list[Inventory]:
    with connect(dsn) as commands:
        return commands.query(f"""
                select o.external_id,
                       date(aa.td, '-1 day') current_day,
                       o.start,
                       o.end,
	                   o.inventory,
	                   o.xml_contents
                from OTAHotelInvCountNotifRQ o
                inner join
                (
                    WITH RECURSIVE cnt(x) AS
                    (
                        SELECT 1
                        UNION ALL
                        SELECT x + 1
                        FROM cnt
                        LIMIT (SELECT (JULIANDAY(?end_date?) - JULIANDAY(?start_date?)))
                    )
                    SELECT date(JULIANDAY(?start_date?), '+' || x || ' days') as td
                    FROM cnt
                ) aa
                on aa.td between o.start and o.end
                where o.external_id = ?external_id?
                """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat(),
                              }, model=Inventory)
