import decimal
from dataclasses import dataclass
from datetime import datetime

from pydapper import connect


@dataclass
class ExtraGuestCharge:
    external_id: str
    adult_charges: decimal
    xml_contents: str


def get_extra_guest_charges(external_id: str,
                            start: datetime,
                            end: datetime,
                            dsn: str) -> list[ExtraGuestCharge]:
    with connect(dsn) as commands:
        return commands.query(f"""
            select e.external_id, e.adult_charges, e.xml_contents
            from ExtraGuestCharges e
            left join ExtraGuestCharges_StayDates esd
            on e.id = esd.parent_id
            and ?start_date? between COALESCE(esd.start, DATE(?start_date?, '-1 day')) and COALESCE(esd.end, DATE(?start_date?, '+1 day'))
            and ?end_date? between COALESCE(esd.end, DATE(?end_date?, '-1 day')) and COALESCE(esd.end, DATE(?end_date?, '+1 day'))
            where e.external_id = ?external_id?     
            """,
                              param={
                                  "external_id": external_id,
                                  "start_date": start.isoformat(),
                                  "end_date": end.isoformat()
                              }, model=ExtraGuestCharge)
