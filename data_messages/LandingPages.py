import decimal
from dataclasses import dataclass
from glom import glom

from data_messages import FileInfo
from fileset import DataHandlers
from fileset.DataHandlers import get_safe_list


@dataclass
class LandingPage:
    display_name: str = None
    url: str = None


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    pass


def create_tables(dsn: str):
    pass


def load_rates(property_data: LandingPage,
               file_info: FileInfo.FileInfo,
               file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    pass


def read_landing_pages(file_args: DataHandlers.DataFileArgs) -> (list[LandingPage], FileInfo.FileInfo):
    if len(file_args.formatted_data.keys()) > 1 or glom(file_args.formatted_data, '**.PointOfSale',
                                                        default=None) is None:
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    landing_pages = []

    points_of_sale = glom(file_args.formatted_data, '**.PointOfSale')
    if len(points_of_sale) == 0:
        return [], None

    point_of_sale = get_safe_list(points_of_sale).pop()
    landing_page = LandingPage()
    display_name_data = glom(point_of_sale, 'DisplayNames', default=None)
    if display_name_data is not None:
        landing_page.display_name = glom(display_name_data, '@display_text', default=None)
    landing_page.url = glom(point_of_sale, 'URL', default=None)
    landing_pages.append(landing_page)
    return landing_pages, results
