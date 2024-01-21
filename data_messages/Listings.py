import decimal
from dataclasses import dataclass
from glom import glom

from data_messages import FileInfo
from fileset import DataHandlers
from fileset.DataHandlers import get_safe_list


@dataclass
class Address:
    street: str = None
    street_extra: str = None
    province_state: str = None
    city: str = None
    postal_code: str = None


@dataclass
class GeoPoint:
    latitude: decimal
    longitude: decimal
    projection: str = "EPSG:3857"  # https://epsg.io/3857


@dataclass
class Listing:
    external_id: str = None
    name: str = None
    address: Address = None
    location: GeoPoint = None
    description: str = None


def insert_records(file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    pass


def create_tables(dsn: str):
    pass


def load_rates(property_data: Listing,
               file_info: FileInfo.FileInfo,
               file_args: DataHandlers.DataFileArgs) -> FileInfo.FileInfo:
    pass


def parse_address(address_block) -> Address:
    components = glom(address_block, '**.component')
    if len(components) == 0:
        return None

    parsed_address = Address()
    for component in get_safe_list(components):
        address_part = glom(component, '@name')
        address_value = component.get("#text", None)
        match address_part:
            case "addr1":
                parsed_address.street = address_value
            case "addr2":
                parsed_address.street_extra = address_value
            case "province":
                parsed_address.province_state = address_value
            case "city":
                parsed_address.city = address_value
            case "postal_code":
                parsed_address.postal_code = address_value

    return parsed_address


def read_listings(file_args: DataHandlers.DataFileArgs) -> (list[Listing], FileInfo.FileInfo):
    if len(file_args.formatted_data.keys()) > 1 or glom(file_args.formatted_data, 'listings', default=None) is None:
        return [], None

    results = FileInfo.FileInfo(file_args.file_name)
    listing_data_sets = []

    listing_data = glom(file_args.formatted_data, 'listings.listing')
    listing = Listing()
    listing.external_id = glom(listing_data, 'id')

    listing_name = glom(listing_data, 'name', default=None)
    if listing_name is not None:
        listing.name = listing_name.get("#text", None)
    listing.address = parse_address(glom(listing_data, 'address', default=None))
    latitude = glom(listing_data, 'latitude', default=0)
    longitude = glom(listing_data, 'longitude', default=0)
    listing.location = GeoPoint(latitude=latitude, longitude=longitude)
    listing.description = glom(listing_data, 'content.text.body', default=None)
    listing_data_sets.append(listing)
    return listing_data_sets, results
