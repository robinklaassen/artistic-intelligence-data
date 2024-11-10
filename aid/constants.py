from zoneinfo import ZoneInfo

from pyproj import Transformer

DEFAULT_TIMEZONE = ZoneInfo("Europe/Amsterdam")  # stroopwafels

WGS84_TO_RDNEW = Transformer.from_crs("EPSG:4326", "EPSG:28992")
