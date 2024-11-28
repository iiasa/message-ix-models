from functools import lru_cache
from typing import Optional

from pycountry import countries, historic_countries

#: Mapping from common, non-standard country names to exact field values occurring in
#: the ISO 3166-1 database.
#:
#: Other code **may** extend this mapping before calling :func:`iso_3166_alpha_3`.
COUNTRY_NAME = {
    "Korea": "Korea, Republic of",
    "Republic of Korea": "Korea, Republic of",
    "Russia": "Russian Federation",
    "South Korea": "Korea, Republic of",
    "Turkey": "Türkiye",
}


@lru_cache(maxsize=2**9)
def iso_3166_alpha_3(name: str) -> Optional[str]:
    """Return an ISO 3166 alpha-3 code for a country `name`.

    Parameters
    ----------
    name : str
        Country name. This is looked up in the `pycountry
        <https://pypi.org/project/pycountry/#countries-iso-3166>`_ 'name',
        'official_name', or 'common_name' field. Values in :data:`COUNTRY_NAME` are
        supported.

    Returns
    -------
    str or None
    """
    # Maybe map a known, non-standard value to a standard value
    name = COUNTRY_NAME.get(name, name)

    # Use pycountry's built-in, case-insensitive lookup on all fields including name,
    # official_name, and common_name
    for db in (countries, historic_countries):
        try:
            return db.lookup(name).alpha_3
        except LookupError:
            continue  # Not found in `db`, e.g. countries; try again

    return None
