"""Handle data from the NewClimate Institute's Climate Policy Database (CPDB).

This module provides:

- :class:`.NewClimatePolicy`, a concrete subclass of the abstract/generic
  :class:`.Policy`, that reflects the data model appearing in the CPDB.

  - Enumerations that reflect values appearing in fields of the database which appear to
    be enumerated (as opposed to free text):
    :class:`HIGH_IMPACT`,
    :class:`JURISDICTION`,
    :class:`OBJECTIVE`,
    :class:`SECTOR`,
    :class:`STATUS`,
    :class:`STRINGENCY`,
    :class:`TYPE`, and
    :class:`UPDATE`.

  - A method :meth:`.NewClimatePolicy.from_csv_dict` that interprets the CSV data
    format in which the database is expressed.

- Functions to :func:`fetch` versions of the database from Zenodo, :func:`read` into
  collections of Python objects, or do both (:func:`get`).

These enable programmatic use of the information in the database. For example:

.. code-block:: python

   from message_ix_models.tools.newclimate import SECTOR, get
   from pycountry import countries

   # Fetch and parse the 2024 edition of the database
   policies = get("2024")
   print(len(policies))  # 6507 objects

   # Filter the dict to a list of policy objects matching a certain sector
   p_transport = list(filter(lambda p: SECTOR.Transport in p.sector, policies.values()))
   print(len(p_transport))  # 1298 objects

   # Filter for any policies concerning the country of Austria, or the EU
   match = {pycountry.lookup("Austria"), "European Union"}
   p_AUT = list(filter(lambda p: set(p.geo) & match, policies.values()))
   print(len(p_AUT)))  # 259 objects

.. todo:: Extend the module:

   - Serialize :class:`.NewClimatePolicy` objects in 1 or more formats, preferably
     standards-based.
   - :func:`fetch` versions of the database more recent than the latest Zenodo record,
     using the `cpdb_api package
     <https://github.com/https-github-com-NewClimateInstitute/CPDB-API>`_ or other code.
   - Convert to/from other data models.
"""

import csv
import logging
from functools import cache
from typing import TYPE_CHECKING

from .structure import (
    HIGH_IMPACT,
    JURISDICTION,
    OBJECTIVE,
    SECTOR,
    STATUS,
    STRINGENCY,
    TYPE,
    UPDATE,
    NewClimatePolicy,
)

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "HIGH_IMPACT",
    "JURISDICTION",
    "NewClimatePolicy",
    "OBJECTIVE",
    "SECTOR",
    "STATUS",
    "STRINGENCY",
    "TYPE",
    "UPDATE",
    "read",
    "get",
    "fetch",
]

log = logging.getLogger(__name__)

#: Pooch information for fetching files from the static version of the database.
SOURCE = {  # noqa: E501
    "newclimate-2024": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.15432946",
            registry={
                "ClimatePolicyDatabase_v2024.csv": (
                    "sha256:e893745bc26d225d8e91d063eb1fdbcbb5da4a51ce05d28ce5b9f51f6ef4408f"
                ),
            },
        ),
    ),
    "newclimate-2023": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.10869734",
            registry={
                "ClimatePolicyDatabase_v2023.xlsx": (
                    "sha256:bdce700c6b0c2eeb7fa06584cb8523793b64ec5799d91ae65818209aaf9de682"
                ),
            },
        ),
    ),
    "newclimate-2022": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.7774473",
            registry={
                "ClimatePolicyDatabase_v2022.csv": (
                    "sha256:fe431e41c4c2fb8513d6718fba6ba3bc0a1fd2c5b9016256a106b998f5f48946"
                ),
            },
        ),
    ),
    "newclimate-2021": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.7774471",
            registry={
                "ClimatePolicyDatabase_v2021.xlsx": (
                    "sha256:d880c2c94c7d8da84bb9cf8d315faf7230e4965cbc679ac1783222ecfe84062a"
                ),
            },
        ),
    ),
    "newclimate-2020": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.7774462",
            registry={
                "ClimatePolicyDatabase_v2020.xlsx": (
                    "sha256:08818156401200ec094985c34250ef65cea6ff5246cbbeb1d0ade317f8fdaa0c"
                ),
            },
        ),
    ),
    "newclimate-2019": dict(
        pooch_args=dict(
            base_url="doi:10.5281/zenodo.7774110",
            registry={
                "ClimatePolicyDatabase _v2019.xlsx": (
                    "sha256:c28cdd613496d503ae00bacf637fc052128e04361580110829843b4bf0235368"
                ),
            },
        )
    ),
}


def fetch(version: str) -> "Path":
    """Retrieve data for `version` of the Climate Policy Database from Zenodo."""
    from message_ix_models.util import pooch

    # Ensure sources for this module are registered
    pooch.SOURCE.update(SOURCE)

    # Construct the key
    source_id = f"newclimate-{version}"

    return pooch.fetch(**pooch.SOURCE[source_id], extra_cache_path="newclimate")[0]


def get(version: str) -> dict[str, NewClimatePolicy]:
    """:func:`fetch` and then :func:`read` data for `version` of the database."""
    f_source = fetch(version)

    if f_source.suffix == ".xlsx":
        # Convert Excel to CSV
        import pandas as pd

        f_read = f_source.with_suffix(".csv")
        if not f_read.exists():
            log.info(f"Unpack {f_source} to {f_read}")
            pd.read_excel(f_source).to_csv(f_read, index=False)
    else:
        f_read = f_source

    # - Force use of UTF-8 on macOS and Windows.
    # - The 2022 CSV file is not in UTF-8 format; use a different encoding.
    kwargs = dict(encoding="latin-1" if version == "2022" else "utf-8")

    try:
        return read(f_read, **kwargs)
    except Exception as e:
        if version in ("2021", "2020", "2019"):
            raise NotImplementedError("Read 2021 and earlier data format") from e
        else:  # pragma: no cover
            raise


@cache
def read(path: "Path", **kwargs) -> dict[str, NewClimatePolicy]:
    """Read a CSV file into a :class:`dict` of Policy objects.

    Returns
    -------
    dict
        Keys are :attr:`.NewClimatePolicy.id`. If the file contains records with the
        same IDs, only the last appears, and a warning is logged.
    """
    with open(path, **kwargs) as f:
        policies = [NewClimatePolicy.from_csv_dict(row) for row in csv.DictReader(f)]

    result = {p.id: p for p in policies}
    if len(result) < len(policies):
        log.warning(f"{len(policies) - len(result)} duplicate IDs in `path`")

    return result
