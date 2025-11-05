"""Handle data from CEPII.

CEPII is the “Centre d’études prospectives et d’informations internationales” (fr).
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import genno
import numpy as np

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.util import MappingAdapter, cached, path_fallback, silence_log
from message_ix_models.util.pooch import SOURCE, fetch

if TYPE_CHECKING:
    from pathlib import Path
    from re import Pattern

    from genno.types import AnyQuantity, Key
    from pandas import DataFrame

log = logging.getLogger(__name__)

#: Labels appearing in the :math:`(i, j)` dimensions of the :class:`BACI` data that are
#: not current ISO 3166-1 numeric codes. These are generally of 3 kinds:
#:
#: - Numeric codes that are in ISO 3166-3 (“Code for formerly used names of countries”),
#:   not ISO 3166-1.
#: - Numeric codes for countries that exist in ISO 3166-1, but simply differ. For
#:   example, ISO has 250 for “France”, but BACI uses 251.
#: - Numeric codes for countries or country groups that do not appear in ISO 3166.
#:
#: This is a subset of the labels appearing in the ``country_code`` column of the file
#: :file:`country_codes_V202501.csv` in the archive :file:`BACI_HS92_V202501.zip`. Only
#: the labels appearing in the data files are included.
COUNTRY_CODES = [
    (58, "BEL"),  # "Belgium-Luxembourg (...1998)"; 56 in ISO 3166-1
    (251, "FRA"),  # 250
    (490, "S19"),  # "Other Asia, nes", not in ISO 3166-1
    (530, "ANT"),  # Part of ISO 3166-3, not -1
    (579, "NOR"),  # 578
    (699, "IND"),  # 356
    (711, "ZA1"),  # "Southern African Customs Union (...1999)"; not in ISO 3166-1
    (736, "SDN"),  # "Sudan (...2011)"; 729
    (757, "CHE"),  # 756
    (842, "USA"),  # 840
    (849, "PUS"),  # "US Misc. Pacific Isds", not in ISO 3166-1
    (891, "SCG"),  # Part of ISO 3166-3, not -1
]

#: Dimensions and data types for input data. In order to reduce memory and disk usage:
#:
#: - :py:`np.uint16` (0 to 65_535) is used for t (year), i (exporter), and j (importer)
#: - :py:`np.uint32` (0 to 4_294_967_295) is used for k (product), since these values
#:   can be as large as 999_999.
DTYPE = {"t": np.uint16, "i": np.uint16, "j": np.uint16, "k": np.uint32}


@register_source
class BACI(ExoDataSource):
    """Provider of data from the BACI data source.

    BACI is the “Base pour l’Analyse du Commerce International” (fr). The source is
    documented at:

    - https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html
    - https://www.cepii.fr/CEPII/en/bdd_modele/bdd_modele_item.asp?id=37

    Currently the class supports:

    - The 202501 release only.
    - The 1992 Harmonized System (HS92) only.

    .. todo::
       - Aggregate to MESSAGE regions.
       - Test with additional HS categorizations.
       - Test with additional releases.
    """

    @dataclass
    class Options(BaseOptions):
        #: By default, do not aggregate.
        aggregate: bool = False
        #: By default, do not interpolate.
        interpolate: bool = False

        #: Either "quantity" or "value".
        measure: str = "quantity"

        #: Dimensions for the returned :class:`.Key`/:class:`.Quantity`.
        #:
        #: Per the BACI README file, these are:
        #:
        #: - "t": year
        #: - "i": exporter
        #: - "j": importer
        #: - "k": product
        dims: tuple[str, ...] = tuple(DTYPE)

        #: Regular expressions for filtering on any of :attr:`dims`. Keys **must** be
        #: in :attr:`dims`; values **must** be regular expressions or compiled
        #: :class:`re.Pattern` that fullmatch the :class:`str` representation of labels
        #: on the respective dimension.
        #:
        #: For example, :py:`filter_pattern=dict(k="270(4..|576)")` matches any 6-digit
        #: label on the :math:`k` dimension starting with '2704', or the exact label
        #: '270576'.
        filter_pattern: dict[str, "str | Pattern"] = field(default_factory=dict)

        #: Set to :any:`True` to use test data from the :mod:`message_ix_models`
        #: repository.
        test: bool = False

        def __post_init__(self) -> None:
            if self.measure not in ("quantity", "value"):
                raise ValueError(
                    f"measure={self.measure}; must be either 'quantity' or 'value'"
                )
            if extra := set(self.filter_pattern) - set(self.dims):
                raise ValueError(
                    f"Filter patterns for non-existent dimension(s): {sorted(extra)}"
                )

    options: Options

    def __init__(self, *args, **kwargs) -> None:
        self.options = self.Options.from_args("BACI", *args, **kwargs)
        super().__init__()

    def get(self) -> "AnyQuantity":
        """Return the raw data.

        This method performs the following steps:

        1. If needed, retrieve the data archive from :data:`.pooch.SOURCE` using the
           entry "CEPII_BACI". The file is stored in the :attr:`.Config.cache_path`, and
           is about 2.2 GiB.
        2. If needed, extract all the members of the archive to a :file:`…/cepii-baci/`
           subdirectory of the cache directory. The extracted size is about 7.9 GiB,
           containing about 2.6 × 10⁸ observations.
        3. Call :func:`.baci_data_from_files` to read the data files and apply
           :attr:`.Options.measure` and :attr:`.Options.filter_pattern`. The function is
           decorated with :func:`.cached`, so identical parameters and file paths result
           in a cache hit.
        4. Convert to :class:`genno.Quantity` and return.
        """

        if not self.options.test:  # pragma: no cover
            # - Fetch (if necessary) and unpack (if necessary) the BACI data archive.
            # - Select only the data files.
            paths: Iterable[Path] = filter(
                lambda p: p.name.startswith("BACI"), fetch(**SOURCE["CEPII_BACI"])
            )
        else:
            paths = path_fallback("cepii-baci", where="test").glob("*.csv")

        # - Read data from the files for the given measure and filters; cache.
        # - Convert from data frame to genno.Quantity.
        return genno.Quantity(
            baci_data_from_files(
                list(paths), self.options.measure, self.options.filter_pattern
            )
            .set_index(list(self.options.dims))
            .iloc[:, 0]
        )

    def transform(self, c: "genno.Computer", base_key: "Key") -> "Key":
        """Prepare `c` to transform raw data from `base_key`.

        1. Map BACI codes for the :math:`(i, j)` dimensions from numeric (mainly ISO
           3166-1 numeric) to ISO 3166-1 alpha_3. See :func:`get_mapping`.
        """
        c.add(base_key[0], get_mapping(), base_key)
        return base_key[0]


@cached
def baci_data_from_files(
    paths: list["Path"], measure: str, filters: dict[str, "str | Pattern"]
) -> "DataFrame":
    """Read the :class:`.BACI` data from files.

    :func:`dask.dataframe.read_csv` and pyarrow are used for better performance.
    :data:`DTYPE` is used to specify columns and dtypes.
    """
    import dask.dataframe as dd

    col = measure[0]  # First character of the measure ID -> "v" or "q" column name

    with silence_log("fsspec.local"):
        ddf = dd.read_csv(
            paths,
            engine="pyarrow",
            dtype=DTYPE | {col: float},
            usecols=list(BACI.Options.dims) + [col],
        )

        # Apply filters for each dimension before/during compute()
        for dim, expr in filters.items():
            ddf = ddf[ddf[dim].astype(str).str.fullmatch(expr)]

        result = ddf.compute()

    log.info(f"{len(result)} observations")
    return result


def get_mapping() -> MappingAdapter:
    """Return an adapter from codes appearing in BACI data.

    The BACI data for dimensions :math:`i` (exporter) and :math:`j` (importer) contain
    ISO 3166-1 numeric codes, plus some other idiosyncratic codes from
    :data:`COUNTRY_CODES`. The returned adapter maps these to the corresponding alpha-3
    code.

    Using the adapter makes data suitable for aggregation using the
    :mod:`message_ix_models` ``node`` code lists, which include those alpha-3 codes as
    children of each region code.
    """
    from pycountry import countries

    # All values from ISO 3166-1, plus some idiosyncratic values from COUNTRY_CODES
    num_to_a3 = COUNTRY_CODES + [(int(c.numeric), c.alpha_3) for c in countries]

    # Use the same mapping for both i and j dimensions
    return MappingAdapter({"i": num_to_a3, "j": num_to_a3}, on_missing="raise")


if __name__ == "__main__":  # pragma: no cover
    from tqdm import tqdm

    from message_ix_models.util import random_sample_from_file

    print("Generate test data for BACI")

    # - Fetch (if necessary) and unpack (if necessary) the BACI data archive.
    # - Select only the data files.
    paths = filter(lambda p: p.name.startswith("BACI"), fetch(**SOURCE["CEPII_BACI"]))

    # Target for test data files
    target_dir = path_fallback("cepii-baci", where="test")

    # Fraction of data to retain
    frac = 0.001

    for file in tqdm(paths):
        # - Read data, sample, and replace with random values.
        # - Write to the test data directory.
        random_sample_from_file(file, frac, cols=["q", "v"], na_values=[""]).to_csv(
            target_dir.joinpath(file.name), float_format="%.3f", index=False
        )
