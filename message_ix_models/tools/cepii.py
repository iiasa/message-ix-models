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
from message_ix_models.util import cached, path_fallback, silence_log
from message_ix_models.util.pooch import SOURCE, fetch

if TYPE_CHECKING:
    from pathlib import Path
    from re import Pattern

    from genno.types import AnyQuantity
    from pandas import DataFrame

log = logging.getLogger(__name__)

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
       - Transform ISO 3166-1 numeric codes for the :math:`i, j` dimensions to
         alpha-3 codes.
       - Aggregate to MESSAGE regions.
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
           entry "CEPII_BACI". The file is stored in the :attr:`.Config.cache_dir`, and
           is about 2.2 GiB.
        2. If needed, extract all the members of the archive to the :file:`cepii-baci`
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
