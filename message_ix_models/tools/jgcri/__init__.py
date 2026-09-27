"""Handle data from JGCRI.

JGCI is the “Joint Global Change Research Institute,” which is a collaboration between
the University of Maryland, College Park, and the Pacific Northwest National Laboratory,
part of the U.S. Department of Energy.
"""

import logging
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import genno
from genno import Key

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.util import cached, path_fallback, silence_log
from message_ix_models.util.pooch import SOURCE, fetch

if TYPE_CHECKING:
    from pathlib import Path

    from genno.types import AnyQuantity
    from pandas import DataFrame
    from sdmx.model.common import Codelist

log = logging.getLogger(__name__)


class DATAFLOW_ID(Enum):
    """Identifier of a CEDS data flow.

    These correspond to the four files available in the Zenodo record.
    """

    MISSING = auto()
    aggregate = auto()
    detailed = auto()
    supplementary_bunkers = auto()
    supplementary_extension = auto()


#: Mapping from original to MESSAGE dimensions.
DIMS = {"country": "n", "em": "e", "fuel": "c", "sector": "t", "year": "y"}

#: Mapping from original "country" labels to ISO 3166-1 alpha-3 codes and others
#: appearing in :doc:`/pkg-data/node`.
NODE = {"GLOBAL": "", "SRB (KOSOVO)": "XKX", "_T": "World"}


@register_source
class CEDS(ExoDataSource):
    """Provider of data from the CEDS.

    CEDS is the “Community Earth-atmosphere Data System for Historical Surface Fluxes”.

    The source is documented at https://www.pnnl.gov/projects/ceds. This class retrieves
    the files from the Zenodo record at https://doi.org/10.5281/zenodo.15059443.

    .. todo::

       - Support the distinct format of :attr:`DATAFLOW_ID.supplementary_bunkers`.
       - Improve :func:`ceds_data_from_files` to prune/remove leading zeros for each
         group of indices dimensions other than |y|. For instance, if for a given
         :math:`(c, e, n, t)` the values prior to :math:`y = 1950` are all zero, drop
         those values.
    """

    @dataclass
    class Options(BaseOptions):
        #: CEDS dataflow to retrieve.
        dataflow: DATAFLOW_ID = DATAFLOW_ID.MISSING

        #: Version or release of the CEDS data set. Currently only the 2025-03-18
        #: version is supported.
        version: str = "2025-03-18"

        #: Minimum value on the `year` dimension. The original data contain values for
        #: year=1750.
        year_min: int = 2000

        #: By default, do not aggregate.
        aggregate: bool = False

        #: Do not interpolate on the time/ |y| dimension. The data have annual
        #: resolution.
        interpolate: bool = False

        #: Set to :any:`True` to use test data from the :mod:`message_ix_models`
        #: repository.
        _test: bool = False

        def __post_init__(self) -> None:
            # Convert a string option to an Enum member
            if isinstance(self.dataflow, str):
                self.dataflow = DATAFLOW_ID[self.dataflow]

        @property
        def fname(self) -> str:
            """Return the name of a CEDS archive file, per :attr:`dataflow`."""
            return f"CEDS_v_{self.version.replace('-', '_')}_{self.dataflow.name}.zip"

    options: Options

    key = Key("emission:c-e-n-t-y:CEDS")

    def __init__(self, *args, **kwargs) -> None:
        self.options = self.Options.from_args("CEDS", *args, **kwargs)
        # Use distinct keys for each data flow
        self.key = self.key + self.options.dataflow.name
        super().__init__()

    def get(self) -> "AnyQuantity":
        """Return the raw data.

        This method performs the following steps:

        1. If needed, retrieve a data archive from :data:`.pooch.SOURCE` using the entry
           "JGCRI_CEDS". The files are stored in the :attr:`.Config.cache_path`, and
           range in size between 800 KiB and 74 MiB.
        2. If needed, extract all the members of the archive to a :file:`…/jgcri-ceds/`
           subdirectory of the cache directory. The extracted size is between 1.9 and
           389 MiB.
        3. Call :func:`.ceds_data_from_files` to read the data files and apply
           :attr:`.Options.measure` and :attr:`.Options.filter_pattern`. The function is
           decorated with :func:`.cached`, so identical parameters and file paths result
           in a cache hit.
        4. Convert to :class:`genno.Quantity` and return.
        """

        if not self.options._test:
            # - Fetch (if necessary) and unpack (if necessary) the BACI data archive.
            # - Select only the data files.
            paths: Iterable[Path] = fetch(
                **SOURCE["JGCRI_CEDS"], fname=self.options.fname
            )
        else:  # pragma: no cover
            paths = path_fallback("jgcri-ceds", where="test").glob("*.csv")

        # NB Reading the entire data set is only a few seconds if not cached, so we do
        #    not preemptively filter the files.

        # Prepare keyword arguments for ceds_data_from_files()
        kw: dict[str, Any] = dict(assign={}, query=f"{self.options.year_min} < year")
        if self.options.dataflow == DATAFLOW_ID.supplementary_extension:
            # No file in this data flow has a "country" column. Insert a fixed value.
            kw.update(assign=dict(country="_T"))

        # - Read data from the files; cache.
        # - Convert from data frame to genno.Quantity.
        return genno.Quantity(
            ceds_data_from_files(list(paths), **kw).rename_axis(index=DIMS)["value"],
            units="kt",
        )

    def transform(self, c: "genno.Computer", base_key: "Key") -> "Key":
        """Prepare `c` to transform raw data from `base_key`."""
        if self.options.interpolate:  # pragma: no cover
            raise ValueError("interpolate=True; CEDS data have annual resolution")

        if self.options.aggregate:
            k = base_key

            # Construct groups for aggregation using the coordinates of the loaded data
            # and the configured node codelist
            c.add(f"agg:n:{__name__}", get_node_groups, base_key, "n::codelist")

            # Aggregate original nodes to the target codelist
            c.add(k[0], "aggregate", base_key, f"agg:n:{__name__}", keep=False)

            return k[0]
        else:
            return base_key


@cached
def ceds_data_from_files(
    paths: list["Path"], assign: dict[str, str], query: str
) -> "DataFrame":
    """Read the :class:`.CEDS` data from 1 or more file `paths`.

    - :func:`dask.dataframe.read_csv` and pyarrow are used for better performance.
    - For :attr:`DATAFLOW_ID.aggregate`, files may lack a column for "country", "fuel",
      or "sector"; these imply totals over the respective dimension. Data from these
      files are assigned the code "_T" (total) for the missing dimension(s).
    """
    import dask.dataframe as dd
    import pandas as pd
    from pandas.api.types import CategoricalDtype

    # Index columns appearing in files
    id_vars = "country em fuel sector".split()

    def assert_units(df: "DataFrame") -> "DataFrame":
        """Assert that all labels in the "units" column begin with `kt`."""
        assert {"kt"} == set(
            df["units"]
            .str.replace(
                "^(kt)(C|CH4|CO|CO2|NH3|NMVOC|NO2|N2O|SO2)$", r"\1", regex=True
            )
            .unique()
        )
        return df.drop(columns="units")

    # Either transform an existing "country" label to upper case, like ISO 3166-1
    # alpha-3, or assign a literal value from the argument `assign`
    assign = dict(country=pd.col("country").str.upper()) | assign

    with silence_log("fsspec.local"):
        # Read CSVs. assume_missing=True treats columns (e.g. X1970) as float, rather
        # than int, if initial files/data contain only zeros.
        ddf = dd.read_csv(paths, engine="pyarrow", assume_missing=True)

        # # Apply filters for each dimension before/during compute()
        # for dim, expr in filters.items():
        #     ddf = ddf[ddf[dim].astype(str).str.fullmatch(expr)]

        # - Compute: read and concatenate the files
        # - Fill NA values in the index columns.
        #   NB This does not work when applied to `ddf` before compute().
        # - Assign or modify values according to `assign`, above.
        # - Apply assert_units() and drop the "units" column.
        # - Convert index columns to categoricals.
        # - Melt from wide to long format, adding "year".
        # - Remove leading "X" from "year" labels and convert to categorical.
        # - Convert to multi-indexed DataFrame with a single column.
        result = (
            ddf.compute()
            .fillna(dict.fromkeys(id_vars, "_T"))
            .assign(**assign)
            .pipe(assert_units)
            .astype(dict.fromkeys(id_vars, "category"))
            .melt(id_vars=id_vars, var_name="year")
            .assign(
                year=pd.col("year")
                .str.lstrip("X")
                .astype(int)
                .astype(CategoricalDtype(ordered=True))
            )
            .query(query)
            .set_index(id_vars + ["year"])
        )

    log.info(
        f"{len(result)} observations from {len(paths)} files; "
        f"{result.memory_usage().sum() / 1024**2:.1f} MiB"
    )
    return result


def get_node_groups(qty: "AnyQuantity", nodes: "Codelist") -> dict:
    """Create groups for aggregating on the |n| dimension."""
    # Update the node mapping with the particular "R##_GLB" region corresponding to
    # GLOBAL
    node_map = NODE.copy()
    for code in nodes:
        if code.id.endswith("_AFR"):
            node_map["GLOBAL"] = code.id.replace("AFR", "GLB")
            break

    result: dict[str, list[str]] = defaultdict(list)

    # Iterate over coordinate labels appearing in the "n" dimension of `qty`
    for node in qty.coords["n"].data:
        # Use a value from `node_map`, or the parent code (=region) from the code
        # list
        group_id = node_map.get(node) or nodes[node].parent
        result[group_id].append(node)

    return dict(n=result)
