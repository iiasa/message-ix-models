"""Tools for IEA (Extended) World Energy Balance (WEB) data."""
import logging
import zipfile
from copy import copy
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd
from genno import Quantity
from genno.core.key import single_key
from platformdirs import user_cache_path

from message_ix_models.tools.exo_data import ExoDataSource, register_source
from message_ix_models.util import (
    cached,
    local_data_path,
    package_data_path,
    private_data_path,
)
from message_ix_models.util._logging import silence_log

if TYPE_CHECKING:
    import os

    import genno

log = logging.getLogger(__name__)

DIMS = ["COUNTRY", "PRODUCT", "TIME", "FLOW", "MEASURE"]

FWF_COLUMNS = {
    "COUNTRY": (0, 26),
    "PRODUCT": (26, 32),
    "TIME": (32, 48),
    "FLOW": (48, 64),
    "MEASURE": (64, 68),
    "Value": (68, 100),
}

#: Subset of columns to load, mapped to returned values.
CSV_COLUMNS = {
    "COUNTRY": "node",
    "PRODUCT": "commodity",
    "TIME": "year",
    "FLOW": "flow",
    "MEASURE": "unit",
    "Flag Codes": "flag",
    "Value": "value",
}

#: Mapping from (provider, year, time stamp) → set of file name(s) containing data.
FILES = {
    ("IEA", "2023"): ("WBIG1.zip", "WBIG2.zip"),  # Timestamped 20230726T0014
    ("OECD", "2021"): ("cac5fa90-en.zip",),  # Timestamped 20211119T1000
    ("OECD", "2022"): ("372f7e29-en.zip",),  # Timestamped 20230406T1000
    ("OECD", "2023"): ("8624f431-en.zip",),  # Timestamped 20231012T1000
}


@register_source
class IEA_EWEB(ExoDataSource):
    """Provider of exogenous data from the IEA Extended World Energy Balances.

    To use data from this source, call :func:`.exo_data.prepare_computer` with the
    :py:`source_kw`:

    - "provider": Either "IEA" or "OECD". See :data:`.FILES`.
    - "edition": one of "2021", "2022", or "2023". See :data:`.FILES`.
    - "product": :class:`str` or :class:`list` of :class:`str.
    - "flow": :class:`str` or :class:`list` of :class:`str.

    The returned data have the extra dimensions "product" and "flow", and are not
    aggregated by year.

    Example
    -------
    >>> keys = prepare_computer(
    ...     context,
    ...     computer,
    ...     source="IEA_EWEB",
    ...     source_kw=dict(
    ...         provider="OECD", edition="2022", product="CHARCOAL", flow="RESIDENT"
    ...     ),
    ... )
    >>> result = computer.get(keys[0])
    """

    id = "IEA_EWEB"

    extra_dims = ("product", "flow")

    def __init__(self, source, source_kw):
        if source != self.id:
            raise ValueError(source)

        _kw = copy(source_kw)

        provider = _kw.pop("provider", None)
        edition = _kw.pop("edition", None)
        assert (provider, edition) in FILES
        self.load_kw = dict(provider=provider, edition=edition)

        self.indexers = dict(MEASURE="TJ")
        if product := _kw.pop("product", None):
            self.indexers.update(product=product)
        if flow := _kw.pop("flow", None):
            self.indexers.update(flow=flow)

        if len(_kw):
            raise ValueError(_kw)

    def __call__(self):
        # - Load the data.
        # - Convert to pd.Series, then genno.Quantity.
        # - Map dimensions.
        # - Apply `indexers` to select.
        return (
            Quantity(load_data(**self.load_kw).set_index(DIMS)["Value"], units="TJ")
            .rename({"COUNTRY": "n", "TIME": "y", "FLOW": "flow", "PRODUCT": "product"})
            .sel(self.indexers, drop=True)
        )

    def transform(self, c: "genno.Computer", base_key: "genno.Key") -> "genno.Key":
        # Aggregate only; do not interpolate on "y"
        return single_key(
            c.add(base_key + "1", "aggregate", base_key, "n::groups", keep=False)
        )


def fwf_to_csv(path: Path, progress: bool = False) -> Path:
    """Convert the IEA fixed-width file format to CSV.

    This appears to operate at about 900k lines / second, about 1 minute for the IEA
    2023 .TXT files. This is faster than doing full pandas I/O, which takes 5–10 minutes
    depending on formats.
    """
    import io
    import re

    # Output path
    path_out = path.with_suffix(".csv")
    if path_out.exists() and path_out.stat().st_mtime > path.stat().st_mtime:
        log.info(f"Skip conversion; file exists and is newer than source: {path_out}")
        return path_out

    # Input and output buffers; read the entire file into memory immediately
    file_in = io.BytesIO(path.read_bytes())
    file_out = io.BytesIO()

    # Regular expression to split lines
    expr = re.compile(b"  +")

    if progress:
        from tqdm import tqdm

        iterator: Iterable[bytes] = tqdm(file_in, desc=f"{path} → {path_out}")
    else:
        iterator = file_in

    # Convert to CSV
    for line in iterator:
        file_out.write(b",".join(expr.split(line)))

    # Write to file
    path_out.write_bytes(file_out.getbuffer())

    return path_out


def unpack_zip(path: Path) -> Path:
    """Unpack a ZIP archive."""
    cache_dir = user_cache_path("message-ix-models", ensure_exists=True).joinpath("iea")

    log.info(f"Decompress {path} to {cache_dir}")
    with zipfile.ZipFile(path) as zf:
        members = zf.infolist()
        assert 1 == len(members)
        zi = members[0]

        # Candidate path for the extracted file
        target = cache_dir.joinpath(zi.filename)
        if target.exists() and target.stat().st_size >= zi.file_size:
            log.info(f"Skip extraction of {target}")
            return target
        else:
            return Path(zf.extract(members[0], path=cache_dir))


@cached
def iea_web_data_for_query(
    base_path: Path, *filenames: str, query_expr: str
) -> pd.DataFrame:
    # Filenames to pass to dask.dataframe
    names_to_read = []

    # Iterate over origin filenames
    for filename in filenames:
        path = base_path.joinpath(filename)

        if path.suffix == ".zip":
            path = unpack_zip(path)

        if path.suffix == ".TXT":
            names_to_read.append(fwf_to_csv(path, progress=True))
            args: Dict[str, Any] = dict(header=None, names=DIMS + ["Value"])
        else:
            names_to_read.append(path)
            args = dict(header=0, usecols=list(CSV_COLUMNS.keys()))

    with silence_log("fsspec.local"):
        ddf = dd.read_csv(names_to_read, engine="pyarrow", **args)
        ddf = ddf[ddf["MEASURE"] == "TJ"]
        # NB compute() must precede query(), else "ValueError: The columns in the
        # computed data do not match the columns in the provided metadata" occurs with
        # the CSV-formatted data.
        result = ddf.compute().query(query_expr).dropna(subset=["Value"])

    log.info(f"{len(result)} observations")
    return result


def load_data(
    provider: str,
    edition: str,
    query_expr="MEASURE == 'TJ' and TIME >= 1980",
    base_path=None,
) -> pd.DataFrame:
    """Load data from the IEA World Energy Balances.

    Parameters
    ----------
    base_path : os.Pathlike, optional
        Path containing :data:`.FILE`.

    Returns
    -------
    pandas.DataFrame
        The data frame has the following columns:

        - flow
        - commodity, i.e. “PRODUCT” in the raw data.
        - node, i.e. “COUNTRY” in the raw data.
        - year, i.e. “TIME” in the raw data. The IEA dimension ID is actually more
          precise here, but “year” is used because it aligns with the :mod:`message_ix`
          formulation
        - value
        - unit
        - flag
    """
    files = FILES[(provider, edition)]

    # Identify a location that contains the `files`
    if base_path is None:
        try:
            base_path = private_data_path("iea")
            assert base_path.joinpath(files[0]).exists()
        except AssertionError:
            base_path = local_data_path("iea")
            assert base_path.joinpath(files[0]).exists()

    return iea_web_data_for_query(base_path, *files, query_expr=query_expr)


def generate_code_lists(
    provider: str, edition: str, output_path: Optional["os.PathLike"] = None
) -> None:
    """Extract structure from the data itself."""
    import sdmx.model.v21 as m

    from message_ix_models.util.sdmx import register_agency, write

    output_path = output_path or package_data_path("sdmx")

    IEA = m.Agency(
        id="IEA",
        name="International Energy Agency",
        contact=[m.Contact(uri=["https://iea.org"])],
    )
    register_agency(IEA)

    # Read the data
    data = iea_web_data_for_query(
        private_data_path("iea"), *FILES[(provider, edition)], query_expr="TIME > 0"
    )

    for concept_id in ("COUNTRY", "FLOW", "PRODUCT"):
        # Create a code list with the unique values from this dimension
        cl = m.Codelist(id=f"{concept_id}_{provider}", maintainer=IEA, version=edition)
        cl.extend(
            m.Code(id=code_id) for code_id in sorted(data[concept_id].dropna().unique())
        )
        write(cl, output_path)


def fuzz_data(base_path=None, target_path=None):
    """Generate a fuzzed subset of the data for testing."""
    df = iea_web_data_for_query(base_path)

    # - Reduce the data by only taking 2 periods for each (flow, product, country).
    # - Replace the actual values with random.
    df = (
        df.groupby(["FLOW", "PRODUCT", "COUNTRY"])
        .take([0, -1])
        .reset_index(drop=True)
        .assign(Value=lambda df: np.random.rand(len(df)))
    )

    # TODO write to file
    # path = (target_path or package_data_path("iea")).joinpath(f"fuzzed-{FILE}")
