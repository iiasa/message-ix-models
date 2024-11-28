"""Tools for IEA (Extended) World Energy Balance (WEB) data."""

import logging
import zipfile
from collections.abc import Iterable
from copy import copy
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from genno import Quantity
from genno.core.key import single_key
from platformdirs import user_cache_path

from message_ix_models.tools.exo_data import ExoDataSource, register_source
from message_ix_models.util import cached, package_data_path, path_fallback
from message_ix_models.util._logging import silence_log

if TYPE_CHECKING:
    import os

    import genno

    from message_ix_models.util.common import MappingAdapter

log = logging.getLogger(__name__)

#: ISO 3166-1 alpha-3 codes for “COUNTRY” codes appearing in the 2024 edition. This
#: mapping only includes values that are not matched by :func:`pycountry.lookup`. See
#: :func:`.iso_3166_alpha_3`.
COUNTRY_NAME = {
    "AUSTRALI": "AUS",
    "BOSNIAHERZ": "BIH",
    "BRUNEI": "BRN",
    "CONGO": "COD",  # Override lookup(…) → COG
    "CONGOREP": "COG",
    "COSTARICA": "CRI",
    "COTEIVOIRE": "CIV",
    "CURACAO": "CUW",
    "CZECH": "CZE",
    "DOMINICANR": "DOM",
    "ELSALVADOR": "SLV",
    "EQGUINEA": "GNQ",
    "HONGKONG": "HKG",
    "KOREA": "KOR",
    "KOREADPR": "PRK",
    "LUXEMBOU": "LUX",
    "MBURKINAFA": "BFA",
    "MCHAD": "TCD",
    "MGREENLAND": "GRL",
    "MMALI": "MLI",
    "MMAURITANI": "MRT",
    "MPALESTINE": "PSE",
    "NETHLAND": "NLD",
    "PHILIPPINE": "PHL",
    "RUSSIA": "RUS",
    "SAUDIARABI": "SAU",
    "SOUTHAFRIC": "ZAF",
    "SRILANKA": "LKA",
    "SSUDAN": "SSD",
    "SWITLAND": "CHE",
    "TAIPEI": "TWN",
    "TRINIDAD": "TTO",
    "TURKEY": "TUR",
    "TURKMENIST": "TKM",
    "UAE": "ARE",
    "UK": "GBR",
}

#: Dimensions of the data.
DIMS = ["COUNTRY", "PRODUCT", "TIME", "FLOW", "MEASURE"]

#: Mapping from (provider, year, time stamp) → set of file name(s) containing data.
FILES = {
    ("IEA", "2024"): (  # Timestamped 20240725T0830
        "web/2024-07-25/WBIG1.zip",
        "web/2024-07-25/WBIG2.zip",
    ),
    ("IEA", "2023"): ("WBIG1.zip", "WBIG2.zip"),  # Timestamped 20230726T0014
    ("OECD", "2021"): ("cac5fa90-en.zip",),  # Timestamped 20211119T1000
    ("OECD", "2022"): ("372f7e29-en.zip",),  # Timestamped 20230406T1000
    ("OECD", "2023"): ("8624f431-en.zip",),  # Timestamped 20231012T1000
}

#: Location of :data:`.FILES`; :py:`where=` argument to :func:`.path_fallback`.
#:
#: .. todo:: Change to :py:`"local test"`` after adjusting :file:`transport.yaml`
#:    workflow in :mod:`.message_data`.
WHERE = "local private test"


@register_source
class IEA_EWEB(ExoDataSource):
    """Provider of exogenous data from the IEA Extended World Energy Balances.

    To use data from this source, call :func:`.exo_data.prepare_computer` with the
    :py:`source_kw`:

    - "provider": Either "IEA" or "OECD". See :data:`.FILES`.
    - "edition": one of "2021", "2022", or "2023". See :data:`.FILES`.
    - "product": :class:`str` or :class:`list` of :class:`str`.
    - "flow": :class:`str` or :class:`list` of :class:`str`.

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

    key = "energy:n-y-product-flow:iea"

    def __init__(self, source, source_kw):
        """Initialize the data source."""
        if source != self.id:
            raise ValueError(source)

        _kw = copy(source_kw)

        p = self.provider = _kw.pop("provider", None)
        e = self.edition = _kw.pop("edition", None)
        try:
            files = FILES[(p, e)]
        except KeyError:
            raise ValueError(f"No IEA data files for (provider={p!r}, edition={e!r})")

        self.indexers = dict(MEASURE="TJ")
        if product := _kw.pop("product", None):
            self.indexers.update(product=product)
        if flow := _kw.pop("flow", None):
            self.indexers.update(flow=flow)

        if len(_kw):
            raise ValueError(_kw)

        # Identify a location that contains the files for the given (provider, edition)
        # Parent directory relative to which `files` are found
        self.path = dir_fallback("iea", files[0], where=WHERE)

    def __call__(self):
        """Load and process the data."""
        # - Load the data.
        # - Convert to pd.Series, then genno.Quantity.
        # - Map dimensions.
        # - Apply `indexers` to select.
        return (
            Quantity(
                load_data(
                    provider=self.provider, edition=self.edition, path=self.path
                ).set_index(DIMS)["Value"],
                units="TJ",
            )
            .rename({"COUNTRY": "n", "TIME": "y", "FLOW": "flow", "PRODUCT": "product"})
            .sel(self.indexers, drop=True)
        )

    def transform(self, c: "genno.Computer", base_key: "genno.Key") -> "genno.Key":
        """Aggregate only; do not interpolate on "y"."""
        # Map values like RUSSIA appearing in the (IEA, 2024) edition to e.g. RUS
        adapter = get_mapping(self.provider, self.edition)
        k = c.add(base_key + "adapted", adapter, base_key)
        return single_key(
            c.add(base_key + "agg", "aggregate", k, "n::groups", keep=False)
        )


def fwf_to_csv(path: Path, progress: bool = False) -> Path:  # pragma: no cover
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
    """Load data from `base_path` / `filenames` in IEA WEB formats."""
    import dask.dataframe as dd

    names_to_read = []  # Filenames to pass to dask.dataframe
    # Keyword arguments for read_csv()
    # - Certain values appearing in (IEA, 2024) are mapped to NaN.
    # - The Value column is numeric.
    args: dict[str, Any] = dict(
        dtype={"Value": float},
        na_values=[".. ", "c ", "x "],
    )

    # Iterate over origin filenames
    for filename in filenames:
        path = base_path.joinpath(filename)

        if path.suffix == ".zip":
            path = unpack_zip(path)

        if path.suffix == ".TXT":  # pragma: no cover
            names_to_read.append(fwf_to_csv(path, progress=True))
            args.update(header=None, names=DIMS + ["Value"])
        else:
            names_to_read.append(path)
            args.update(header=0, usecols=DIMS + ["Value"])

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
    path: Optional[Path] = None,
) -> pd.DataFrame:
    """Load data from the IEA World Energy Balances.

    Parameters
    ----------
    provider : str
        First entry in :data:`.FILES`.
    edition : str
        Second entry in :data:`.FILES`.
    query_expr : str, optional
        Used with :meth:`pandas.DataFrame.query` to reduce the returned data.
    base_path : os.Pathlike, optional
        Path containing :data:`.FILES`. If not provided, locations within
        :mod:`message_data` or :mod:`message_ix_models` are used.

    Returns
    -------
    pandas.DataFrame
        The data frame has one column for each of :data:`.DIMS`, plus "Value".
    """
    path = path or package_data_path("test", "iea")
    if "test" in path.parts:
        log.warning(f"Reading random data from {path}")
    return iea_web_data_for_query(
        path, *FILES[(provider, edition)], query_expr=query_expr
    )


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
    files = FILES[(provider, edition)]
    path = dir_fallback("iea", files[0], where=WHERE)
    data = iea_web_data_for_query(path, *files, query_expr="TIME > 0")

    for concept_id in ("COUNTRY", "FLOW", "PRODUCT"):
        # Create a code list with the unique values from this dimension
        cl = m.Codelist(id=f"{concept_id}_{provider}", maintainer=IEA, version=edition)
        cl.extend(
            m.Code(id=code_id) for code_id in sorted(data[concept_id].dropna().unique())
        )
        write(cl, output_path)


def dir_fallback(*parts, **kwargs) -> Path:
    """Return path to the directory that *contains* a particular file.

    If the last of `parts` is a string with path separators (for instance "a/b/c"), this
    function returns a parent of the path returned by :func:`.path_fallback`, in which
    this part is located.
    """
    f = Path(parts[-1])
    return path_fallback("iea", f, where=WHERE).parents[len(f.parts) - 1]


def get_mapping(provider: str, edition: str) -> "MappingAdapter":
    """Return a Mapping Adapter from codes appearing in IEA EWEB data.

    For each code in the ``COUNTRY`` code list for (`provider`, `edition`) that is a
    country name, the adapter maps the name to a corresponding ISO 3166-1 alpha-3 code.
    :data:`COUNTRY_NAME` is used for values particular to IEA EWEB.

    Using the adapter makes data suitable for aggregation using the
    :mod:`message_ix_models` ``node`` code lists, which include those alpha-3 codes as
    children of each region code.
    """
    from message_ix_models.util import MappingAdapter, pycountry
    from message_ix_models.util.sdmx import read

    maps = dict()
    for concept, dim in ("COUNTRY", "n"), ("FLOW", "flow"), ("PRODUCT", "product"):
        if concept == "COUNTRY":
            cl = read(f"IEA:{concept}_{provider}({edition})")
            pycountry.COUNTRY_NAME.update(COUNTRY_NAME)

            maps[dim] = list()
            for code in cl:
                new_id = pycountry.iso_3166_alpha_3(code.id) or code.id
                maps[dim].append((code.id, new_id))

    return MappingAdapter(maps)
