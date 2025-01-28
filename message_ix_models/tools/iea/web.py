"""Tools for IEA (Extended) World Energy Balance (WEB) data."""

import logging
import zipfile
from collections.abc import Iterable
from copy import copy
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

import genno
import pandas as pd
from genno import Key
from genno.core.key import single_key
from genno.operator import concat
from platformdirs import user_cache_path

from message_ix_models.model.structure import get_codelist
from message_ix_models.tools.exo_data import ExoDataSource, register_source
from message_ix_models.util import cached, package_data_path, path_fallback
from message_ix_models.util._logging import silence_log

if TYPE_CHECKING:
    import os

    import genno
    from genno.types import AnyQuantity

    from message_ix_models.types import KeyLike
    from message_ix_models.util.common import MappingAdapter

log = logging.getLogger(__name__)

#: ISO 3166-1 alpha-3 codes for 'COUNTRY' codes appearing in the 2024 edition. This
#: mapping includes only values that are not matched by :func:`pycountry.lookup`. See
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
    following `source_kw`:

    - :py:`provider`: Either 'IEA' or 'OECD'. See :data:`.FILES`.
    - :py:`edition`: one of '2021', '2022', or '2023'. See :data:`.FILES`.
    - :py:`product` (optional): :class:`str` or :class:`list` of :class:`str`. Select
      only these labels from the 'PRODUCT' dimension.
    - :py:`flow` (optional): :class:`str` or :class:`list` of :class:`str`. Select only
      these labels from the 'FLOW' dimension.
    - :py:`transform` (optional): either "A" (default) or "B". See :meth:`.transform`.
    - :py:`regions`: **must** also be given with the value :py:`"R12"` if giving
      :py:`transform="B"`.

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

    key = Key("energy:n-y-product-flow:iea")

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

        # Handle the 'transform' keyword
        self.transform_method = _kw.pop("transform", "A")
        regions = _kw.pop("regions", None)
        if self.transform_method not in "AB":
            raise ValueError(f"transform={self.transform_method!r}")
        elif self.transform_method == "B":
            if (p, e) != ("IEA", "2024"):
                raise ValueError(
                    f"transform='B' only supported for (provider='IEA', "
                    f"edition='2024'); got {(p, e)!r}"
                )
            elif regions != "R12":
                raise ValueError(
                    f"transform='B' only supported for regions='R12'; got {regions!r}"
                )

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
            genno.Quantity(
                load_data(
                    provider=self.provider, edition=self.edition, path=self.path
                ).set_index(DIMS)["Value"],
                units="TJ",
            )
            .rename({"COUNTRY": "n", "TIME": "y", "FLOW": "flow", "PRODUCT": "product"})
            .sel(self.indexers, drop=True)
        )

    def transform(self, c: "genno.Computer", base_key: "genno.Key") -> "genno.Key":
        """Prepare `c` to transform raw data from `base_key`.

        1. Map IEA ``COUNTRY`` codes to ISO 3166-1 alpha-3 codes, where such mapping
           exists. See :func:`get_mapping` and :data:`COUNTRY_NAME`.

        The next steps depend on whether :py:`transform="A"` or :py:`transform="B"` was
        given with the `source_kw`.

        :py:`transform="A"` (default)
           2. Aggregate using "n::groups"—the same as :meth:`.ExoDataSource.transform`.
              This operates on the |n| labels transformed to alpha-3 codes by step (1)
              above.

        :py:`transform="B"`
           2. Compute intermediate quantities using :func:`.transform_B`.
           3. Aggregate using the groups returned by :func:`get_node_groups_B`.

        This method does *not* prepare interpolation or aggregation on |y|.
        """
        # Map values like RUSSIA appearing in the (IEA, 2024) edition to e.g. RUS
        adapter = get_mapping(self.provider, self.edition)
        k = c.add(base_key + "adapted", adapter, base_key)

        if self.transform_method == "A":
            # Key for aggregation groups: hierarchy from the standard code lists,
            # already added by .exo_data.prepare_computer()
            k_n_agg: "KeyLike" = "n::groups"
        elif self.transform_method == "B":
            # Derive intermediate values "_IIASA_{AFR,PAS,SAS}"
            k = c.add(base_key + "adapted" + "2", transform_B, k)

            # Add groups for aggregation, including these intermediate values
            k_n_agg = single_key(c.add(f"n::groups+{self.id}", get_node_groups_B))

        # Aggregate on 'n' dimension using the `k_n_agg`
        return single_key(c.add(base_key + "agg", "aggregate", k, k_n_agg, keep=False))


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

    For each code in the IEA 'COUNTRY' code list for (`provider`, `edition`) that is a
    country name, the adapter maps the name to a corresponding ISO 3166-1 alpha-3 code.
    :data:`COUNTRY_NAME` is used for values particular to IEA EWEB.

    Using the adapter makes data suitable for aggregation using the
    :mod:`message_ix_models` ``node`` code lists, which include those alpha-3 codes as
    children of each region code.
    """
    from message_ix_models.util import MappingAdapter, pycountry
    from message_ix_models.util.sdmx import read

    maps: dict[str, list[tuple[str, str]]] = dict()
    for concept, dim in ("COUNTRY", "n"), ("FLOW", "flow"), ("PRODUCT", "product"):
        if concept == "COUNTRY":
            cl = read(f"IEA:{concept}_{provider}({edition})")
            pycountry.COUNTRY_NAME.update(COUNTRY_NAME)

            maps[dim] = list()
            for code in cl:
                new_id = pycountry.iso_3166_alpha_3(code.id) or code.id
                maps[dim].append((code.id, new_id))

    return MappingAdapter(maps)


def get_node_groups_B() -> dict[Literal["n"], dict[str, list[str]]]:
    """Return groups for aggregating on |n| as part of the :py:`transform='B'` method.

    These are of three kinds:

    1. For the nodes 'R12_FSU', 'R12_PAO', 'R12_RCPA', 'R12_WEU', the common :ref:`R12`
       is used.
    2. For the nodes 'R12_CHN', 'R12_MEA', 'R12_LAM', 'R12_NAM', the labels in the
       ``material-region`` annotation are used. These may reference certain labels
       specific to IEA EWEB; omit certain alpha-3 codes present in (1); or both.
    3. For the nodes 'R12_AFR',  'R12_PAS', 'R12_SAS', a mix of the codes generated by
       :func:`transform_B` and alpha-3 codes are used.

    .. note:: This function mirrors the behaviour of code that is not present in
       :mod:`message_ix_models` using a file :file:`R12_SSP_V1.yaml` that is also not
       present. See
       `iiasa/message-ix-models#201 <https://github.com/iiasa/message-ix-models/pull/201#pullrequestreview-2144852265>`_
       for a detailed discussion.

    See also
    --------
    .IEA_EWEB.transform
    """
    result = dict(
        R12_AFR=["_IIASA_AFR"],
        R12_PAS="KOR IDN MYS MMR PHL SGP THA BRN TWN _IIASA_PAS".split(),
        R12_SAS="BGD IND NPL PAK LKA _IIASA_SAS".split(),
    )

    cl = get_codelist("node/R12")
    for n in "R12_FSU", "R12_PAO", "R12_RCPA", "R12_WEU":
        result[n] = [c.id for c in cl[n].child]
    for n in "R12_CHN", "R12_MEA", "R12_EEU", "R12_LAM", "R12_NAM":
        result[n] = cl[n].eval_annotation(id="material-region")

    return dict(n=result)


def transform_B(qty: "AnyQuantity") -> "AnyQuantity":
    """Compute some derived intermediate labels along the |n| dimension of `qty`.

    These are used via :meth:`.IEA_EWEB.transform` in the aggregations specified by
    :func:`get_node_groups_B`.

    1. ``_IIASA_AFR = AFRICA - DZA - EGY - LBY - MAR - SDN - SSD - TUN``. Note that
       'AFRICA' is 'AFRICATOT' in the reference notebook, but no such label appears in
       the data.
    2. ``_IIASA_PAS = UNOCEANIA - AUS - NZL``. Note that 'UNOCEANIA' is 'OCEANIA' in the
       reference notebook, but no such label appears in the data.
    3. ``_IIASA_SAS = OTHERASIA - _IIASA_PAS``.

    .. note:: This function mirrors the behaviour of code in a file
       :file:`Step2_REGIONS.ipynb` that is not present in :mod:`message_ix_models`.

    Returns
    -------
    genno.Quantity
       the original `qty` with 3 appended |n| labels as above.
    """
    n_afr = ["DZA", "EGY", "LBY", "MAR", "SDN", "SSD", "TUN"]
    q_afr = qty.sel(n="AFRICA", drop=True) - qty.sel(n=n_afr).sum(dim="n")
    q_pas = qty.sel(n="UNOCEANIA", drop=True) - qty.sel(n=["AUS", "NZL"]).sum(dim="n")
    q_sas = qty.sel(n="OTHERASIA", drop=True) - q_pas

    return concat(
        qty,
        q_afr.expand_dims({"n": ["_IIASA_AFR"]}),
        q_pas.expand_dims({"n": ["_IIASA_PAS"]}),
        q_sas.expand_dims({"n": ["_IIASA_SAS"]}),
    )
