import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import filterfalse
from typing import TYPE_CHECKING, Union

from genno import Keys

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import path_fallback

if TYPE_CHECKING:
    from genno import Computer, Key
    from genno.types import AnyQuantity

__all__ = [
    "SSPOriginal",
    "SSPUpdate",
]

log = logging.getLogger(__name__)


class SSPDataSource(ExoDataSource):
    """Common base class for :class:`.SSPOriginal` and :class:`.SSPUpdate`."""

    @dataclass
    class Options(BaseOptions):
        #: Model name.
        model: str = ""

        #: Partial URN for a code in the SSP code list, e.g. "ICONICS:SSP(2017).1".
        #: :attr:`ssp_id` **should** be preferred.
        source: str = ""

        #: Short ID of the SSP code, e.g. "1".
        ssp_id: str = ""

        def handle_source(self, prefix: str) -> None:
            """Check that :attr:`source` starts with `prefix`; update :attr:`ssp_id`."""
            if not self.source:
                return

            prefix += "."
            _, sep, ssp_id = self.source.rpartition(prefix)

            if sep != prefix:
                raise ValueError(f"{self.source!r} does not start with {prefix!r}")
            elif self.ssp_id and self.ssp_id != ssp_id:  # Mismatch
                raise ValueError(
                    f"Mismatch: {self.source=!r} != {prefix!r} + {self.ssp_id=!r}"
                )

            self.ssp_id = ssp_id

    options: Options

    #: Alias from short measure IDs to IAMC 'variable'. See :meth:`make_query`.
    variable = {"GDP": "GDP|PPP", "POP": "Population"}

    #: `replace` argument to :func:`iamc.to_quantity`.
    replace: dict[str, Union[str, dict[str, str]]] = {}

    #: `unique` argument to :func:`iamc.to_quantity`.
    unique: str = "MODEL SCENARIO VARIABLE UNIT"

    #: :py:`where` argument to :func:`path_fallback`. In order:
    #:
    #: 1. Currently data is stored in message-static-data, cloned and linked from within
    #:    the user's 'local' data directory.
    #: 2. Previously some files were stored directly within message_ix_models (available
    #:    in an editable install from a clone of the git repository, 'package') or in
    #:    :mod:`message_data` ('private'). These settings are only provided for backward
    #:    compatibility.
    #:
    #: Fuzzed/random test data ('test') is also available, but not enabled by default.
    where = ["local", "package", "private"]

    def get(self):
        # Use prepared path, query, and replacements
        return iamc_like_data_for_query(
            self.path, self.query, replace=self.replace, unique=self.unique
        )

    def make_query(
        self,
        dim_case: Callable[[str], str],
        model_scenario: Iterable[tuple[str, str]],
        unit: str,
    ) -> None:
        """Assemble and store a :meth:`pandas.DataFrame.query` string.

        Parameters
        ----------
        dim_case :
            Function to apply to IAMC dimension IDs, for instance :meth:`str.upper` to
            use "MODEL".
        model_scenario :
            Iterable of (`model_name`, `scenario_name`) pairs. `model_name` **may** be
            an empty string.
        unit :
            Units. **May** be an empty string.
        """
        # Map the `measure` option to an IAMC 'variable' label appearing in the data
        variable = self.variable[self.options.measure]

        parts = [
            f"{dim_case('variable')} == {variable!r} and",
            f"{dim_case('unit')} == {unit!r} and" if unit else "",
            "(False",
        ]

        # Add query pieces for desired combinations of (model name, scenario name)
        for m, s in model_scenario:
            parts.extend(
                [
                    f"or ({dim_case('scenario')} == {s!r}",
                    f"and {dim_case('model')} == {m!r})" if m else ")",
                ]
            )

        self.query = " ".join(parts + [")"])
        log.debug(f"query: {self.query!s}")


@register_source
class SSPOriginal(SSPDataSource):
    """Provider of exogenous data from the original SSP database.

    This database is accessible from https://tntcat.iiasa.ac.at/SspDb/dsd.

    To use data from this source:

    1. Read the general documentation for :mod:`.project.ssp.data`.
    2. If necessary, obtain copy of the original data file(s).
    3. Call :meth:`.SSPOriginal.add_tasks` with keyword arguments corresponding to
       :class:`SSPDataSource.Options`. In particular:

       - :attr:`~SSPDataSource.Options.model` **should** be one of:

         - IIASA GDP
         - IIASA-WiC POP
         - NCAR
         - OECD Env-Growth
         - PIK GDP-32

      - :attr:`~SSPDataSource.Options.measure`: The measures available differ according
        to the model; see the source data for details.
      - :attr:`~SSPDataSource.Options.unit` is not recognized/has no effect.

    Example
    -------
    >>> keys = SSPOriginal.add_tasks(
    ...     computer, context, ssp_id="3", measure="POP", model="IIASA-WiC POP",
    ... )
    >>> result = computer.get(keys[0])
    """

    #: Name of file containing the data.
    filename = "SspDb_country_data_2013-06-12.csv.zip"

    #: One-to-one correspondence between "model" codes and date fragments in scenario
    #: codes.
    model_date = {
        "IIASA GDP": "130219",
        "IIASA-WiC POP": "130115",
        "NCAR": "130115",
        "OECD Env-Growth": "130325",
        "PIK GDP-32": "130424",
    }

    #: Replacements to apply when loading the data.
    replace = {"billion US$2005/yr": "billion USD_2005/yr"}

    def __init__(self, *args, **kwargs) -> None:
        opt = self.options = self.Options.from_args(self, *args, **kwargs)
        opt.handle_source("ICONICS:SSP(2017)")

        # Identify input data path
        self.path = path_fallback("ssp", self.filename, where=self._where())

        # Create .key
        super().__init__()

        # Extra pieces for scenario identifier
        # - Determine a date based on the model ID. There is a 1:1 correspondence.
        # - Append "d" in a certain case
        s_extra = "d" if (opt.ssp_id == "4" and opt.model == "IIASA-WiC POP") else ""
        date = self.model_date[opt.model]
        scenario = f"SSP{opt.ssp_id}{s_extra}_v9_{date}"

        # Assemble and store a query string
        self.make_query(str.upper, [(opt.model, scenario)], "")


@register_source
class SSPUpdate(SSPDataSource):
    """Provider of exogenous data from the SSP Update database.

    This database is accessible from https://data.ece.iiasa.ac.at/ssp.

    To use data from this source:

    1. Read the general documentation for :mod:`.project.ssp.data`.
    2. If necessary, obtain copy of the original data file(s).
    3. Call :meth:`.SSPUpdate.add_tasks` with keyword arguments corresponding to
       :class:`SSPUpdate.Options`. In particular:

       - For :attr:`~Options.release` up to "3.1" and :attr:`~Options.measure` "GDP",
         :attr:`~Options.model` **must** be one of "IIASA GDP 2023" or
         "OECD ENV-Growth 2023".
       - For :attr:`~Options.release` "3.2.beta" and :attr:`~Options.measure` "GDP":

         - :attr:`~Options.model` **must** be "OECD ENV-Growth 2025".
         - :attr:`~Options.unit` **must** be given, with a value such as
           "billion USD_2010/yr", "billion USD_2015/yr", or "billion USD_2017/yr".
           Without these, keys are not unique.

    Example
    -------
    >>> keys = SSPUpdate.add_tasks(
    ...     computer,
    ...     context,
    ...     release="3.1",
    ...     ssp_id="3",
    ...     measure="GDP"
    ...     model="IIASA GDP 2023",
    ... )
    >>> result = computer.get(keys[0])
    """

    @dataclass
    class Options(SSPDataSource.Options):
        #: Release: one of the keys of :attr:`filename`. **Required**.
        release: str = ""

        #: Selector for the IAMC 'UNIT' dimension.
        unit: str = ""

    options: Options

    #: File names containing the data, according to the release.
    filename = {
        "3.0": "1706548837040-ssp_basic_drivers_release_3.0_full.csv.gz",
        "3.0.1": "1710759470883-ssp_basic_drivers_release_3.0.1_full.csv.gz",
        "3.1": "1721734326790-ssp_basic_drivers_release_3.1_full.csv.gz",
        "3.2.beta": "0000000000000-ssp_basic_drivers_release_3.2.beta_full.csv.xz",
        "preview": "SSP-Review-Phase-1.csv.gz",
    }

    def __init__(self, *args, **kwargs) -> None:
        opt = self.options = self.Options.from_args(self, *args, **kwargs)
        opt.handle_source("ICONICS:SSP(2024)")

        # Identify input data path
        self.path = path_fallback(
            "ssp", self.filename[opt.release], where=self._where()
        )

        super().__init__()  # Create .key

        # Replacements to apply, if any
        self.replace = {}

        # Prepare query pieces
        m_s = []
        scenario = f"SSP{opt.ssp_id}"
        if (opt.release, opt.measure) == ("3.0", "GDP") or opt.release in (
            "3.0.1",
            "3.1",
            "3.2.beta",
        ):
            # Configure to also load data for (m=…, s="Historical Reference")
            # These data will be deduplicated later in .transform()

            # Model name for historical data
            m_hist = "OECD ENV-Growth" if opt.measure == "GDP" else "IIASA-WiC POP"
            if opt.release in ("3.2.beta",):
                m_hist += " 2025"
                # Period in which historical and project data overlap
                self.y_overlap = 2025
            else:
                m_hist += " 2023"
                self.y_overlap = 2020

            # Also retrieve data for this (model name, scenario name) pair
            m_s.append((m_hist, "Historical Reference"))

            # Map the model name for historical data to the same value
            self.replace.update(Model={m_hist: opt.model})

            # Result of iamc_like_data_for_query() will *not* have unique 'SCENARIO'
            # This also signals to .transform() to deduplicate
            self.unique = "MODEL VARIABLE UNIT"
        elif opt.release == "preview":
            # Add scenario name suffix
            scenario += " - Review Phase 1"
        elif opt.release not in self.filename:
            msg = (
                f"{opt.release = } invalid for {type(self)}; expected one of: "
                f"{set(self.filename)}"
            )
            log.error(msg)
            raise ValueError(msg)

        # Select the indicated (model, scenario)
        m_s.append((opt.model, scenario))

        # Assemble and store a query string
        self.make_query(str.title, m_s, opt.unit)

    def transform(self, c: "Computer", base_key: "Key") -> "Key":
        """Add tasks to `c` to transform raw data from `base_key`.

        If necessary, data for period 2020 or 2025 are deduplicated."""
        k = Keys(
            base=base_key,
            # Use id of the class instance to avoid duplication in multiple use
            idx=f"indexers:SCENARIO-y:SSPUpdate+{id(self)}",
            result=base_key,
        )

        if "SCENARIO" not in self.unique:
            # Construct indexers; select only historical or projected data
            # This discards data for (s="Historical Reference", y=2020 or 2025)
            for tag, sense in ("hist", False), ("proj", True):
                c.add(k.idx[tag], make_indexers, k.base, sense=sense, y0=self.y_overlap)
                c.add(k.base[tag], "select", k.base, k.idx[tag])

            # Concatenate again
            k.result = k.base + "deduplicated"
            c.add(k.result, "concat", k.base["hist"], k.base["proj"])

        return super().transform(c, k.result)


def make_indexers(qty: "AnyQuantity", *, sense: bool, y0: int) -> dict:
    """Construct indexers for :func:`genno.operator.select`.

    The indexers include subsets of the coords of `qty`:

    - "SCENARIO" dimension: a single value, either "Historical Reference" or something
      else.
    - "y" dimension: a sorted :class:`list` of :class:`int`; either those less than
      `y0`, or `y0` and greater.

    Used in :meth:`SSPUpdate.transform`.

    Parameters
    ----------
    sense :
        if :any:`True`, return indexers for projected values; otherwise for
        "Historical Reference" values.
    """
    func = filter if sense else filterfalse
    result = {
        dim: sorted(func(predicate, qty.coords[dim].data))
        for dim, predicate in (
            ("SCENARIO", lambda s: s != "Historical Reference"),
            ("y", lambda y: y >= y0),
        )
    }
    if 1 != len(result["SCENARIO"]):
        raise ValueError(
            f"Must yield exactly 1 scenario label; got {result['SCENARIO']} from "
            f"{qty.coords['SCENARIO'].data}"
        )
    result.update(SCENARIO=result["SCENARIO"][0])
    return result
