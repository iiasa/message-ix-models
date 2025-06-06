import logging
from dataclasses import dataclass
from typing import Union

from message_ix_models.tools.exo_data import BaseOptions, ExoDataSource, register_source
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import path_fallback

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
        source: str = ""

        #: Short id of the SSP, e.g. "1".
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

    #: Alias from short measure IDs to IAMC 'variable'.
    variable = {"GDP": "GDP|PPP", "POP": "Population"}

    #: Replacements.
    replace: dict[str, Union[str, dict[str, str]]] = {}

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
        return iamc_like_data_for_query(self.path, self.query, replace=self.replace)


@register_source
class SSPOriginal(SSPDataSource):
    """Provider of exogenous data from the original SSP database.

    This database is accessible from https://tntcat.iiasa.ac.at/SspDb/dsd.

    To use data from this source:

    1. Read the general documentation for :mod:`.project.ssp.data`.
    2. If necessary, obtain copy of the original data file(s).
    3. Call :func:`.exo_data.prepare_computer` with the arguments:

       - `source`: Any value from :data:`.SSP_2017` or equivalent string, for instance
         "ICONICS:SSP(2017).2". The specific SSP for which data is returned is
         determined from the value.
       - `source_kw` including:

         - "model": one of:

           - IIASA GDP
           - IIASA-WiC POP
           - NCAR
           - OECD Env-Growth
           - PIK GDP-32

         - "measure": The measures available differ according to the model; see the
           source data for details.

    Example
    -------
    >>> keys = prepare_computer(
    ...     context,
    ...     computer,
    ...     source="ICONICS:SSP(2015).3",
    ...     source_kw=dict(measure="POP", model="IIASA-WiC POP"),
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

        super().__init__()  # Create .key

        # Map the `measure` option to an IAMC 'variable' label appearing in the data
        v = self.variable[opt.measure]

        # Determine the date based on the model ID. There is a 1:1 correspondence.
        date = self.model_date[opt.model]

        # Identify input data path
        self.path = path_fallback("ssp", self.filename, where=self._where())

        # Assemble a query string
        extra = "d" if opt.ssp_id == "4" and opt.model == "IIASA-WiC POP" else ""
        self.query = (
            f"SCENARIO == 'SSP{opt.ssp_id}{extra}_v9_{date}' and VARIABLE == {v!r} and "
            + (f"MODEL == {opt.model!r}" if opt.model else "True")
        )


@register_source
class SSPUpdate(SSPDataSource):
    """Provider of exogenous data from the SSP Update database.

    This database is accessible from https://data.ece.iiasa.ac.at/ssp.

    To use data from this source:

    1. Read the general documentation for :mod:`.project.ssp.data`.
    2. If necessary, obtain copy of the original data file(s).
    3. Call :func:`.exo_data.prepare_computer` with the arguments:

       - `source`: Any value from :data:`.SSP_2024` or equivalent string, for instance
         "ICONICS:SSP(2024).2".
       - `release`: One of "3.1", "3.0.1", "3.0", or "preview".

    Example
    -------
    >>> keys = prepare_computer(
    ...     context,
    ...     computer,
    ...     source="ICONICS:SSP(2024).3",
    ...     source_kw=dict(measure="GDP", model="IIASA GDP 2023"),
    ... )
    >>> result = computer.get(keys[0])
    """

    @dataclass
    class Options(SSPDataSource.Options):
        #: Release.
        release: str = ""

    options: Options

    #: File names containing the data, according to the release.
    filename = {
        "3.0": "1706548837040-ssp_basic_drivers_release_3.0_full.csv.gz",
        "3.0.1": "1710759470883-ssp_basic_drivers_release_3.0.1_full.csv.gz",
        "3.1": "1721734326790-ssp_basic_drivers_release_3.1_full.csv.gz",
        "preview": "SSP-Review-Phase-1.csv.gz",
    }

    def __init__(self, *args, **kwargs) -> None:
        opt = self.options = self.Options.from_args(self, *args, **kwargs)
        opt.handle_source("ICONICS:SSP(2024)")

        super().__init__()  # Create .key

        # Map the `measure` option to an IAMC 'variable' label appearing in the data
        v = self.variable[opt.measure]

        # Replacements to apply, if any
        self.replace = {}

        # Prepare query pieces
        models: list[str] = []
        scenarios = []

        if opt.release in ("3.1", "3.0.1", "3.0"):
            scenarios.append(f"SSP{opt.ssp_id}")

            if opt.measure == "GDP":
                # Configure to prepend (m="OECD…", s="Historical Reference")
                # observations to series
                models.extend({opt.model, "OECD ENV-Growth 2023"})
                scenarios.append("Historical Reference")
                self.replace.update(
                    Model={"OECD ENV-Growth 2023": opt.model},
                    Scenario={"Historical Reference": scenarios[0]},
                )
        elif opt.release == "preview":
            models.extend([opt.model] if opt.model else [])
            scenarios.append(f"SSP{opt.ssp_id} - Review Phase 1")
        else:
            log.error(
                f"{opt.release = } invalid for {type(self)}; expected one of: "
                f"{set(self.filename)}"
            )
            raise ValueError(opt.release)

        # Identify input data path
        self.path = path_fallback(
            "ssp", self.filename[opt.release], where=self._where()
        )

        # Assemble and store a query string
        self.query = f"Scenario in {scenarios!r} and Variable == {v!r} and " + (
            f"Model in {models!r}" if models else "True"
        )
