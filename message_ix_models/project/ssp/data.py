import logging

from message_ix_models.tools.exo_data import ExoDataSource, register_source
from message_ix_models.tools.iamc import iamc_like_data_for_query
from message_ix_models.util import path_fallback

__all__ = [
    "SSPOriginal",
    "SSPUpdate",
]

log = logging.getLogger(__name__)

#: :py:`where` argument to :func:`path_fallback`, used by both :class:`.SSPOriginal` and
#: :class:`.SSPUpdate`. In order:
#:
#: 1. Currently data is stored in message-static-data, cloned and linked from within the
#:    user's 'local' data directory.
#: 2. Previously some files were stored directly within message_ix_models (available in
#:    an editable install from a clone of the git repository, 'package') or in
#:    :mod:`message_data` ('private'). These settings are only provided for backward
#:    compatibility.
#: 3. If the above are not available, use the fuzzed/random test data ('test').
WHERE = "local package private test"


@register_source
class SSPOriginal(ExoDataSource):
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

    id = "SSP"

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

    def __init__(self, source, source_kw):
        s = "ICONICS:SSP(2017)."
        if not source.startswith(s):
            raise ValueError(source)

        *parts, ssp_id = source.partition(s)

        # Map the `measure` keyword to a string appearing in the data
        self.measure = source_kw.pop("measure")
        measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[self.measure]

        # Store the model ID, if any
        model = source_kw.pop("model", None)

        # Determine the date based on the model ID. There is a 1:1 correspondence.
        date = self.model_date[model]

        self.raise_on_extra_kw(source_kw)

        # Identify input data path
        self.path = path_fallback("ssp", self.filename, where=WHERE)
        if "test" in self.path.parts:
            log.warning(f"Read random data from {self.path}")

        # Assemble a query string
        extra = "d" if ssp_id == "4" and model == "IIASA-WiC POP" else ""
        self.query = (
            f"SCENARIO == 'SSP{ssp_id}{extra}_v9_{date}' and VARIABLE == '{measure}'"
            + (f" and MODEL == '{model}'" if model else "")
        )

    def __call__(self):
        # Use prepared path, query, and replacements
        return iamc_like_data_for_query(self.path, self.query, replace=self.replace)


@register_source
class SSPUpdate(ExoDataSource):
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

    id = "SSP update"

    #: File names containing the data, according to the release.
    filename = {
        "3.0": "1706548837040-ssp_basic_drivers_release_3.0_full.csv.gz",
        "3.0.1": "1710759470883-ssp_basic_drivers_release_3.0.1_full.csv.gz",
        "3.1": "1721734326790-ssp_basic_drivers_release_3.1_full.csv.gz",
        "preview": "SSP-Review-Phase-1.csv.gz",
    }

    def __init__(self, source, source_kw):
        s = "ICONICS:SSP(2024)."
        if not source.startswith(s):
            raise ValueError(source)

        *parts, ssp_id = source.partition(s)

        # Map the `measure` keyword to a 'Variable' dimension code
        self.measure = source_kw.pop("measure")
        measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[self.measure]

        # Store the model code, if any
        model = source_kw.pop("model", None)

        # Identify the data release date/version/label
        release = source_kw.pop("release", "3.0")

        self.raise_on_extra_kw(source_kw)

        # Replacements to apply, if any
        self.replace = {}

        # Prepare query pieces
        models = []
        scenarios = []

        if release in ("3.1", "3.0.1", "3.0"):
            scenarios.append(f"SSP{ssp_id}")

            if measure == "GDP|PPP":
                # Configure to prepend (m="OECD…", s="Historical Reference")
                # observations to series
                models.extend({model, "OECD ENV-Growth 2023"})
                scenarios.append("Historical Reference")
                self.replace.update(
                    Model={"OECD ENV-Growth 2023": model},
                    Scenario={"Historical Reference": scenarios[0]},
                )
        elif release == "preview":
            models.extend([model] if model is not None else [])
            scenarios.append(f"SSP{ssp_id} - Review Phase 1")
        else:
            log.error(
                f"{release = } invalid for {type(self)}; expected one of: "
                f"{set(self.filename)}"
            )
            raise ValueError(release)

        # Identify input data path
        self.path = path_fallback("ssp", self.filename[release], where=WHERE)
        if "test" in self.path.parts:
            log.warning(f"Read random data from {self.path}")

        # Assemble and store a query string
        self.query = f"Scenario in {scenarios!r} and Variable == '{measure}'" + (
            f"and Model in {models!r}" if models else ""
        )

    def __call__(self):
        # Use prepared path, query, and replacements
        return iamc_like_data_for_query(self.path, self.query, replace=self.replace)
