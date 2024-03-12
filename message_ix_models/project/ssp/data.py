import logging

from message_ix_models.tools.exo_data import (
    ExoDataSource,
    iamc_like_data_for_query,
    register_source,
)
from message_ix_models.util import package_data_path, private_data_path

__all__ = [
    "SSPOriginal",
    "SSPUpdate",
]

log = logging.getLogger(__name__)


@register_source
class SSPOriginal(ExoDataSource):
    """Provider of exogenous data from the original SSP database.

    To use data from this source, call :func:`.exo_data.prepare_computer` with the
    arguments:

    - `source`: Any value from :data:`.SSP_2017` or equivalent string, for instance
      "ICONICS:SSP(2017).2". The specific SSP for which data is returned is determined
      from the value.
    - `source_kw` including:

      - "model": one of:

          - IIASA GDP
          - IIASA-WiC POP
          - NCAR
          - OECD Env-Growth
          - PIK GDP-32

      - "measure": The measures available differ according to the model; see the source
        data for details.

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
        measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[source_kw.pop("measure")]

        # Store the model ID, if any
        model = source_kw.pop("model", None)

        # Determine the date based on the model ID. There is a 1:1 correspondence.
        date = self.model_date[model]

        self.raise_on_extra_kw(source_kw)

        # Assemble a query string
        extra = "d" if ssp_id == "4" and model == "IIASA-WiC POP" else ""
        self.query = (
            f"SCENARIO == 'SSP{ssp_id}{extra}_v9_{date}' and VARIABLE == '{measure}'"
            + (f" and MODEL == '{model}'" if model else "")
        )
        # log.debug(query)

        # Iterate over possible locations for the data file
        dirs = [private_data_path("ssp"), package_data_path("test", "ssp")]
        for path in [d.joinpath(self.filename) for d in dirs]:
            if not path.exists():
                log.info(f"Not found: {path}")
                continue
            if "test" in path.parts:
                log.warning(f"Reading random data from {path}")
            break

        self.path = path

    def __call__(self):
        # Use prepared path, query, and replacements
        return iamc_like_data_for_query(self.path, self.query, replace=self.replace)


@register_source
class SSPUpdate(ExoDataSource):
    """Provider of exogenous data from the SSP Update database.

    To use data from this source, call :func:`.exo_data.prepare_computer` with the
    arguments:

    - `source`: Any value from :data:`.SSP_2024` or equivalent string, for instance
      "ICONICS:SSP(2024).2".
    - `release`: One of "3.0" or "preview".

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
        "preview": "SSP-Review-Phase-1.csv.gz",
    }

    def __init__(self, source, source_kw):
        s = "ICONICS:SSP(2024)."
        if not source.startswith(s):
            raise ValueError(source)

        *parts, ssp_id = source.partition(s)

        # Map the `measure` keyword to a 'Variable' dimension code
        measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[source_kw.pop("measure")]

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

        if release == "3.0":
            # Directories in which to locate `self.filename`; stored directly within
            # message_ix_models
            dirs = [package_data_path("ssp")]

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
            # Look first in message_data, then in message_ix_models test data
            dirs = [private_data_path("ssp"), package_data_path("test", "ssp")]

            scenarios.append(f"SSP{ssp_id} - Review Phase 1")
        else:
            log.error(
                f"{release = } invalid for {type(self)}; expected one of: "
                f"{set(self.filename)}"
            )
            raise ValueError(release)

        # Assemble and store a query string
        self.query = f"Scenario in {scenarios!r} and Variable == '{measure}'" + (
            f"and Model in {models!r}" if models else ""
        )
        # log.info(f"{self.query = }")

        # Iterate over possible locations for the data file
        for path in [d.joinpath(self.filename[release]) for d in dirs]:
            if not path.exists():
                log.info(f"Not found: {path}")
                continue
            if "test" in path.parts:
                log.warning(f"Reading random data from {path}")
            break

        self.path = path

    def __call__(self):
        # Use prepared path, query, and replacements
        return iamc_like_data_for_query(self.path, self.query, replace=self.replace)
