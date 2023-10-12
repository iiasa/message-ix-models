import logging
from copy import copy

from message_ix_models.tools.exo_data import (
    ExoDataSource,
    iamc_like_data_for_query,
    register_source,
)
from message_ix_models.util import (
    HAS_MESSAGE_DATA,
    package_data_path,
    private_data_path,
)

log = logging.getLogger(__name__)


@register_source
class SSPOriginal(ExoDataSource):
    """Provider of exogenous data from the original SSP database.

    In the `source_kw` to :func:`.exo_data.prepare_computer`, the following are valid:

    ``model``
       - IIASA POP
       - IIASA-WiC POP
       - NCAR
       - OECD Env-Growth
       - PIK GDP-32

    The measures available differ according to the model; see the source data for
    details.
    """

    id = "SSP"

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

        *parts, self.ssp_number = source.partition(s)

        # Map the `measure` keyword to a string appearing in the data
        _kw = copy(source_kw)
        self.measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[_kw.pop("measure")]

        # Store the model ID, if any
        self.model = _kw.pop("model", None)

        # Determine the date based on the model ID. There is a 1:1 correspondence.
        self.date = self.model_date[self.model]

        if len(_kw):
            raise ValueError(_kw)

    def __call__(self):
        # Assemble a query string
        extra = "d" if self.ssp_number == "4" and self.model == "IIASA-WiC POP" else ""
        query = " and ".join(
            [
                f"SCENARIO == 'SSP{self.ssp_number}{extra}_v9_{self.date}'",
                f"VARIABLE == '{self.measure}'",
                f"MODEL == '{self.model}'" if self.model else "True",
            ]
        )
        log.debug(query)

        parts = ("ssp", "SspDb_country_data_2013-06-12.csv.zip")
        if HAS_MESSAGE_DATA:
            path = private_data_path(*parts)
        else:
            path = package_data_path("test", *parts)
            log.warning(f"Reading random data from {path}")

        return iamc_like_data_for_query(path, query, replace=self.replace)


@register_source
class SSPUpdate(ExoDataSource):
    """Provider of exogenous data from the SSP Update database."""

    id = "SSP update"

    def __init__(self, source, source_kw):
        s = "ICONICS:SSP(2024)."
        if not source.startswith(s):
            raise ValueError(source)

        *parts, self.ssp_number = source.partition(s)

        # Map the `measure` keyword to a string appearing in the data
        _kw = copy(source_kw)
        self.measure = {
            "GDP": "GDP|PPP",
            "POP": "Population",
        }[_kw.pop("measure")]

        # Store the model ID, if any
        self.model = _kw.pop("model", None)

        if len(_kw):
            raise ValueError(_kw)

    def __call__(self):
        # Assemble a query string
        query = " and ".join(
            [
                f"Scenario == 'SSP{self.ssp_number} - Review Phase 1'",
                f"Variable == '{self.measure}'",
                f"Model == '{self.model}'" if self.model else "True",
            ]
        )

        parts = ("ssp", "SSP-Review-Phase-1.csv.gz")
        if HAS_MESSAGE_DATA:
            path = private_data_path(*parts)
        else:
            path = package_data_path("test", *parts)
            log.warning(f"Reading random data from {path}")

        return iamc_like_data_for_query(path, query)
