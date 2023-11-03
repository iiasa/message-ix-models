from dataclasses import dataclass
from typing import Literal, Optional

BASE_YEAR = 2021
ADJ_BASE_YEAR = 2020
FIRST_MODEL_YEAR = 2020
LAST_MODEL_YEAR = 2100
PRE_LAST_YEAR_RATE = 0.01
TIME_STEPS = 5
HORIZON_START = 1960
HORIZON_END = 2110


# Conversion rate from 2021 USD to 2005 USD
# Taken from https://www.officialdata.org/us/inflation/2021?endYear=2005&amount=1
CONVERSION_2021_TO_2005_USD = 0.72


@dataclass
class Config:
    """Configuration for :mod:`.costs`."""

    test_val: int = 2

    #: Base year for projections.
    base_year: int = BASE_YEAR

    #: Year of convergence; used when :attr:`.method` is "convergence". See
    #: :func:`.create_projections_converge`.
    convergence_year: int = 2050

    #: Rate of increase/decrease of fixed operating and maintenance costs.
    fom_rate: float = 0.025

    #: Format of output. One of:
    #:
    #: - "iamc": IAMC time series data structure.
    #: - "message": :mod:`message_ix` parameter data.
    format: Literal["iamc", "message"] = "message"

    #: Spatial resolution
    node: Literal["R11", "R12", "R20"] = "R12"

    #: Projection method; one of:
    #:
    #: - "convergence": uses :func:`.create_projections_converge`
    #: - "gdp": :func:`.create_projections_gdp`
    #: - "learning": :func:`.create_projections_converge`
    method: Literal["convergence", "gdp", "learning"] = "gdp"

    #: Model variant to prepare data for.
    module: Literal["base", "materials"] = "base"

    #: Reference region; default "{node}_NAM".
    ref_region: Optional[str] = None

    #: Set of SSPs referenced by :attr:`scenario`. One of:
    #:
    #: - "original": :obj:`SSP_2017`
    #: - "updated": :obj:`SSP_2024`
    scenario_version: Literal["original", "updated"] = "updated"

    #: Scenario(s) for which to create data.
    scenario: Literal["all", "LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"] = "all"

    def __post_init__(self):
        if self.ref_region is None:
            self.ref_region = f"{self.node}_NAM"
