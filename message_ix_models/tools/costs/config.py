from dataclasses import dataclass
from typing import Literal, Optional

ADJ_BASE_YEAR = 2020
FIRST_MODEL_YEAR = 2020  # FIXME Read from year/A or year/B
LAST_MODEL_YEAR = 2100  # FIXME Clarify why this is not the same as 2110
PRE_LAST_YEAR_RATE = 0.01
TIME_STEPS = 5  # FIXME Read from year/A or year/B


@dataclass
class Config:
    """Configuration for :mod:`.costs`.

    On creation:

    - If not given, :attr:`.ref_region` is set based on :attr:`.node` using, for
      instance, :py:`ref_region="R12_NAM"` for :py:`node="R12"`.
    """

    #: Base year for projections.
    base_year: int = 2021

    #: Year of convergence; used when :attr:`.method` is "convergence". See
    #: :func:`.create_projections_converge`.
    convergence_year: int = 2050

    #: Rate of increase/decrease of fixed operating and maintenance costs.
    fom_rate: float = 0.025

    #: Format of output from :func:`.create_cost_projections`. One of:
    #:
    #: - "iamc": IAMC time series data structure.
    #: - "message": :mod:`message_ix` parameter data.
    format: Literal["iamc", "message"] = "message"

    #: Node code list / spatial resolution to use.
    node: Literal["R11", "R12", "R20"] = "R12"

    #: Projection method; one of:
    #:
    #: - "convergence": uses :func:`.create_projections_converge`
    #: - "gdp": :func:`.create_projections_gdp`
    #: - "learning": :func:`.create_projections_converge`
    method: Literal["convergence", "gdp", "learning"] = "gdp"

    #: Model variant to prepare data for.
    module: Literal["energy", "materials"] = "energy"

    #: Reference region; default "{node}_NAM" for a given :attr:`.node`.
    ref_region: Optional[str] = None

    #: Set of SSPs referenced by :attr:`scenario`. One of:
    #:
    #: - "original": :obj:`SSP_2017`
    #: - "updated": :obj:`SSP_2024`
    #: - "all": both of the above.
    scenario_version: Literal["original", "updated", "all"] = "updated"

    #: Scenario(s) for which to create data. "all" implies the remaining values.
    scenario: Literal["all", "LED", "SSP1", "SSP2", "SSP3", "SSP4", "SSP5"] = "all"

    def __post_init__(self):
        if self.ref_region is None:
            self.ref_region = f"{self.node}_NAM"

    def check(self):
        """Validate settings."""
        valid_nodes = {"R11", "R12", "R20"}
        if self.node not in valid_nodes:
            raise NotImplementedError(
                f"Cost projections for {self.node!r}; use one of {valid_nodes}"
            )
