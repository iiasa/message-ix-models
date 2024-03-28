from dataclasses import dataclass, field
from typing import List, Literal, Optional

from message_ix_models import ScenarioInfo


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

    #: Final year for projections. Note that the default is different from the final
    #: model year of 2110 commonly used in MESSAGEix-GLOBIOM (:doc:`/pkg-data/year`).
    final_year: int = 2100

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

    #: TODO Document the meaning of this setting.
    pre_last_year_rate: float = 0.01

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

    # Internal: Scenario Info object used for y0, Y, seq_years
    _info: ScenarioInfo = field(default_factory=ScenarioInfo)

    def __post_init__(self):
        from message_ix_models.model.structure import get_codes

        if self.ref_region is None:
            self.ref_region = f"{self.node}_NAM"

        # Default periods 'B'
        self._info.year_from_codes(get_codes("year/B"))

    @property
    def y0(self) -> int:
        """The first model period."""
        return self._info.y0

    @property
    def Y(self) -> List[int]:
        """List of model periods."""
        return self._info.Y

    @property
    def seq_years(self) -> List[int]:
        """Similar to :attr:`Y`.

        This list of periods differs in that it:

        1. Excludes periods after :attr:`.final_year`.
        2. Includes 5-year periods even when these are not in :attr:`.Y`.
        """
        return list(range(self.y0, self.final_year + 1, 5))

    def check(self):
        """Validate settings."""
        valid_nodes = {"R11", "R12", "R20"}
        if self.node not in valid_nodes:
            raise NotImplementedError(
                f"Cost projections for {self.node!r}; use one of {valid_nodes}"
            )
