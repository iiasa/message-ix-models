"""NAVIGATE project."""

import logging
import operator
from collections.abc import Generator, Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from enum import Flag, auto
from functools import lru_cache, reduce
from typing import TYPE_CHECKING, Literal, Optional, Union

import yaml
from sdmx.model.common import Code
from sdmx.model.v21 import Annotation

from message_ix_models.model.workflow import Config as WfConfig
from message_ix_models.project.engage.workflow import PolicyConfig
from message_ix_models.util import as_codes, package_data_path

if TYPE_CHECKING:
    from sdmx.model.common import BaseAnnotation

log = logging.getLogger(__name__)


class T35_POLICY(Flag):
    """Flags for demand-side policies in Task 3.5."""

    REF = 0

    ACT = auto()
    ELE = auto()
    TEC = auto()

    ALL = ACT | ELE | TEC

    @classmethod
    def parse(cls, value):
        """Parse a NAVIGATE scenario from a string.

        Parameters
        ----------
        value : str
            Zero or more of "act", "ele", and/or "tec", joined with "+".
        """
        if isinstance(value, cls):
            return value
        try:
            return reduce(
                operator.or_,
                map(cls.__getitem__, filter(None, (value or "ref").upper().split("+"))),
            )
        except KeyError as e:
            raise ValueError(f"Unknown NAVIGATE scenario {e.args[0]}") from None


#: Mapping of climate policy labels to :class:`.engage.workflow.PolicyConfig` objects.
#:
#: - Some of the ``budget`` values were originally from
#:   :file:`message_data/projects/engage/config.yaml`, but have been updated using,
#:   variously:
#:
#:   - The results of diagnostic runs like Ctax-ref with various values for the --ctax=
#:     parameter.
#:   - The Excel-based ENGAGE budget calculator in
#:     :file:`projects/engage/doc/v4.1.7_T4.5_budget_calculation_r3.1.xlsx`.
#:   - The interpolate_budget() function via "mix-models navigate check-budget".
#:
#: - In the :file:`engage/config.yaml`, values for :attr:`low_dem_scen` (a scenario
#:   name only) appear to form a "waterfall", with each successively lower budget
#:   referencing the previous, which seems to preclude running lower budgets without
#:   first running every larger budget.
#:   In the NAVIGATE workflow, the :attr:`demand_scenario` values (scenario info style,
#:   a :class:`dict` of ``model`` name, ``scenario`` name, and optional ``version``) are
#:   set in .navigate.workflow.generate().
CLIMATE_POLICY: dict[Optional[str], WfConfig] = {
    # Default
    None: WfConfig(
        reserve_margin=False,
        solve=dict(
            model="MESSAGE", solve_options=dict(barcrossalg=2), max_adjustment=0.1
        ),
    ),
}

# Add further values, reusing the same defaults above
for _pc in (
    # No climate policy → no ENGAGE workflow steps
    PolicyConfig(label="NPi", steps=[]),
    # Only step 1. This item does not appear in the official NAVIGATE scenarios
    # list, but is used in EXTRA_SCENARIOS below.
    PolicyConfig(label="1000 Gt", steps=[1], budget=2449),
    # All steps 1–3
    PolicyConfig(
        label="20C",
        # 1150 Gt target — for T3.5 and initial WP6
        # budget=2700,  # MESSAGE only, y₀=2025
        # budget=2511,  # MESSAGE-MACRO, y₀=2030: determined via check-budget. This
        # #               value is infeasible for NPi-ref, but feasible for NPi-Default.
        # budget=2580   # Manually adjusted for NPi-ref. Not feasible.
        budget=2585,  # Manually adjusted for NPi-ref. Lowest feasible
        #
        # 900 Gt target — for current WP6, i.e. based on NPi-{Default,AdvPE}
        # budget=1931,  # MESSAGE only, y₀=2025
        # budget=1239,  # MESSAGE-MACRO, y₀=2030: 1847 via check-budget, minus 558 per
        # #               JM. Not feasible.
        # budget=1889,  # Determined via-check-budget. Feasible for NPi-Default.
        # budget=1889,  # Determined via check-budget. Feasible for NPi-Default.
        # budget=1155,  # Determined via check-budget. Not feasible.
        # budget=1830,  # Manually adjusted for NPi-Default. Feasible.
    ),
    # Originally from an item labelled "600" in engage/config.yaml. Current values
    # calculated based on Ctax runs with price of 1000 USD / t CO₂.
    PolicyConfig(
        label="15C",
        # Feasible for non-ref demand-side scenarios aiming for 650 Gt:
        # budget=1357,
        # Feasible for -ref demand-side scenario aiming for 850 Gt:
        # budget=1840,
        # Calculated using check-budget based on "NPi-act+MACRO_ENGAGE_20C_step-3+B#3"
        # and "Ctax-ref+B#1"
        budget=1190,
    ),
    # The following do not appear in the official NAVIGATE scenarios list, but are used
    # in EXTRA_SCENARIOS below.
    # From an item labelled "1600" in engage/config.yaml
    PolicyConfig(label="1600 Gt", steps=[1], budget=4162),
    # From an item labelled "2000" in engage/config.yaml
    PolicyConfig(label="2000 Gt", steps=[1], budget=5320),
):
    CLIMATE_POLICY[_pc.label] = replace(_pc, **asdict(CLIMATE_POLICY[None]))

#: Special "20C" policy for Task 6.2 scenarios *other than* PEP-2C-Default.
#:
#: - Only ENGAGE step 3 is run; i.e. not step 1 or 2.
#: - tax_emission_scenario changes the behaviour of engage.step_3: CO2 prices are
#:   retrieved from the indicated scenario, i.e. PEP-2C-Default.
CLIMATE_POLICY["20C T6.2"] = PolicyConfig(
    label="20C",
    steps=[3],
    tax_emission_scenario=dict(scenario="2C-Default ENGAGE_20C_step-2"),
    step_3_type_emission=["TCE"],
    **asdict(CLIMATE_POLICY[None]),  # Reuse defaults
)


# Common annotations for `EXTRA_SCENARIOS`
_A = {
    "act+tec": Annotation(id="navigate_T35_policy", text="act+tec"),
    "all": Annotation(id="navigate_T35_policy", text="act+ele+tec"),
    "ele": Annotation(id="navigate_T35_policy", text="ele"),
    "ref": Annotation(id="navigate_T35_policy", text=""),
    "T3.5": Annotation(id="navigate_task", text="T3.5"),
    "T6.1": Annotation(id="navigate_task", text="T6.1"),
    "T6.2": Annotation(id="navigate_task", text="T6.2"),
    "advanced": Annotation(id="navigate_WP6_production", text="advanced"),
    "default": Annotation(id="navigate_WP6_production", text="default"),
}


def _anno(names: str, climate_policy) -> list["BaseAnnotation"]:
    """Return the annotations given by `names` from :data:`_A`.

    Shorthand function used to prepare :data:`EXTRA_SCENARIOS`.
    """
    # Collect predefined annotations from _A
    result: list["BaseAnnotation"] = [_A[name] for name in names.split()]

    # Add an annotation with the value of the `climate_policy` argument
    result.append(Annotation(id="navigate_climate_policy", text=climate_policy))

    # Return as a list of annotations
    return result


#: Extra scenario IDs not appearing in the authoritative NAVIGATE list per the workflow
#: repository.
#:
#: - ``NAV_Dem-15C-ref`` and ``NAV_Dem-20C-ref`` were added to the NAVIGATE list in
#:   iiasa/navigate-workflow#43, but with invalid "navigate_T35_policy" annotations.
#:   Retained here pending iiasa/navigate-workflow#47. cf. corresponding pin to an
#:   earlier commit in :file:`pytest.yaml` workflow.
#: - ``NAV_Dem-(* Gt|Ctax)-*``: diagnostic scenarios
#: - ``PC-*``: no-policy scenarios corresponding to WP6 scenarios.
EXTRA_SCENARIOS = [
    Code(id="NAV_Dem-1000 Gt-ref", annotations=_anno("T3.5 ref", "1000 Gt")),
    Code(id="NAV_Dem-1600 Gt-ref", annotations=_anno("T3.5 ref", "1600 Gt")),
    Code(id="NAV_Dem-2000 Gt-ref", annotations=_anno("T3.5 ref", "2000 Gt")),
    Code(id="NAV_Dem-Ctax-ref", annotations=_anno("T3.5 ref", "Ctax")),
    Code(id="NAV_Dem-Ctax-all", annotations=_anno("T3.5 all", "Ctax")),
    Code(id="PC-NPi-Default", annotations=_anno("T6.1 ref default", "NPi")),
    Code(id="PC-NPi-AdvPE", annotations=_anno("T6.1 ele advanced", "NPi")),
    Code(id="PC-NPi-LowCE", annotations=_anno("T6.1 act+tec default", "NPi")),
    Code(id="PC-NPi-AllEn", annotations=_anno("T6.1 all advanced", "NPi")),
    Code(id="PC-Ctax-Default", annotations=_anno("T6.1 ref default", "Ctax")),
    Code(id="PEP-Ctax-AllEn", annotations=_anno("T6.2 all advanced", "Ctax")),
]


@lru_cache()
def _read() -> list[Code]:
    """Read the codes from the NAVIGATE workflow directory.

    This function previously used a separate clone of the (private, non-installable)
    https://github.com/iiasa/navigate-workflow repository.

    It currently uses a copy of the file from this repository, stored within
    :file:`message_ix_models/data/navigate`.
    """
    # Previous location
    # workflow_dir = Path(
    #     ixmp.config.get("navigate workflow dir")
    # ).expanduser().resolve()
    #
    # # Use the particular path scheme of Scenario Explorer config repositories
    # path = workflow_dir.joinpath("definitions", "scenario", "scenarios.yaml")

    path = package_data_path("navigate", "scenarios.yaml")
    log.info(f"Read scenarios from {path}")
    with open(path) as f:
        _content = yaml.safe_load(f)

    # Transform into a form intelligible by as_codes()
    content: dict[str, Union[str, Code]] = {}
    for item in _content:
        if isinstance(item, str):
            content[item] = Code(id=item, name=item)
        else:
            content.update(item)

    # Transform to a list of SDMX codes
    return as_codes(content)


def get_policy_config(label, **kwargs) -> WfConfig:
    obj = CLIMATE_POLICY.get(label) or CLIMATE_POLICY[None]
    return replace(deepcopy(obj), **kwargs)


def iter_scenario_codes(
    context, filters: Optional[Mapping] = None
) -> Generator[Code, None, None]:
    """Iterate over the scenarios defined in the ``navigate-workflow`` repository.

    Parameters
    ----------
    filters : dict (str -> any), optional
        Only codes with annotations that match these filters are returned.
    """
    filters = filters or dict()

    codes = _read()

    # Yield codes whose annotations match `filters`
    for code in codes + EXTRA_SCENARIOS:
        match = True
        for key, value in filters.items():
            try:
                if str(code.get_annotation(id=key).text) not in value:
                    match = False
                    break
            except KeyError:
                match = False
                break
        if match:
            yield code


@dataclass
class Config:
    """Configuration for NAVIGATE."""

    #: Level of carbon tax for ``Ctax-*`` scenarios, via :func:`add_tax_emission`.
    carbon_tax: float = 1000.0

    #: Other scenario from which to copy historical time series data for reporting.
    copy_ts: dict = field(default_factory=dict)

    #: Target data structure for submission prep
    dsd: Literal["iiasa-ece", "navigate"] = "navigate"

    #: First period for policy scenarios.
    policy_year: Literal[2025, 2030] = 2030

    #: Single target scenario for :mod:`.navigate.workflow.generate_workflow`
    scenario: Optional[str] = None

    #: :data:`True` to use MESSAGEix-Buildings (:mod:`.model.buildings`).
    buildings: bool = True

    #: :data:`True` to use MESSAGEix-Materials (:mod:`.model.material`).
    material: bool = True

    #: :data:`True` to use MESSAGEix-Transport (:mod:`message_data.model.transport`)
    #: alongside MESSAGEix-Buildings and MESSAGEix-Materials.
    transport: bool = True
