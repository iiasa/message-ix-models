"""NAVIGATE project."""
import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Generator, List, Mapping, Optional, Union

import ixmp
import yaml
from message_ix_models.util import as_codes
from sdmx.model.v21 import Annotation, Code

from message_data.projects.engage.workflow import PolicyConfig

log = logging.getLogger(__name__)

# Shorthand for use in `CLIMATE_POLICY`
_kw: Mapping = dict(
    reserve_margin=False,
    solve=dict(model="MESSAGE", solve_options=dict(barcrossalg=2)),
)

#: Mapping of climate policy labels to :class:`.engage.workflow.PolicyConfig` objects.
#:
#: - Some of the ``budget`` values were originally from
#:   :file:`message_data/projects/engage/config.yaml`, but have been updated using the
#:   results of diagnostic Ctax runs and the ENGAGE budget calculator
#:   :file:`message_data/projects/engage/doc/v4.1.7_T4.5_budget_calculation_r3.1.xlsx`.
#: - The values for ``low_dem_scen`` have no effect in the NAVIGATE workflow as
#:   currently implemented. In the :file:`engage/config.yaml`, they appear to form a
#:   "waterfall", with each successively lower budget referencing the previous, which
#:   seems to preclude running lower budgets without first running every larger budget.
CLIMATE_POLICY: Dict[Any, Optional[PolicyConfig]] = {
    pc.label: pc
    for pc in (
        # No climate policy → no ENGAGE workflow steps
        PolicyConfig("NPi", steps=[], **_kw),
        # Only step 1. This item does not appear in the official NAVIGATE scenarios
        # list, but is used in EXTRA_SCENARIOS below.
        PolicyConfig(
            "1000 Gt",
            steps=[1],
            budget=2449,
            low_dem_scen="EN_NPi2020_1200_step1",
            **_kw,
        ),
        # All steps 1–3
        # Originally from an item labelled "1000" in engage/config.yaml
        PolicyConfig("20C", budget=2700, low_dem_scen="EN_NPi2020_1200_step1", **_kw),
        # Originally from an item labelled "600" in engage/config.yaml. Current values
        # calculated based on Ctax runs with price of 1000 USD / t CO₂.
        PolicyConfig(
            "15C",
            # Achievable for non-ref demand-side scenarios aiming for 650 Gt:
            # budget=1357,
            # Achievable for -ref demand-side scenario aiming for 850 Gt:
            budget=1840,
            low_dem_scen="EN_NPi2020_700_step1",
            **_kw,
        ),
        # The following do not appear in the official NAVIGATE scenarios list, but are
        # used in EXTRA_SCENARIOS below.
        # From an item labelled "1600" in engage/config.yaml
        PolicyConfig(
            "1600 Gt",
            steps=[1],
            budget=4162,
            low_dem_scen="EN_NPi2020_1800_step1",
            **_kw,
        ),
        # From an item labelled "2000" in engage/config.yaml
        PolicyConfig(
            "2000 Gt",
            steps=[1],
            budget=5320,
            low_dem_scen="EN_NPi2020_2500_step1",
            **_kw,
        ),
    )
}
# Placeholder
CLIMATE_POLICY["Ctax"] = None


# Common annotations for `EXTRA_SCENARIOS`
_A = [
    Annotation(id="navigate_T35_policy", text=""),
    Annotation(id="navigate_task", text="T3.5"),
]

#: Extra scenario IDs not appearing in the authoritative NAVIGATE list per the workflow
#: repository. These each have the same 3 annotations.
#:
#: - ``NAV_Dem-15C-ref`` and ``NAV_Dem-20C-ref`` were added to the NAVIGATE list in
#:   iiasa/navigate-workflow#43, but with invalid "navigate_T35_policy" annotations.
#:   Retained here pending iiasa/navigate-workflow#47. cf. corresponding pin to an
#:   earlier commit in :file:`pytest.yaml` workflow.
EXTRA_SCENARIOS = [
    # In order to have a no-policy corresponding to PC-15C-LowCE
    Code(
        id="PC-NPi-LowCE",
        annotations=[
            Annotation(id="navigate_climate_policy", text="NPi"),
            Annotation(id="navigate_T35_policy", text="act+tec"),
            Annotation(id="navigate_task", text="T6.1"),
        ],
    ),
    Code(
        id="NAV_Dem-15C-ref",
        annotations=[Annotation(id="navigate_climate_policy", text="15C")] + _A,
    ),
    Code(
        id="NAV_Dem-20C-ref",
        annotations=[Annotation(id="navigate_climate_policy", text="20C")] + _A,
    ),
    Code(
        id="NAV_Dem-1000 Gt-ref",
        annotations=[Annotation(id="navigate_climate_policy", text="1000 Gt")] + _A,
    ),
    Code(
        id="NAV_Dem-1600 Gt-ref",
        annotations=[Annotation(id="navigate_climate_policy", text="1600 Gt")] + _A,
    ),
    Code(
        id="NAV_Dem-2000 Gt-ref",
        annotations=[Annotation(id="navigate_climate_policy", text="2000 Gt")] + _A,
    ),
    Code(
        id="NAV_Dem-Ctax-ref",
        annotations=[Annotation(id="navigate_climate_policy", text="Ctax")] + _A,
    ),
    Code(
        id="NAV_Dem-Ctax-all",
        annotations=[
            Annotation(id="navigate_climate_policy", text="Ctax"),
            Annotation(id="navigate_T35_policy", text="act+ele+tec"),
            Annotation(id="navigate_task", text="T3.5"),
        ],
    ),
]


@lru_cache()
def _read() -> List[Code]:
    """Read the codes from the NAVIGATE workflow directory."""
    workflow_dir = Path(ixmp.config.get("navigate workflow dir")).expanduser().resolve()

    # Use the particular path scheme of Scenario Explorer config repositories
    path = workflow_dir.joinpath("definitions", "scenario", "scenarios.yaml")
    log.info(f"Read scenarios from {path}")
    with open(path) as f:
        _content = yaml.safe_load(f)

    # Transform into a form intelligible by as_codes()
    content: Dict[str, Union[str, Code]] = {}
    for item in _content:
        if isinstance(item, str):
            content[item] = Code(id=item, name=item)
        else:
            content.update(item)

    # Transform to a list of SDMX codes
    return as_codes(content)


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
