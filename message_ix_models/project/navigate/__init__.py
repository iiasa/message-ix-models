"""NAVIGATE project."""
import logging
from pathlib import Path
from typing import Generator, Mapping, Optional

import yaml
from message_ix_models.util import as_codes
from sdmx.model import Annotation, Code

from message_data.projects.engage.workflow import PolicyConfig

log = logging.getLogger(__name__)

#: Values from engage/config.yaml, expressed as objects.
#:
#: .. todo:: The low_dem_scen values appear to form a "waterfall", with each
#:    successively lower budget referencing the previous. Investigate how to run only
#:    some budgets without the previous ones.
_so = dict(model="MESSAGE", solve_options=dict(barcrossalg=2))
CLIMATE_POLICY = {
    pc.label: pc
    for pc in (
        # No climate policy
        PolicyConfig("NPi", steps=[], solve=_so),
        # Only step 1. This value does not appear in the official NAVIGATE scenarios
        # list, but is used in EXTRA_SCENARIOS below.
        PolicyConfig(
            "1000 Gt",
            steps=[1],
            budget=2449,
            low_dem_scen="EN_NPi2020_1200_step1",
            solve=_so,
        ),
        # All steps 1–3
        # From an entry labelled "1000" in engage/config.yaml
        PolicyConfig(
            "20C", budget=2449, low_dem_scen="EN_NPi2020_1200_step1", solve=_so
        ),
        # From an entry labelled "600" in engage/config.yaml
        PolicyConfig(
            "15C", budget=1288, low_dem_scen="EN_NPi2020_700_step1", solve=_so
        ),
    )
}


EXTRA_SCENARIOS = [
    Code(
        id="NAV_Dem-20C-ref",
        annotations=[
            Annotation(id="navigate-climate-policy", text="20C"),
            Annotation(id="navigate-T3.5-policy", text=""),
            Annotation(id="navigate-task", text="T3.5"),
        ],
    ),
    Code(
        id="NAV_Dem-1000 Gt-ref",
        annotations=[
            Annotation(id="navigate-climate-policy", text="1000 Gt"),
            Annotation(id="navigate-T3.5-policy", text=""),
            Annotation(id="navigate-task", text="T3.5"),
        ],
    ),
]


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

    workflow_dir = Path("~/vc/iiasa/navigate-workflow").expanduser()

    # Use the particular path scheme of Scenario Explorer config repositories
    path = workflow_dir.joinpath("definitions", "scenario", "scenarios.yaml")
    log.info(f"Read scenarios from {path}")
    with open(path) as f:
        _content = yaml.safe_load(f)

    # Transform into a form intelligible by as_codes()
    content = {}
    for item in _content:
        if isinstance(item, str):
            content[item] = Code(id=item, name=item)
        else:
            content.update(item)

    # Transform to a list of SDMX codes
    codes = as_codes(content)

    # Yield codes whose annotations match `filters`
    for code in codes + EXTRA_SCENARIOS:
        match = True
        for key, value in filters.items():
            try:
                if str(code.get_annotation(id=key).text) != value:
                    match = False
                    break
            except KeyError:
                match = False
                break
        if match:
            yield code
