"""Common fixtures for water module tests.

This conftest provides shared fixtures to reduce test boilerplate and ensure
consistent scenario and context setup across water module tests.
"""

import pandas as pd
import pytest
from message_ix import Scenario

from message_ix_models import ScenarioInfo
from message_ix_models.model.structure import get_codes
from message_ix_models.util import package_data_path


@pytest.fixture(scope="function")
def water_scenario(request, test_context):
    """Create a water module test scenario with standardized setup.

    This fixture handles common setup for water module tests:
    - Creates a basic scenario with standard sets
    - Configures context with water-specific parameters
    - Computes nodes from region codelists
    - Sets up basin mappings when needed

    Parameters are passed via indirect parametrization:

        @pytest.mark.parametrize(
            "water_scenario",
            [
                {"regions": "R12", "type_reg": "global", "RCP": "6p0"},
                {"regions": "ZMB", "type_reg": "country", "SDG": "baseline"},
            ],
            indirect=True,
        )
        def test_something(water_scenario):
            scenario = water_scenario["scenario"]
            context = water_scenario["context"]

    Supported parameters:
    - regions: str, default "R12" (e.g., "R11", "R12", "ZMB", "R14")
    - type_reg: str, default "global" (options: "global", "country")
    - time: str, default "year" (options: "year", "month")
    - RCP: str, optional (e.g., "2p6", "6p0", "7p0", "no_climate")
    - REL: str, optional (e.g., "low", "med", "high")
    - SDG: str/bool, optional (e.g., "baseline", "not_baseline", "ambitious", True)
    - nexus_set: str, optional (e.g., "nexus", "cooling")
    - setup_basins: bool, default False - whether to setup valid_basins

    Returns
    -------
    dict
        Dictionary with keys:
        - "scenario": message_ix.Scenario
        - "context": message_ix_models.Context
    """
    # Extract parameters from request.param or use defaults
    if hasattr(request, "param"):
        params = request.param if isinstance(request.param, dict) else {}
    else:
        params = {}

    regions = params.get("regions", "R12")
    type_reg = params.get("type_reg", "global")
    time = params.get("time", "year")
    RCP = params.get("RCP")
    REL = params.get("REL")
    SDG = params.get("SDG")
    nexus_set = params.get("nexus_set")
    setup_basins = params.get("setup_basins", False)

    # Setup context
    test_context.type_reg = type_reg
    test_context.regions = regions
    test_context.time = time

    if RCP is not None:
        test_context.RCP = RCP
    if REL is not None:
        test_context.REL = REL
    if SDG is not None:
        test_context.SDG = SDG
    if nexus_set is not None:
        test_context.nexus_set = nexus_set

    # Compute nodes from codes
    nodes = get_codes(f"node/{regions}")
    nodes = list(map(str, nodes[nodes.index("World")].child))
    test_context.map_ISO_c = {regions: nodes[0]}

    # Setup valid basins if requested
    if setup_basins:
        basin_file = f"basins_by_region_simpl_{regions}.csv"
        basin_path = package_data_path("water", "delineation", basin_file)
        df_basins = pd.read_csv(basin_path)
        test_context.valid_basins = set(df_basins["BCU_name"].astype(str))

    # Create scenario
    mp = test_context.get_platform()
    scenario_info = {
        "mp": mp,
        "model": f"{request.node.name}/test water model",
        "scenario": f"{request.node.name}/test water scenario",
        "version": "new",
    }
    s = Scenario(**scenario_info)
    s.add_horizon(year=[2020, 2030, 2040])
    s.add_set("technology", ["tech1", "tech2"])
    s.add_set("year", [2020, 2030, 2040])
    s.commit(comment="basic water test model")

    test_context.set_scenario(s)
    test_context["water build info"] = ScenarioInfo(s)

    return {"scenario": s, "context": test_context}