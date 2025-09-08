from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional

import pytest
import yaml
from message_ix import make_df

from message_ix_models.util import broadcast, package_data_path

if TYPE_CHECKING:
    from message_ix import Scenario

    from message_ix_models import Context


from message_ix_models import ScenarioInfo, testing
from message_ix_models.tools.inter_pipe import Config, build, generate_bare_sheets


@pytest.fixture
def test_config_file() -> Generator[Path, None, None]:
    """Generate a test config file for inter_pipe testing in the data directory."""
    config_data = {
        "scenario": {
            "start_model": "test_model",
            "start_scen": "test_scenario",
            "target_model": "test_target_model",
            "target_scen": "test_target_scenario",
        },
        "pipe_tech": {
            "tech_mother_pipe": ["elec_t_d"],
            "tech_mother_shorten_pipe": "elec",
            "tech_suffix_pipe": "pipe",
            "tech_number_pipe": 1,
            "commodity_mother_pipe": "electr",
            "commodity_suffix_pipe": "pipe",
            "level_mother_pipe": "secondary",
            "level_mother_shorten_pipe": "elec",
            "level_suffix_pipe": "pipe",
        },
        "pipe_supplytech": {
            "tech_mother_supply": ["solar_res1"],
            "tech_suffix_supply": "pipe",
            "tech_number_supply": 1,
            "commodity_mother_supply": "electr",
            "commodity_suffix_supply": "pipe",
            "level_mother_supply": "final",
            "level_mother_shorten_supply": "elec",
            "level_suffix_supply": "pipe",
        },
        "first_model_year": 2030,
        "spec": {
            "spec_tech_pipe": False,
            "spec_tech_pipe_group": False,
            "spec_supply_pipe_group": False,
        },
    }

    # Create config file in the actual data/inter_pipe directory
    config_dir = package_data_path("inter_pipe")
    config_file = config_dir / "config_test.yaml"

    with open(config_file, "w") as f:
        yaml.dump(config_data, f, default_flow_style=False)

    yield config_file

    if config_file.exists():
        config_file.unlink()


@pytest.fixture
def scenario(
    request: "pytest.FixtureRequest", test_context: "Context"
) -> Generator["Scenario", None, None]:
    # Code only functions with R12
    test_context.model.regions = "R12"
    # inter_pipe_build() only broadcasts data from 2030 onwards
    s = testing.bare_res(request, test_context, solved=False).clone(
        shift_first_model_year=2030
    )
    s.check_out()

    # Add the elec_t_d technology and related sets
    s.add_set("technology", ["elec_t_d"])
    s.add_set("commodity", ["electr"])
    s.add_set("level", ["secondary", "final"])
    s.add_set("mode", ["M1"])

    # Add R12_GLB node for inter-regional pipe technologies
    s.add_set("node", ["R12_GLB"])  # add temporarily for the test scenario

    # Add input parameter for elec_t_d
    # Input: electr at secondary level with value 1.1
    info = ScenarioInfo(s)

    # Create input parameter data
    input_df = make_df(
        "input",
        technology="elec_t_d",
        commodity="electr",
        level="secondary",
        mode="M1",
        time="year",
        time_origin="year",
        value=1.1,
        unit="GWa",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    # Add node_origin column (same as node_loc for input)
    input_df["node_origin"] = input_df["node_loc"]

    s.add_par("input", input_df)

    # Add output parameter for elec_t_d
    # Output: electr at final level with value 1.0
    output_df = make_df(
        "output",
        technology="elec_t_d",
        commodity="electr",
        level="final",
        mode="M1",
        time="year",
        time_dest="year",
        value=1.0,
        unit="GWa",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    # Add node_dest column (same as node_loc for output)
    output_df["node_dest"] = output_df["node_loc"]

    s.add_par("output", output_df)

    # Add missing parameters for the dummy pipe technology
    # Technical lifetime
    technical_lifetime_df = make_df(
        "technical_lifetime",
        technology="elec_t_d",
        value=30,
        unit="y",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y)

    s.add_par("technical_lifetime", technical_lifetime_df)

    # Investment cost
    inv_cost_df_elec = make_df(
        "inv_cost",
        technology="elec_t_d",
        value=1500.0,
        unit="USD/GWa",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y)

    s.add_par("inv_cost", inv_cost_df_elec)

    # Fixed cost
    fix_cost_df = make_df(
        "fix_cost",
        technology="elec_t_d",
        value=50.0,
        unit="USD/GWa",
        mode="M1",
        time="year",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    s.add_par("fix_cost", fix_cost_df)

    # Variable cost
    var_cost_df = make_df(
        "var_cost",
        technology="elec_t_d",
        value=10.0,
        unit="USD/GWa",
        mode="M1",
        time="year",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    s.add_par("var_cost", var_cost_df)

    # Capacity factor
    capacity_factor_df = make_df(
        "capacity_factor",
        technology="elec_t_d",
        value=0.85,
        unit="%",
        mode="M1",
        time="year",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    s.add_par("capacity_factor", capacity_factor_df)

    # Add solar_res1 technology
    s.add_set("technology", ["solar_res1"])

    # Add missing parameters for dummy pipe supply technology, e.g., solar_res1
    # Output parameter for solar_res1
    output_df_solar = make_df(
        "output",
        technology="solar_res1",
        commodity="electr",
        level="final",
        mode="M1",
        time="year",
        time_dest="year",
        value=1.0,
        unit="GWa",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    # Add node_dest column (same as node_loc for output)
    output_df_solar["node_dest"] = output_df_solar["node_loc"]

    s.add_par("output", output_df_solar)

    # Technical lifetime for solar_res1
    technical_lifetime_df_solar = make_df(
        "technical_lifetime",
        technology="solar_res1",
        value=25,
        unit="y",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y)

    s.add_par("technical_lifetime", technical_lifetime_df_solar)

    # Fixed cost for solar_res1
    fix_cost_df_solar = make_df(
        "fix_cost",
        technology="solar_res1",
        value=30.0,
        unit="USD/GWa",
        mode="M1",
        time="year",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    s.add_par("fix_cost", fix_cost_df_solar)

    # Variable cost for solar_res1
    var_cost_df_solar = make_df(
        "var_cost",
        technology="solar_res1",
        value=5.0,
        unit="USD/GWa",
        mode="M1",
        time="year",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    s.add_par("var_cost", var_cost_df_solar)

    # Capacity factor for solar_res1
    capacity_factor_df_solar = make_df(
        "capacity_factor",
        technology="solar_res1",
        value=0.25,
        unit="%",
        mode="M1",
        time="year",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y, year_act=info.Y)

    s.add_par("capacity_factor", capacity_factor_df_solar)

    # Add inv_cost parameter for solar_res1
    # Investment cost for solar_res1 with value 1000 USD/kW
    inv_cost_df = make_df(
        "inv_cost",
        technology="solar_res1",
        value=200.0,
        unit="USD/GWa",
    ).pipe(broadcast, node_loc=info.N, year_vtg=info.Y)

    s.add_par("inv_cost", inv_cost_df)

    s.commit("Added mother technology features.")
    yield s


class TestConfig:
    @pytest.mark.usefixtures("test_config_file")
    @pytest.mark.parametrize(
        "name",
        [
            None,  # Default/packaged file
            "config",  # Default/packaged file
            "config_test",  # Generated by test_config_file, above
        ],
    )
    def test_from_file(self, name: Optional[str]) -> None:
        """Configuration can be loaded from file."""
        config = Config.from_file(name)

        # The config has all required contents for inter_pipe_bare
        assert isinstance(config.pipe.tech_number, int)
        assert isinstance(config.supply.tech_mother, list)


def test_inter_pipe_bare_with_test_config(tmp_path, test_config_file, scenario) -> None:
    """Test :func:`.inter_pipe_bare` with a generated test config file."""
    # Temporary test directory is empty of CSV files
    assert 0 == len(list(tmp_path.glob("*.csv")))

    # Function runs with the given config file
    generate_bare_sheets(
        scenario, config_name=str(test_config_file), target_dir=tmp_path
    )

    # Expected CSV files were generated
    assert {
        "capacity_factor_pipe_exp",
        "capacity_factor_pipe_supply",
        "fix_cost_pipe_exp_edit",
        "fix_cost_pipe_supply",
        "input_pipe_exp_edit",
        "input_pipe_imp",
        "inv_cost_pipe_exp_edit",
        "inv_cost_pipe_supply",
        "level",
        "output_pipe_exp",
        "output_pipe_imp",
        "output_pipe_supply",
        "relation",
        "technical_lifetime_pipe_exp_edit",
        "technical_lifetime_pipe_supply",
        "technology",
        "var_cost_pipe_exp_edit",
        "var_cost_pipe_supply",
    } == set(p.stem for p in tmp_path.glob("*.csv"))


def test_build(tmp_path, test_config_file, scenario) -> None:
    """Test :func:`.inter_pipe_build` with generated test config and data files."""
    # Generate bare data files
    generate_bare_sheets(
        scenario, config_name=str(test_config_file), target_dir=tmp_path
    )

    # Function runs without error
    build(scenario, config_name=str(test_config_file), data_dir=tmp_path)

    # TODO Expand with assertions about changes to the scenario
