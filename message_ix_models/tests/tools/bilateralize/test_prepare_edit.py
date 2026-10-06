from pathlib import Path

import message_ix
import pandas as pd
import pytest

from message_ix_models.tools.bilateralize.prepare_edit import (
    build_accounting_relations,
    build_flow_input,
    build_flow_output,
    build_flow_Vcosts,
    flow_as_trade_input,
)

COMMON_YEARS = dict(year_vtg="broadcast", year_rel="broadcast", year_act="broadcast")
COMMON_COLS = dict(mode="M1", time="year", time_origin="year", time_dest="year")


@pytest.fixture
def network_setup() -> dict:
    df = pd.DataFrame(
        {
            "exporter": ["R12_AFR", "R12_CHN"],
            "importer": ["R12_CHN", "R12_AFR"],
            "export_technology": ["gas_pipe_exp_chn", "gas_pipe_exp_afr"],
            "import_technology": ["gas_pipe_imp", "gas_pipe_imp"],
        }
    )
    return {"gas_piped": df}


def test_build_accounting_relations_with_emission_factor(network_setup: dict) -> None:
    config_dict = {
        "trade_commodity": {"gas_piped": "NG"},
        "trade_technology": {"gas_piped": "gas_pipe"},
        "emission_factor": {"gas_piped": {"CO2": 0.5}},
    }
    parameter_outputs: dict[str, dict[str, dict]] = {"gas_piped": {"trade": {}}}

    build_accounting_relations(
        tec="gas_piped",
        network_setup=network_setup,
        config_dict=config_dict,
        common_years=COMMON_YEARS,
        common_cols=COMMON_COLS,
        parameter_outputs=parameter_outputs,
    )

    df_rel = parameter_outputs["gas_piped"]["trade"]["relation_activity_CO2_Emission"]

    exports = df_rel[df_rel["technology"] != "gas_piped_imp"]
    imports = df_rel[df_rel["technology"] == "gas_piped_imp"]

    assert not exports.empty
    assert (exports["value"] == -0.5).all()

    assert not imports.empty
    assert (imports["value"] == 0.5).all()


def test_build_accounting_relations_without_emission_factor(
    network_setup: dict,
) -> None:
    config_dict = {
        "trade_commodity": {"gas_piped": "NG"},
        "trade_technology": {"gas_piped": "gas_pipe"},
        "emission_factor": {"gas_piped": {"CO2": None}},
    }
    parameter_outputs: dict[str, dict[str, dict]] = {"gas_piped": {"trade": {}}}

    build_accounting_relations(
        tec="gas_piped",
        network_setup=network_setup,
        config_dict=config_dict,
        common_years=COMMON_YEARS,
        common_cols=COMMON_COLS,
        parameter_outputs=parameter_outputs,
    )

    df_rel = parameter_outputs["gas_piped"]["trade"]["relation_activity_CO2_Emission"]
    assert (df_rel["value"] == 0).all()


def test_build_flow_input_gas_pipe_default(network_setup: dict) -> None:
    config_dict = {
        "flow_fuel_input": {"gas_piped": {"gas_pipe_flow": ["electr"]}},
        "flow_material_input": {"gas_piped": {"gas_pipe_flow": []}},
        "flow_constraint": {"gas_piped": "bilateral"},
        "export_level": {"gas_piped": "secondary"},
        "bunker_technology": {"gas_piped": None},
    }
    parameter_outputs = {"gas_piped": {"flow": {"input": message_ix.make_df("input")}}}

    build_flow_input(
        flow_tec="gas_pipe_flow",
        tec="gas_piped",
        network_setup=network_setup,
        config_dict=config_dict,
        common_years=COMMON_YEARS,
        common_cols=COMMON_COLS,
        parameter_outputs=parameter_outputs,
    )

    df_input = parameter_outputs["gas_piped"]["flow"]["input"]
    assert not df_input.empty
    assert (df_input["value"] == 0.002).all()


def test_build_flow_output_pipe_overrides(network_setup: dict) -> None:
    config_dict = {
        "flow_constraint": {"gas_piped": "bilateral"},
        "flow_commodity_output": {"gas_piped": "NG"},
        "flow_units": {"gas_piped": "GWa"},
        "trade_level": {"gas_piped": "secondary"},
        "bunker_technology": {"gas_piped": None},
    }

    values_by_flow_tec = {}
    for flow_tec in ["gas_pipe_flow", "oil_pipe_flow", "LNG_Tanker_flow"]:
        parameter_outputs = {"gas_piped": {"flow": {"output": pd.DataFrame()}}}
        build_flow_output(
            flow_tec=flow_tec,
            tec="gas_piped",
            network_setup=network_setup,
            config_dict=config_dict,
            common_years=COMMON_YEARS,
            common_cols=COMMON_COLS,
            parameter_outputs=parameter_outputs,
        )
        df_output = parameter_outputs["gas_piped"]["flow"]["output"]
        values_by_flow_tec[flow_tec] = set(df_output["value"].unique())

    assert values_by_flow_tec["gas_pipe_flow"] == {20}
    assert values_by_flow_tec["oil_pipe_flow"] == {10}
    assert values_by_flow_tec["LNG_Tanker_flow"] == {1}


def test_build_flow_Vcosts_global_mode(network_setup: dict) -> None:
    config_dict = {
        "flow_constraint": {"gas_piped": "global"},
        "flow_units": {"gas_piped": "GWa"},
    }
    parameter_outputs = {
        "gas_piped": {"flow": {"var_cost": message_ix.make_df("var_cost")}}
    }

    build_flow_Vcosts(
        flow_tec="gas_pipe_flow",
        tec="gas_piped",
        network_setup=network_setup,
        config_dict=config_dict,
        common_years=COMMON_YEARS,
        common_cols=COMMON_COLS,
        parameter_outputs=parameter_outputs,
        message_regions="R12",
    )

    df_vcost = parameter_outputs["gas_piped"]["flow"]["var_cost"]
    assert (df_vcost["mode"] == "M1").all()
    # No duplicate rows after concatenation
    assert len(df_vcost) == len(df_vcost.drop_duplicates())


@pytest.mark.parametrize(
    "trade_commodity, expected_filename",
    [
        ("LNG", "R12_LNG_distances.csv"),
        ("coal", "R12_base_distances.csv"),
    ],
)
def test_flow_as_trade_input_distance_file(
    tmp_path: Path, network_setup: dict, trade_commodity: str, expected_filename: str
) -> None:
    distances_dir = tmp_path / "distances"
    distances_dir.mkdir()

    # Only create the file expected to be read; a wrong lookup fails with
    # FileNotFoundError instead of silently reading the other file.
    pd.DataFrame(
        {
            "Node1": ["R12_AFR"],
            "Node2": ["R12_CHN"],
            "Distance_km": [1000.0],
        }
    ).to_csv(distances_dir / expected_filename, index=False)

    pd.DataFrame(
        {"Commodity": [trade_commodity], "Specific Energy (GWa/Mt)": [0.5]}
    ).to_excel(tmp_path / "specific_energy.xlsx", index=False)

    config_dict = {
        "flow_constraint": {"gas_piped": "global"},
        "flow_commodity_output": {"gas_piped": "NG"},
        "flow_units": {"gas_piped": "GWa"},
        "trade_level": {"gas_piped": "secondary"},
        "trade_units": {"gas_piped": "GWa"},
        "trade_commodity": {"gas_piped": trade_commodity},
        "trade_technology": {"gas_piped": "gas_pipe"},
    }
    parameter_outputs = {"gas_piped": {"trade": {"input": message_ix.make_df("input")}}}

    flow_as_trade_input(
        flow_tec="gas_pipe_flow",
        tec="gas_piped",
        network_setup=network_setup,
        config_dict=config_dict,
        common_years=COMMON_YEARS,
        common_cols=COMMON_COLS,
        parameter_outputs=parameter_outputs,
        message_regions="R12",
        data_path=tmp_path,
    )

    df_input = parameter_outputs["gas_piped"]["trade"]["input"]
    matched = df_input[df_input["node_loc"] == "R12_AFR"]

    # Distance (1000 km) / specific energy (0.5 GWa/Mt) = 2000
    assert (matched["value"] == 2000).all()
