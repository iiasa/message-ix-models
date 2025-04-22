import pandas as pd
import pandas.testing as pdt
import pytest
from message_ix import make_df

from message_ix_models.model.water.dsl_engine import run_standard
from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.tests.model.water.rules_test import (
    COOL_TECH_OUTPUT_RULES,
    DESALINATION_OUTPUT_RULES,
    E_FLOW_RULES_BOUND,
    EXTRACTION_INPUT_RULES,
    INDUSTRIAL_DEMAND,
    SHARE_CONSTRAINTS_GW,
    SHARE_MODE_RULES,
    SLACK_TECHNOLOGY_RULES,
    WATER_AVAILABILITY,
    WD_CONST,
    WS_CONST,
)
from message_ix_models.util import broadcast, same_node, same_time


def test_industrial_demand_rule():
    """Test the INDUSTRIAL_DEMAND rule, mimicking legacy make_df usage."""
    # Input data mimicking the structure expected by the rule
    manuf_mw = pd.DataFrame(
        {
            "node": ["BasinA", "BasinB"],
            "year": [2020, 2020],
            "time": ["year", "year"],
            "value": [100, 200],
        }
    )
    manuf_uncollected_wst = pd.DataFrame(
        {
            "node": ["BasinA", "BasinC"],
            "year": [2020, 2030],
            "time": ["year", "year"],
            "value": [50, 80],
        }
    )

    input_dfs = {
        "manuf_mw": manuf_mw,
        "manuf_uncollected_wst": manuf_uncollected_wst,
    }

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Part 1: industry_mw demand
    expected_part1 = make_df(
        "demand",
        node="B" + manuf_mw["node"],
        commodity="industry_mw",
        level="final",
        year=manuf_mw["year"],
        time=manuf_mw["time"],
        value=manuf_mw["value"] * WD_CONST["UNIT_CONVERSION"],
        unit="km3/year",
    )

    # Part 2: industry_uncollected_wst demand (negative)
    expected_part2 = make_df(
        "demand",
        node="B" + manuf_uncollected_wst["node"],
        commodity="industry_uncollected_wst",
        level="final",
        year=manuf_uncollected_wst["year"],
        time=manuf_uncollected_wst["time"],
        value=manuf_uncollected_wst["value"]
        * WD_CONST["UNIT_CONVERSION"]
        * WD_CONST["NEGATIVE_MULTIPLIER"],
        unit="km3/year",
    )

    expected_df = pd.concat([expected_part1, expected_part2], ignore_index=True)
    # --- End Ground Truth ---

    # Run the DSL engine
    results = []
    for rule in INDUSTRIAL_DEMAND.get_rule():
        result_df = run_standard(rule, {"rule_dfs": input_dfs})
        results.append(result_df)
    result_df = pd.concat(results, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        result_df.sort_values(by=["node", "commodity", "year"]).reset_index(drop=True),
        expected_df.sort_values(by=["node", "commodity", "year"]).reset_index(
            drop=True
        ),
    )


def test_water_availability_rule():
    """Test the WATER_AVAILABILITY rule, mimicking legacy make_df usage."""
    df_sw = pd.DataFrame(
        {
            "Region": ["BasinX", "BasinY"],
            "year": [2020, 2020],
            "time": ["year", "month1"],
            "value": [1000, 500],
        }
    )
    df_gw = pd.DataFrame(
        {
            "Region": ["BasinX", "BasinZ"],
            "year": [2020, 2030],
            "time": ["year", "year"],
            "value": [200, 300],
        }
    )

    input_dfs = {"df_sw": df_sw, "df_gw": df_gw}

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Part 1: surfacewater_basin demand
    expected_part1 = make_df(
        "demand",
        node="B" + df_sw["Region"].astype(str),
        commodity="surfacewater_basin",
        level="water_avail_basin",
        year=df_sw["year"],
        time=df_sw["time"],
        value=df_sw["value"] * WD_CONST["NEGATIVE_MULTIPLIER"],
        unit="km3/year",
    )

    # Part 2: groundwater_basin demand
    expected_part2 = make_df(
        "demand",
        node="B" + df_gw["Region"].astype(str),
        commodity="groundwater_basin",
        level="water_avail_basin",
        year=df_gw["year"],
        time=df_gw["time"],
        value=df_gw["value"] * WD_CONST["NEGATIVE_MULTIPLIER"],
        unit="km3/year",
    )
    expected_df = pd.concat([expected_part1, expected_part2], ignore_index=True)
    # --- End Ground Truth ---

    # Run the DSL engine
    results = []
    for rule in WATER_AVAILABILITY.get_rule():
        result_df = run_standard(rule, {"rule_dfs": input_dfs})
        results.append(result_df)
    result_df = pd.concat(results, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        result_df.sort_values(by=["node", "commodity", "year", "time"]).reset_index(
            drop=True
        ),
        expected_df.sort_values(by=["node", "commodity", "year", "time"]).reset_index(
            drop=True
        ),
    )


def test_share_constraints_gw_rule():
    """Test the SHARE_CONSTRAINTS_GW rule, mimicking legacy make_df usage."""
    df_sw = pd.DataFrame(
        {
            "Region": ["BasinP", "BasinQ", "BasinP"],
            "year": [2020, 2020, 2030],
            "time": ["year", "year", "year"],
            "value": [800, 600, 700],
        }
    )
    df_gw = pd.DataFrame(
        {
            "Region": ["BasinP", "BasinQ", "BasinR"],
            "year": [2020, 2020, 2020],
            "time": ["year", "year", "year"],
            "value": [200, 400, 100],  # BasinP 2020 has both sw and gw
        }
    )

    # Create the expected DataFrame using make_df with aligned data
    expected_df = make_df(
        "share_commodity_lo",
        shares="share_low_lim_GWat",
        node_share="B" + df_gw["Region"].astype(str),
        year_act=df_gw["year"],
        time=df_gw["time"],
        value=df_gw["value"]
        / (df_sw["value"] + df_gw["value"])
        * WD_CONST["SHARE_GW_MULT"],
        unit="-",
    )
    # --- End Ground Truth ---

    # Run the DSL engine

    input_dfs = {"df_sw": df_sw, "df_gw": df_gw}
    results = []
    for rule in SHARE_CONSTRAINTS_GW.get_rule():
        result_df = run_standard(rule, {"rule_dfs": input_dfs})
        results.append(result_df)
    result_df = pd.concat(results, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        result_df.sort_values(by=["node_share", "year_act"]).reset_index(drop=True),
        expected_df.sort_values(by=["node_share", "year_act"]).reset_index(drop=True),
        check_dtype=False,
    )  # Allow float differences


# --- Water Supply Rules Tests ---


@pytest.fixture
def supply_test_data():
    """Provides common dummy data for water supply tests."""
    df_node = pd.DataFrame(
        {
            "BCU_name": ["BasinA", "BasinB"],
            "REGION": ["Region1", "Region1"],
            "node": ["BBasinA", "BBasinB"],
            "mode": ["MBasinA", "MBasinB"],
            "region": ["Region1", "Region1"],
        }
    )
    df_gwt = pd.DataFrame(
        {
            "BCU_name": ["BasinA", "BasinB"],
            "REGION": ["Region1", "Region1"],
            "region": ["Region1", "Region1"],
            "GW_per_km3_per_year": [0.1, 0.2],
        }
    )
    df_hist = pd.DataFrame(
        {
            "BCU_name": ["BBasinA", "BBasinB"],
            "hist_cap_sw_km3_year": [10, 15],
            "hist_cap_gw_km3_year": [5, 8],
        }
    )
    runtime_vals = {"year_wat": (2020, 2030)}
    sub_time = pd.Series(["year"], name="time")
    yv_ya_sw = pd.DataFrame({"year_vtg": [2020, 2030], "year_act": [2020, 2030]})
    yv_ya_gw = pd.DataFrame({"year_vtg": [2020, 2030], "year_act": [2020, 2030]})
    df_sw = pd.DataFrame(
        {
            "region": ["BBasinA", "BBasinB"],
            "mode": ["MBasinA", "MBasinB"],
            "date": ["2020-01-01", "2020-01-01"],
            "MSGREG": ["Region1", "Region1"],
            "share": [0.6, 0.4],
            "year": [2020, 2020],
            "time": ["year", "year"],
        }
    )
    df_env = pd.DataFrame(
        {
            "Region": ["BasinA", "BasinB"],
            "year": [2020, 2030],
            "time": ["year", "year"],
            "value": [50, 60],
        }
    )
    return {
        "df_node": df_node,
        "df_gwt": df_gwt,
        "df_hist": df_hist,
        "runtime_vals": runtime_vals,
        "sub_time": sub_time,
        "yv_ya_sw": yv_ya_sw,
        "yv_ya_gw": yv_ya_gw,
        "df_sw": df_sw,
        "df_env": df_env,
    }


def test_slack_technology_rules(supply_test_data):
    """Test SLACK_TECHNOLOGY_RULES, mimicking legacy make_df usage."""
    data = supply_test_data
    df_node = data["df_node"]
    runtime_vals = data["runtime_vals"]
    year_wat = runtime_vals["year_wat"]
    sub_time = data["sub_time"]


    inp1 = (
        make_df(
            "input",
            technology="return_flow",
            value=WS_CONST["IDENTITY"],
            unit="-",
            level="water_avail_basin",
            commodity="surfacewater_basin",
            mode="M1",
            year_vtg=year_wat,
            year_act=year_wat,
        )
        .pipe(broadcast, node_loc=df_node["node"], time=sub_time)
        .pipe(same_node)
        .pipe(same_time)
    )

    inp2 = (
        make_df(
            "input",
            technology="gw_recharge",
            value=WS_CONST["IDENTITY"],
            unit="-",
            level="water_avail_basin",
            commodity="groundwater_basin",
            mode="M1",
            year_vtg=year_wat,
            year_act=year_wat,
        )
        .pipe(broadcast, node_loc=df_node["node"], time=sub_time)
        .pipe(same_node)
        .pipe(same_time)
    )

    inp3 = (
        make_df(
            "input",
            technology="basin_to_reg",
            value=WS_CONST["IDENTITY"],
            unit="-",
            level="water_supply_basin",
            commodity="freshwater_basin",
            mode=df_node["mode"],
            node_origin=df_node["node"],
            node_loc=df_node["region"],
        )
        .pipe(broadcast, year_vtg=year_wat, time=sub_time)
        .pipe(same_time)
    )
    inp3["year_act"] = inp3["year_vtg"]

    # NB: Legacy code skips salinewater_return if context.nexus_set == "nexus"
    # We assume nexus setting here for comparison.
    expected_df = pd.concat([inp1, inp2, inp3], ignore_index=True)
    # --- End Ground Truth ---

    # Run the DSL engine
    slack_inputs = []
    base_slack = {
        "rule_dfs": {"df_node": df_node, "runtime_vals": {"year_wat": year_wat}},
        "sub_time": pd.Series(sub_time),
        "node_loc": df_node["node"],
    }
    for r in SLACK_TECHNOLOGY_RULES.get_rule():
        match r["technology"]:
            case "return_flow" | "gw_recharge":
                df_rule = run_standard(r, base_slack)
                slack_inputs.append(df_rule)
            case "basin_to_reg":
                df_rule = run_standard(r, base_slack, extra_args={"year_vtg": year_wat})
                slack_inputs.append(df_rule)
            case "salinewater_return":
                continue  # Skip this technology
            case _:
                raise ValueError(f"Invalid technology: {r['technology']}")
    slack_df = pd.concat(slack_inputs, ignore_index=True)
    slack_df["year_act"] = slack_df["year_vtg"]

    # Compare results
    pdt.assert_frame_equal(
        slack_df.sort_values(by=list(slack_df.columns)).reset_index(drop=True),
        expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True),
    )


def test_extraction_input_rules(supply_test_data):
    """Test EXTRACTION_INPUT_RULES, mimicking legacy make_df usage."""
    data = supply_test_data
    df_node = data["df_node"]
    df_gwt = data["df_gwt"]
    sub_time = data["sub_time"]
    yv_ya_sw = data["yv_ya_sw"]
    yv_ya_gw = data["yv_ya_gw"]

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Compare with water_supply_legacy.py lines 344-441
    inp1 = (
        make_df(
            "input",
            technology="extract_surfacewater",
            value=WS_CONST["IDENTITY"],
            unit="-",
            level="water_avail_basin",
            commodity="surfacewater_basin",
            mode="M1",
            node_origin=df_node["node"],
            node_loc=df_node["node"],
        )
        .pipe(broadcast, yv_ya_sw, time=sub_time)
        .pipe(same_time)
    )

    inp2 = (
        make_df(
            "input",
            technology="extract_groundwater",
            value=WS_CONST["IDENTITY"],
            unit="-",
            level="water_avail_basin",
            commodity="groundwater_basin",
            mode="M1",
            node_origin=df_node["node"],
            node_loc=df_node["node"],
        )
        .pipe(broadcast, yv_ya_gw, time=sub_time)
        .pipe(same_time)
    )

    inp3 = make_df(
        "input",
        technology="extract_surfacewater",
        value=WS_CONST["SW_ELEC_IN"],
        unit="-",
        level="final",
        commodity="electr",
        mode="M1",
        time_origin="year",
        node_origin=df_node["region"],
        node_loc=df_node["node"],
    ).pipe(broadcast, yv_ya_sw, time=sub_time)

    inp4 = make_df(
        "input",
        technology="extract_groundwater",
        value=df_gwt["GW_per_km3_per_year"] + WS_CONST["GW_ADD_ELEC_IN"],
        unit="-",
        level="final",
        commodity="electr",
        mode="M1",
        time_origin="year",
        node_origin=df_node["region"],
        node_loc=df_node["node"],
    ).pipe(broadcast, yv_ya_gw, time=sub_time)

    inp5 = make_df(
        "input",
        technology="extract_gw_fossil",
        value=(df_gwt["GW_per_km3_per_year"] + WS_CONST["GW_ADD_ELEC_IN"])
        * WS_CONST["FOSSIL_GW_ELEC_MULT"],
        unit="-",
        level="final",
        commodity="electr",
        mode="M1",
        time_origin="year",
        node_origin=df_node["region"],
        node_loc=df_node["node"],
    ).pipe(broadcast, yv_ya_gw, time=sub_time)

    expected_df = pd.concat([inp1, inp2, inp3, inp4, inp5], ignore_index=True)
    # --- End Ground Truth ---

    # Run the DSL engine
    extraction_inputs = []
    for r in EXTRACTION_INPUT_RULES.get_rule():
        bcast = yv_ya_sw if r["technology"] == "extract_surfacewater" else yv_ya_gw
        base_extract = {
            "rule_dfs": {"df_node": df_node, "df_gwt": df_gwt},
            "broadcast_year": bcast,
            "sub_time": sub_time,
        }
        df_rule = run_standard(r, base_extract)
        extraction_inputs.append(df_rule)
    extraction_df = pd.concat(extraction_inputs, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        extraction_df.sort_values(by=list(extraction_df.columns)).reset_index(
            drop=True
        ),
        expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True),
        check_dtype=False,  # Allow float differences
    )


def test_share_mode_rules(supply_test_data):
    """Test SHARE_MODE_RULES, mimicking legacy make_df usage."""
    data = supply_test_data
    df_sw = data["df_sw"]  # Dummy output from map_basin_region_wat

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Compare with water_supply_legacy.py lines 652-665
    expected_df = make_df(
        "share_mode_up",
        shares="share_basin",
        technology="basin_to_reg",
        mode=df_sw["mode"],
        node_share=df_sw["MSGREG"],
        time=df_sw["time"],
        value=df_sw["share"],
        unit="%",
        year_act=df_sw["year"],
    )
    # --- End Ground Truth ---

    # Run the DSL engine
    input_dfs = {"df_sw": df_sw}
    results = []
    for rule in SHARE_MODE_RULES.get_rule():
        result_df = run_standard(rule, {"rule_dfs": input_dfs})
        results.append(result_df)
    result_df = pd.concat(results, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        result_df.sort_values(by=list(result_df.columns)).reset_index(drop=True),
        expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True),
    )


def test_e_flow_rules_bound(supply_test_data):
    """Test E_FLOW_RULES_BOUND, mimicking legacy make_df usage."""
    data = supply_test_data
    df_env = data["df_env"]

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Compare with water_supply_legacy.py lines 848-860
    # NB: Legacy code applies this only if context.SDG != "baseline"
    # We assume that condition is met for the test.
    expected_df = make_df(
        "bound_activity_lo",
        node_loc="B" + df_env["Region"].astype(str),
        technology="return_flow",
        year_act=df_env["year"],
        mode="M1",
        time=df_env["time"],
        value=df_env["value"],
        unit="km3/year",
    )

    # Legacy code also has a capping logic based on demand (lines 862-868)
    # This test focuses only on the make_df part.
    # Capping test would require df_dmd data and separate logic.

    # --- End Ground Truth ---

    # Run the DSL engine
    input_dfs = {"df_env": df_env}
    results = []
    for rule in E_FLOW_RULES_BOUND.get_rule():
        result_df = run_standard(rule, {"rule_dfs": input_dfs})
        # Apply post-processing similar to legacy if needed by the rule itself
        # (e.g., filtering by year >= 2025 if not handled by rule engine)
        # result_df = result_df[result_df["year_act"] >= 2025].reset_index(drop=True)
        results.append(result_df)
    result_df = pd.concat(results, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        result_df.sort_values(by=list(result_df.columns)).reset_index(drop=True),
        expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True),
    )


def test_desalination_output_rules(supply_test_data):
    """Test DESALINATION_OUTPUT_RULES, mimicking legacy make_df usage."""
    data = supply_test_data
    df_node = data["df_node"]
    sub_time = data["sub_time"]
    year_wat = (2020, 2030)
    first_year = 2020

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Compare with infrastructure.py add_desalination function
    expected_df = (
        make_df(
            "output",
            # node_loc is added by the broadcast call below, matching legacy
            technology="extract_salinewater_basin",
            value=1,
            unit="km3/year",
            level="water_avail_basin",
            commodity="salinewater_basin",
            mode="M1",
        )
        .pipe(
            broadcast,
            map_yv_ya_lt(year_wat, 20, first_year),
            node_loc=df_node["node"],
            time=pd.Series(sub_time),
        )
        .pipe(same_node)
        .pipe(same_time)
    )

    # --- End Ground Truth ---

    # Run the DSL engine
    # Arguments needed for the pipe flags in the rule
    output_args = {
        "lt": 20,  # technical lifetime from rule/legacy
        "rule_dfs": {},
        "node_loc": df_node["node"],
        "sub_time": sub_time,  # DSL expects Series/list directly
        "first_year": first_year,
        "year_wat": year_wat,
        # rule_dfs is not needed here as the rule value is constant
    }
    results = []
    for rule in DESALINATION_OUTPUT_RULES.get_rule():
        result_df = run_standard(rule, output_args)
        results.append(result_df)
    result_df = pd.concat(results, ignore_index=True)

    # Compare results
    pdt.assert_frame_equal(
        result_df.sort_values(by=list(result_df.columns)).reset_index(drop=True),
        expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True),
    )


# --- Cooling Technology Rules Tests ---


@pytest.fixture
def cool_tech_test_data():
    """Provides common dummy data for cooling technology tests."""
    input_cool = pd.DataFrame(
        {
            "node_loc": ["RegionA", "RegionB"],
            "technology_name": ["coal_ppl__ot_fresh", "gas_ppl__cl_fresh"],
            "year_vtg": [2020, 2020],
            "year_act": [2020, 2030],
            "mode": ["M1", "M2"],
            "node_origin": ["RegionA", "RegionB"],
            "value_return": [0.9, 0.8],  # Example return values for nexus case
        }
    )
    icfb_df = pd.DataFrame(
        {  # Mimics icmse_df filtered by node_loc
            "node_loc": ["RegionA", "RegionA"],
            "technology_name": ["coal_ppl__ot_fresh", "solar_csp__cl_fresh"],
            "year_vtg": [2020, 2020],
            "year_act": [2020, 2025],
            "mode": ["M1", "M1"],
            "value_return": [0.9, 0.85],
        }
    )
    df_node = pd.DataFrame(
        {
            "region": ["RegionA", "RegionA", "RegionB"],
            "node": ["BBasin1", "BBasin2", "BBasin3"],
        }
    )
    df_sw = pd.DataFrame(
        {  # Mimics output of map_basin_region_wat
            "node_dest": ["BBasin1", "BBasin2", "BBasin3"],
            "time_dest": ["year", "year", "year"],
            "year_act": [2020, 2020, 2020],
            "share": [
                0.6,
                0.4,
                1.0,
            ],  # Shares for node_dest BBasin1, BBasin2 in RegionA
        }
    )
    sub_time = pd.Series(["year"], name="time")
    return {
        "input_cool": input_cool,
        "icfb_df": icfb_df,
        "df_node": df_node,
        "df_sw": df_sw,
        "sub_time": sub_time,
    }


def test_cool_tech_output_rules(cool_tech_test_data):
    """Test COOL_TECH_OUTPUT_RULES, mimicking legacy make_df usage."""
    data = cool_tech_test_data
    input_cool = data["input_cool"]
    icfb_df = data["icfb_df"]  # Used for nexus condition
    df_node = data["df_node"]
    df_sw = data["df_sw"]
    sub_time = data["sub_time"]

    # --- Ground Truth Calculation (using make_df like legacy) ---
    # Compare with water_for_ppl.py lines 587-635

    # Part 1: Default output (level='share')
    expected_part1 = make_df(
        "output",
        node_loc=input_cool["node_loc"],
        technology=input_cool["technology_name"],
        year_vtg=input_cool["year_vtg"],
        year_act=input_cool["year_act"],
        mode=input_cool["mode"],
        node_dest=input_cool["node_origin"],
        commodity=input_cool["technology_name"]
        .str.split("__")
        .str[1],  # Legacy uses .str.get(1) which is equivalent
        level="share",
        time="year",
        time_dest="year",
        value=1,  # Legacy uses CT_CONST["cool_tech_output_default"] which is 1
        unit="-",
    )

    # Part 2: Nexus output (level='water_avail_basin')
    # Simulate the loop and broadcast/merge from legacy
    expected_part2_list = []

    for nn in icfb_df.node_loc.unique():
        icfb_df = icfb_df[icfb_df["node_loc"] == nn]
        bs = list(df_node[df_node["region"] == nn]["node"])  # Basins in the region

        out_t = make_df(
            "output",
            node_loc=icfb_df["node_loc"],
            technology=icfb_df["technology_name"],
            year_vtg=icfb_df["year_vtg"],
            year_act=icfb_df["year_act"],
            mode=icfb_df["mode"],
            commodity="surfacewater_basin",
            level="water_avail_basin",
            time="year",
            value=icfb_df["value_return"],
            unit="MCM/GWa",  # Legacy used MCM/GWa, rule uses M CM/GWa
        ).pipe(
            broadcast,
            node_dest=bs,
            time_dest=pd.Series(sub_time),  # Legacy passes pd.Series(sub_time)
        )
        # Merge with basin shares (df_sw)
        out_t = out_t.merge(df_sw, how="left")
        # Multiply by basin water availability share
        out_t["value"] = out_t["value"] * out_t["share"]
        out_t.drop(columns={"share"}, inplace=True)
        expected_part2_list.append(out_t)

        expected_part2 = pd.concat(expected_part2_list, ignore_index=True)
        # Legacy drops NA after merge, but our merge setup shouldn't create NAs here
        # Legacy resets index

        expected_df = pd.concat([expected_part1, expected_part2], ignore_index=True)
        # --- Apply dropna to match legacy behaviour ---
        expected_df = expected_df.dropna(subset=["value"]).reset_index(drop=True)
        # --- End Ground Truth ---

        # Run the DSL engine
        all_results = []  # Initialize list to store results from all rules
        # Define rule_dfs structure similar to what cool_tech_rf.py might pass
        rule_dfs_base = {
            "input_cool": input_cool,
            "icfb_df": icfb_df,
            "df_node": df_node,
            "df_sw": df_sw,
        }

        for rule in COOL_TECH_OUTPUT_RULES.get_rule():
            # Ensure correct condition checking and handling
            current_condition = rule.get("condition")

            if current_condition == "default":
                # Pass only input_cool for the default rule
                default_result_df = run_standard(rule, {"rule_dfs": input_cool})
                all_results.append(default_result_df)  # Append default results

            elif current_condition == "nexus":
                # Handle the 'nexus' rule
                nexus_results_list = []
                # Ensure rule_dfs_base["icfb_df"] exists and is not empty before looping
                if "icfb_df" in rule_dfs_base and not rule_dfs_base["icfb_df"].empty:
                    for nn in rule_dfs_base["icfb_df"].node_loc.unique():
                        # Filter the icfb_df for the current node_loc 'nn'
                        current_icfb_df = rule_dfs_base["icfb_df"][
                            rule_dfs_base["icfb_df"]["node_loc"] == nn
                        ]
                        # Pass the filtered icfb_df along with other required data
                        current_rule_dfs = {
                            "icfb_df": current_icfb_df,
                            "df_node": rule_dfs_base["df_node"],
                            "df_sw": rule_dfs_base["df_sw"],
                        }
                        # The broadcast in legacy happens *after* make_df.
                        # The rule itself has "flag_broadcast": True.
                        # We need to provide the target dimensions for broadcasting.
                        bs = list(
                            rule_dfs_base["df_node"][
                                rule_dfs_base["df_node"]["region"] == nn
                            ]["node"]
                        )
                        extra_args = {
                            "node_dest": bs,
                            "time_dest": data["sub_time"],  # Pass the series directly
                        }
                        results_nexus_run = run_standard(
                            rule, {"rule_dfs": current_rule_dfs}, extra_args=extra_args
                        )

                        results_nexus_processed = results_nexus_run.merge(
                            df_sw, how="left"
                        )
                        results_nexus_processed["value"] = (
                            results_nexus_processed["value"]
                            * results_nexus_processed["share"]
                        )
                        results_nexus_processed.drop(columns={"share"}, inplace=True)
                        nexus_results_list.append(results_nexus_processed)

                    # Concatenate results for
                    #  this specific rule condition *after* the nn loop
                    if nexus_results_list:  # Only proceed if list is not empty
                        nexus_result_df = pd.concat(
                            nexus_results_list, ignore_index=True
                        )
                        nexus_result_df = nexus_result_df.dropna(
                            subset=["value"]
                        )  # Match legacy dropna
                        if (
                            not nexus_result_df.empty
                        ):  # Only append if not empty after dropna
                            all_results.append(
                                nexus_result_df
                            )  # Append processed nexus results
                    # If nexus_results_list is empty or becomes
                    # empty after dropna, nothing is appended
                    # for this rule.
            result_df = pd.concat(all_results, ignore_index=True)

        # Compare results
        # Ensure columns are in the same order and types are compatible
        expected_cols = sorted(list(expected_df.columns))
        result_cols = sorted(list(result_df.columns))
        assert expected_cols == result_cols, (
            "Columns differ between expected and result"
        )

        pdt.assert_frame_equal(
            result_df.sort_values(by=expected_cols).reset_index(drop=True),
            expected_df.sort_values(by=expected_cols).reset_index(drop=True),
            check_dtype=False,  # Allow int/float differences
            check_like=True,  # Ignore column order before sorting
        )
