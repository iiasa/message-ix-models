import numpy as np
import pandas as pd

from message_ix_models.tools.iea.weo import (
    adj_nam_cost_conversion,
    adj_nam_cost_manual,
    adj_nam_cost_message,
    adj_nam_cost_reference,
    calculate_region_cost_ratios,
    compare_original_and_weo_nam_costs,
    conversion_2017_to_2005_usd,
    dict_weo_r11,
    dict_weo_technologies,
    get_cost_assumption_data,
    get_weo_data,
)


def test_get_weo_data():
    result = get_weo_data()

    # Check that the minimum and maximum years are correct
    assert min(result.year) == "2021"
    assert max(result.year) == "2050"

    # Check that the regions are correct
    # (e.g., in the past, "Europe" changed to "European Union")
    assert all(
        [
            "European Union",
            "United States",
            "Japan",
            "Russia",
            "China",
            "India",
            "Middle East",
            "Africa",
        ]
        == result.region.unique()
    )

    # Check one sample value
    assert (
        result.loc[
            (result.technology == "steam_coal_subcritical")
            & (result.region == "United States")
            & (result.year == "2021")
            & (result.cost_type == "capital_costs"),
            "value",
        ].values[0]
        == 1800
    )


def test_get_cost_assumption_data():
    res = get_cost_assumption_data()

    assert len(res.index) == 122
    assert (
        round(
            res.loc[
                (res.message_technology == "coal_ppl")
                & (res.cost_type == "capital_costs"),
                "cost_NAM_original_message",
            ].values[0]
        )
        == 1435
    )
    assert (
        round(
            res.loc[
                (res.message_technology == "coal_ppl")
                & (res.cost_type == "annual_om_costs"),
                "cost_NAM_original_message",
            ].values[0]
        )
        == 57
    )


def test_compare_original_and_weo_nam_costs():
    weo = get_weo_data()
    orig = get_cost_assumption_data()

    res = compare_original_and_weo_nam_costs(
        weo, orig, dict_weo_technologies, dict_weo_r11
    )

    assert dict_weo_r11["NAM"] == "United States"
    assert dict_weo_technologies["coal_ppl"] == "steam_coal_subcritical"
    assert min(weo.year) == "2021"
    assert (
        round(
            res.loc[
                (res.message_technology == "coal_ppl")
                & (res.cost_type == "capital_costs"),
                "cost_NAM_original_message",
            ].values[0]
        )
        == 1435
    )
    assert (
        round(
            res.loc[
                (res.message_technology == "coal_ppl")
                & (res.cost_type == "capital_costs"),
                "cost_NAM_weo_2021",
            ].values[0]
        )
        == 1800
    )


def test_conversion_rate():
    assert round(conversion_2017_to_2005_usd, 2) == 0.81


def test_adj_nam_cost_conversion():
    dummy_data = pd.DataFrame({"cost_NAM_weo_2021": [1, 10, 100]})
    adj_nam_cost_conversion(dummy_data, conversion_2017_to_2005_usd)

    assert round(dummy_data["cost_NAM_adjusted"], 2).array == [0.81, 8.1, 80.97]


def test_adj_nam_cost_message():
    dummy_message_tech = ["coal_ppl", "gas_ppl", "biomass_i"]
    dummy_weo_tech = ["steam_coal_subcritical", "gas_turbine", "bioenergy_medium_chp"]
    dummy_inv_cost = [1000, 500, 250]
    dummy_fom_cost = [100, 45, 30]
    dummy_columns = [
        "message_technology",
        "weo_technology",
        "cost_type",
        "cost_NAM_original_message",
    ]

    dummy_df1 = pd.DataFrame(
        data=[
            dummy_message_tech,
            dummy_weo_tech,
            ["capital_costs", "capital_costs", "capital_costs"],
            dummy_inv_cost,
        ],
    ).T
    dummy_df1.columns = dummy_columns

    dummy_df2 = pd.DataFrame(
        data=[
            dummy_message_tech,
            dummy_weo_tech,
            ["annual_om_costs", "annual_om_costs", "annual_om_costs"],
            dummy_fom_cost,
        ],
    ).T
    dummy_df2.columns = dummy_columns

    dummy_df = pd.concat([dummy_df1, dummy_df2])

    adj_nam_cost_message(dummy_df, ["biomass_i"], ["gas_ppl"])

    assert (
        bool(
            dummy_df.loc[
                (dummy_df.message_technology == "gas_ppl")
                & (dummy_df.cost_type == "annual_om_costs"),
                "cost_NAM_original_message",
            ].values[0]
            == dummy_df.loc[
                (dummy_df.message_technology == "gas_ppl")
                & (dummy_df.cost_type == "annual_om_costs"),
                "cost_NAM_adjusted",
            ].values[0]
        )
        is True
    )

    assert (
        bool(
            dummy_df.loc[
                (dummy_df.message_technology == "gas_ppl")
                & (dummy_df.cost_type == "annual_om_costs"),
                "cost_NAM_original_message",
            ].values[0]
            == dummy_df.loc[
                (dummy_df.message_technology == "gas_ppl")
                & (dummy_df.cost_type == "annual_om_costs"),
                "cost_NAM_adjusted",
            ].values[0]
        )
        is True
    )


def test_adj_nam_cost_manual():
    dummy_dict_inv = {
        "wind_ppl": 1111,
        "wind_ppf": 2222,
        "solar_pv_ppl": 3333,
    }

    dummy_dict_fom = {
        "h2_coal": 111,
        "h2_smr": 222,
        "h2_coal_ccs": 333,
    }

    dummy_dict_all = dict(dummy_dict_inv)
    dummy_dict_all.update(dummy_dict_fom)

    weo = get_weo_data()
    orig = get_cost_assumption_data()

    res = compare_original_and_weo_nam_costs(
        weo, orig, dict_weo_technologies, dict_weo_r11
    )
    res = res.loc[res.message_technology.isin(dummy_dict_all)]
    adj_nam_cost_manual(res, dummy_dict_inv, dummy_dict_fom)

    assert np.all(
        res.loc[
            (res.message_technology.isin(dummy_dict_inv))
            & (res.cost_type == "capital_costs"),
            "cost_NAM_adjusted",
        ].values
        == [i for i in dummy_dict_inv.values()]
    )

    assert np.all(
        res.loc[
            (res.message_technology.isin(dummy_dict_fom))
            & (res.cost_type == "annual_om_costs"),
            "cost_NAM_adjusted",
        ].values
        == [i for i in dummy_dict_fom.values()]
    )


def test_adj_nam_cost_reference():
    dummy_message_tech = ["tech1", "tech2", "tech3"]
    dummy_inv_cost = [1555, 762, 800]
    dummy_fom_cost = [97, 45, 30]
    dummy_inv_cost_adj = [1750, 800, 670]
    dummy_fom_cost_adj = [85, 56, 27]

    dummy_columns = [
        "message_technology",
        "cost_type",
        "cost_NAM_original_message",
        "cost_NAM_adjusted",
    ]

    dummy_df1 = pd.DataFrame(
        data=[
            dummy_message_tech,
            ["capital_costs", "capital_costs", "capital_costs"],
            dummy_inv_cost,
            dummy_inv_cost_adj,
        ],
    ).T
    dummy_df1.columns = dummy_columns

    dummy_df2 = pd.DataFrame(
        data=[
            dummy_message_tech,
            ["annual_om_costs", "annual_om_costs", "annual_om_costs"],
            dummy_fom_cost,
            dummy_fom_cost_adj,
        ],
    ).T
    dummy_df2.columns = dummy_columns

    dummy_df = pd.concat([dummy_df1, dummy_df2])

    dummy_dict_inv = {
        "tech2": {"reference_tech": "tech1", "reference_cost_type": "capital_costs"}
    }
    dummy_dict_fom = {
        "tech2": {"reference_tech": "tech3", "reference_cost_type": "annual_om_costs"}
    }

    adj_nam_cost_reference(dummy_df, dummy_dict_inv, dummy_dict_fom)

    assert (
        bool(
            dummy_df.loc[
                (dummy_df.message_technology == "tech2")
                & (dummy_df.cost_type == "capital_costs"),
                "cost_NAM_adjusted",
            ].values[0]
            == (1750 * (762 / 1555))
        )
        is True
    )

    assert (
        bool(
            dummy_df.loc[
                (dummy_df.message_technology == "tech2")
                & (dummy_df.cost_type == "annual_om_costs"),
                "cost_NAM_adjusted",
            ].values[0]
            == (27 * (45 / 30))
        )
        is True
    )


def test_calculate_region_cost_ratios():
    weo = get_weo_data()
    res = calculate_region_cost_ratios(weo, dict_weo_r11)

    assert np.all(
        [
            min(res.loc[res.r11_region == "NAM"].cost_ratio),
            max(res.loc[res.r11_region == "NAM"].cost_ratio),
        ]
        == [1, 1]
    )
