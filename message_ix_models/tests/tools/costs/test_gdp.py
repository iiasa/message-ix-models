import numpy as np

from message_ix_models.tools.costs.gdp import (
    get_gdp_data,
    linearly_regress_tech_cost_vs_gdp_ratios,
)
from message_ix_models.tools.costs.weo import (
    DICT_COST_COLS,
    DICT_TECH_ROWS,
    DICT_WEO_R11,
    calculate_region_cost_ratios,
    get_weo_data,
)

res = get_gdp_data()


def test_get_gdp_data():
    res = get_gdp_data()

    # Check SSP1, SSP2, and SSP3 are all present in the data
    assert np.all(res.scenario.unique() == ["SSP1", "SSP2", "SSP3"])

    # Check that R11 regions are present
    assert np.all(
        res.r11_region.unique()
        == ["AFR", "CPA", "EEU", "FSU", "LAM", "MEA", "NAM", "PAO", "PAS", "SAS", "WEU"]
    )

    # Check that the GDP ratio for NAM is zero
    assert min(res.loc[res.r11_region == "NAM", "gdp_ratio_oecd"]) == 1.0
    assert max(res.loc[res.r11_region == "NAM", "gdp_ratio_oecd"]) == 1.0


df_gdp = get_gdp_data()
df_weo = get_weo_data(DICT_TECH_ROWS, DICT_COST_COLS)
df_tech_cost_ratios = calculate_region_cost_ratios(df_weo, DICT_WEO_R11)

dff = linearly_regress_tech_cost_vs_gdp_ratios(df_gdp, df_tech_cost_ratios)


def test_linearly_regress_tech_cost_vs_gdp_ratios():
    df_gdp = get_gdp_data()
    df_weo = get_weo_data(DICT_TECH_ROWS, DICT_COST_COLS)
    df_tech_cost_ratios = calculate_region_cost_ratios(df_weo, DICT_WEO_R11)

    res = linearly_regress_tech_cost_vs_gdp_ratios(df_gdp, df_tech_cost_ratios)

    # Check SSP1, SSP2, and SSP3 are all present in the data
    assert np.all(res.scenario.unique() == ["SSP1", "SSP2", "SSP3"])

    # Check capital costs and annual o&m costs are present in the data
    assert np.all(res.cost_type.unique() == ["annual_om_costs", "capital_costs"])

    # The absolute value of the slopes should be less than 1 probably
    assert abs(min(res.slope)) <= 1
    assert abs(max(res.slope)) <= 1
