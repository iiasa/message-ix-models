import pandas as pd
import pytest
from message_ix.util import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    gen_plastics_emission_factors,
    map_iea_db_to_msg_regs,
)

DATA = [
    ["ALB", "R12_EEU"],
    ["AND", "R12_WEU"],
    ["ARM", "R12_FSU"],
    ["ASME", "R12_MEA"],
    ["AUS", "R12_PAO"],
    ["AUT", "R12_WEU"],
    ["AZE", "R12_FSU"],
    ["BEL", "R12_WEU"],
    ["BGD", "R12_SAS"],
    ["BGR", "R12_EEU"],
    ["BIH", "R12_EEU"],
    ["BLR", "R12_FSU"],
    ["BRN", "R12_PAS"],
    ["CAN", "R12_NAM"],
    ["CHE", "R12_WEU"],
    ["CHINAREG", "R12_CHN"],
    ["CHL", "R12_LAM"],
    ["COL", "R12_LAM"],
    ["CRI", "R12_LAM"],
    ["CYP", "R12_WEU"],
    ["CZE", "R12_EEU"],
    ["DEU", "R12_WEU"],
    ["DNK", "R12_WEU"],
    ["DZA", "R12_MEA"],
    ["EGY", "R12_MEA"],
    ["ESP", "R12_WEU"],
    ["EST", "R12_EEU"],
    ["FIN", "R12_WEU"],
    ["FRA", "R12_WEU"],
    ["FRO", "R12_WEU"],
    ["GBR", "R12_WEU"],
    ["GEO", "R12_FSU"],
    ["GIB", "R12_WEU"],
    ["GRC", "R12_WEU"],
    ["GREENLAND", "R12_WEU"],
    ["HRV", "R12_EEU"],
    ["HUN", "R12_EEU"],
    ["IDN", "R12_PAS"],
    ["IIASA_AFRICA", "R12_AFR"],
    ["IIASA_PAS", "R12_PAS"],
    ["IIASA_SAS", "R12_SAS"],
    ["IND", "R12_SAS"],
    ["IRL", "R12_WEU"],
    ["ISL", "R12_WEU"],
    ["ISR", "R12_MEA"],
    ["ITA", "R12_WEU"],
    ["JPN", "R12_PAO"],
    ["KAZ", "R12_FSU"],
    ["KGZ", "R12_FSU"],
    ["KHM", "R12_RCPA"],
    ["KOR", "R12_PAS"],
    ["KOSOVO", "R12_EEU"],
    ["LAO", "R12_RCPA"],
    ["LATAMER", "R12_LAM"],
    ["LBY", "R12_MEA"],
    ["LIE", "R12_WEU"],
    ["LKA", "R12_SAS"],
    ["LTU", "R12_EEU"],
    ["LUX", "R12_WEU"],
    ["LVA", "R12_EEU"],
    ["MAR", "R12_MEA"],
    ["MDA", "R12_FSU"],
    ["MEX", "R12_LAM"],
    ["MKD", "R12_EEU"],
    ["MLT", "R12_WEU"],
    ["MMR", "R12_PAS"],
    ["MNE", "R12_EEU"],
    ["MNG", "R12_RCPA"],
    ["MPALESTINE", "R12_MEA"],
    ["MYS", "R12_PAS"],
    ["NLD", "R12_WEU"],
    ["NOR", "R12_WEU"],
    ["NPL", "R12_SAS"],
    ["NZL", "R12_PAO"],
    ["PAK", "R12_SAS"],
    ["PHL", "R12_PAS"],
    ["POL", "R12_EEU"],
    ["PRK", "R12_RCPA"],
    ["PRT", "R12_WEU"],
    ["ROU", "R12_EEU"],
    ["RUS", "R12_FSU"],
    ["SDN", "R12_MEA"],
    ["SGP", "R12_PAS"],
    ["SJM", "R12_WEU"],
    ["SRB", "R12_EEU"],
    ["SSD", "R12_MEA"],
    ["SVK", "R12_EEU"],
    ["SVN", "R12_EEU"],
    ["SWE", "R12_WEU"],
    ["THA", "R12_PAS"],
    ["TJK", "R12_FSU"],
    ["TKM", "R12_FSU"],
    ["TUN", "R12_MEA"],
    ["TUR", "R12_WEU"],
    ["TWN", "R12_PAS"],
    ["UKR", "R12_FSU"],
    ["USA", "R12_NAM"],
    ["UZB", "R12_FSU"],
    ["VNM", "R12_RCPA"],
]


def test_map_iea_db_to_msg_regs() -> None:
    # Convert test data to data frame
    df = pd.DataFrame(DATA, columns=["COUNTRY", "REGION"])

    # Function argument: a data frame without a "REGION" column
    df_in = df[["COUNTRY"]]

    # Function runs, returns a data frame with an added "REGION" column
    df_out = map_iea_db_to_msg_regs(df_in)

    # Check correctness:
    # - Merge `df_out` and `df`; this yields columns "REGION_x" and "REGION_y".
    # - Add a column with True if these two are equal.
    # - Assert all are equal.
    assert df_out.merge(df, on="COUNTRY").eval("Z = REGION_x == REGION_y").Z.all()


@pytest.mark.parametrize(
    "species",
    [
        "HVCs",
        pytest.param("methanol", marks=pytest.mark.xfail(raises=NotImplementedError)),
    ],
)
def test_gen_plastics_emission_factors(species):
    info = ScenarioInfo()
    info.set["node"] = ["node0", "node1"]
    info.set["year"] = [2020, 2025]
    out = gen_plastics_emission_factors(info, "HVCs")

    assert not out.isna().any(axis=None)  # Completely full

    # Data have the expected columns
    assert sorted(make_df("relation_activity").columns) == sorted(out.columns)
