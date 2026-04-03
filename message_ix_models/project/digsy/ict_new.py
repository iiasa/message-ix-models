import numpy as np
import pandas as pd
import pint_pandas
from message_ix import make_df

from message_ix_models import ScenarioInfo
from message_ix_models.util import private_data_path

projections_2100_path = "R12_Clean IAM Version_Finalised_2100_update_2026-03-31.xlsx"
sheet_name = "R5_DC_2100"
ENERGY_PATH = private_data_path("projects", "digsy", "ict")

PRE2030_CHN_SHARE = (
    277 / 378
)  # ~73.28% (Based on IEA) Fixed China share in Asia Pacific (Based on IEA)

APAC_REMAINDER_SHARES = {
    "R12_RCPA": 0.034,
    "R12_PAS": 0.302,
    "R12_SAS": 0.150,
    "R12_PAO": 0.514,
}

EUROPE_SHARES = {
    "R12_WEU": 0.75,
    "R12_EEU": 0.14,
    "R12_FSU": 0.11,
}

R12_TO_IEA_PARENT = {
    "R12_AFR": "Africa",
    "R12_CHN": "Asia Pacific",
    "R12_PAS": "Asia Pacific",
    "R12_RCPA": "Asia Pacific",
    "R12_SAS": "Asia Pacific",
    "R12_PAO": "Asia Pacific",
    "R12_LAM": "Central and South America",
    "R12_EEU": "Europe",
    "R12_WEU": "Europe",
    "R12_FSU": "Europe",
    "R12_MEA": "Middle East",
    "R12_NAM": "North America",
}

R12_TO_MASANET_PARENT = {
    "R12_CHN": "Asia Pacific",
    "R12_PAS": "Asia Pacific",
    "R12_RCPA": "Asia Pacific",
    "R12_SAS": "Asia Pacific",
    "R12_PAO": "Asia Pacific",
    "R12_EEU": "Europe",
    "R12_WEU": "Europe",
    "R12_FSU": "Europe",
    "R12_NAM": "North America",
    "R12_LAM": "Latin America",
    "R12_MEA": "Middle East and Africa",
    "R12_AFR": "Middle East and Africa",
}


def merge_r12_to_parent(
    projection_df: pd.DataFrame, parent_map, reg_col="Region"
) -> pd.DataFrame:
    projection_df = (
        pd.DataFrame(parent_map.items(), columns=["MESSAGE", reg_col])
        .merge(projection_df, on=reg_col)
        .drop(reg_col, axis=1)
    )
    projection_df = projection_df.set_index(
        [i for i in projection_df.columns if i not in ["Value", reg_col]]
    )
    return projection_df


def disaggregate_parent_to_children(projection_df: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame.from_dict(
        {k: v * (1 - PRE2030_CHN_SHARE) for k, v in APAC_REMAINDER_SHARES.items()}
        | EUROPE_SHARES
        | {"R12_CHN": PRE2030_CHN_SHARE},
        orient="index",
        columns=["Value"],
    )
    df.index.name = "MESSAGE"
    projection_df = projection_df.mul(df, fill_value=1)
    return projection_df


def run_pre2030_distribution() -> pd.DataFrame:
    projection_df = pd.read_csv(
        ENERGY_PATH.joinpath("2020-2030_Regional DC.csv"), usecols=[0, 1, 3]
    ).rename(columns={"Lower Bound (TWh)": "Value"})
    pmap = R12_TO_IEA_PARENT.copy()
    pmap.update({"R12_LAM": "Latin America"})
    projection_df = merge_r12_to_parent(projection_df, parent_map=pmap)
    projection_df = disaggregate_parent_to_children(projection_df).rename_axis(
        index={"MESSAGE": "Region"}
    )
    return projection_df


def run_iea_distribution() -> pd.DataFrame:
    iea_proj_2050 = pd.read_csv(
        ENERGY_PATH.joinpath(
            "DC_Projection_2035_2050_IEA_Demand_Led_Corrected_NOGDP.csv"
        )
    ).rename(columns={"Projected_Overall_TWh": "Value"})
    iea_proj_2050 = merge_r12_to_parent(
        iea_proj_2050, R12_TO_IEA_PARENT, reg_col="Region (IEA)"
    )
    iea_dist_2050 = disaggregate_parent_to_children(iea_proj_2050)
    iea_proj_2050_2100 = (
        pd.read_excel(
            ENERGY_PATH.joinpath(projections_2100_path),
            sheet_name="R5 DC_2100_revised",
            usecols=[0, 1, 2, 4],
        )
        .rename(columns={"Upper Bound DC (TWh)": "Value"})
        .query("Year >= 2055")
    )
    iea_proj_2050_2100 = merge_r12_to_parent(
        iea_proj_2050_2100, parent_map=R12_TO_MASANET_PARENT
    )
    mea_afr_2050 = (
        iea_dist_2050.loc["R12_MEA", 2050] + iea_dist_2050.loc["R12_AFR", 2050]
    )
    shr_afr_mea_2050 = (
        iea_dist_2050.swaplevel(0, 1).loc[2050].loc[["R12_AFR", "R12_MEA"], :]
        / mea_afr_2050
    )
    iea_proj_2050_2100 = disaggregate_parent_to_children(iea_proj_2050_2100).mul(
        shr_afr_mea_2050.swaplevel(0, 1), fill_value=1
    )
    iea_proj = pd.concat([iea_dist_2050, iea_proj_2050_2100])
    return iea_proj.rename_axis(index={"MESSAGE": "Region"})


def load_masanet_elasticities(elasticity_df: pd.DataFrame) -> dict:
    out = elasticity_df.copy()
    out.columns = out.columns.str.strip()
    out["Region"] = out["Region"].astype(str).str.strip()
    for col in ("Year_Start", "Year_End"):
        out[col] = pd.to_numeric(out[col], errors="coerce")

    masanet_raw = out.query("Year_Start == 2012 and Year_End == 2017").copy()
    masanet_mea = masanet_raw[masanet_raw["Region"] == "Middle East and Africa"]
    if masanet_mea.empty:
        return {}

    return (
        masanet_mea.groupby("Region", as_index=True)[["beta", "regional_constant"]]
        .mean()
        .to_dict("index")
    )


def run_masanet_distribution() -> pd.DataFrame:
    masanet_proj = (
        pd.read_excel(
            ENERGY_PATH.joinpath(projections_2100_path),
            sheet_name="R5 DC_2100_revised",
            usecols=[0, 1, 2, 3],
        )
        .rename(columns={"Lower Bound DC (TWh)": "Value"})
        .query("Year >= 2035")
    )
    masanet_proj = merge_r12_to_parent(
        masanet_proj, R12_TO_MASANET_PARENT, reg_col="Region"
    )
    masanet_proj = disaggregate_parent_to_children(masanet_proj)

    elasticity_df = pd.read_csv(ENERGY_PATH.joinpath("regional_dc_elasticity.csv"))
    masanet_elast = load_masanet_elasticities(elasticity_df)
    beta = masanet_elast["Middle East and Africa"]["beta"]
    const = masanet_elast["Middle East and Africa"]["regional_constant"]
    basepath = "socio_economic_projections/"
    dig_trans_2030 = (
        pd.read_csv(
            ENERGY_PATH.joinpath("2030 region aggregated_by_messageix.csv"),
            usecols=[0, 1, 2, 5],
        )
        .rename(columns={"Region (MESSAGEix)": "Region"})
        .set_index("Region")["Dig. Trans"]
    )
    dig_trans_proj = (
        pd.read_csv(
            ENERGY_PATH.joinpath("Dig Trans_with_messageix_by_region2100.csv"),
            usecols=[0, 1, 2, 4],
        )
        .rename(columns={"Region (MESSAGEix)": "Region"})
        .query("Year >= 2035")
        .set_index(["Region", "Year", "Scenario"])["Dig Trans"]
    )
    weights = np.exp(
        beta * dig_trans_proj.apply(np.log).sub(dig_trans_2030.apply(np.log)) + const
    )
    weights = weights.reset_index().assign(
        parent=weights.reset_index()["Region"].map(R12_TO_MASANET_PARENT)
    )
    weights = (
        weights.set_index(["Region", "Year", "Scenario", "parent"])
        .div(weights.groupby(["parent", "Year", "Scenario"]).sum(numeric_only=True))
        .droplevel(3)
        .rename(columns={0: "Value"})
    )
    masanet_proj = masanet_proj.rename_axis(index={"MESSAGE": "Region"}).mul(
        weights.loc[["R12_AFR", "R12_MEA"]], fill_value=1
    )
    return masanet_proj


def build_weighted_distribution() -> None:
    IEA_LOW_2035, IEA_HIGH_2035, IEA_CENTRAL_2035 = 707, 1719, 1193
    iea_index = (IEA_CENTRAL_2035 - IEA_LOW_2035) / (IEA_HIGH_2035 - IEA_LOW_2035)
    iea_index = min(1.0, max(0.0, iea_index))
    masanet = run_masanet_distribution()
    iea = run_iea_distribution()
    return masanet.add(iea.sub(masanet).mul(iea_index))


def build_avg_distribution() -> None:
    masanet = run_masanet_distribution()
    iea = run_iea_distribution()
    return masanet.add(iea).div(2)


def gen_post2030_proj(dc_scen: str, ssp: str):
    scen_fmap = {
        "Low": run_masanet_distribution,
        "Medium": build_weighted_distribution,
        "High": run_iea_distribution,
        "Mean": build_avg_distribution,
    }
    return scen_fmap[dc_scen]().loc[:, :, ssp]


def tc_ratio(tc_scen: str) -> float:
    ratios = {"High": 0.91, "Low": 0.78, "Medium": 0.78}
    return ratios[tc_scen]


def convert_to_demand_par(df: pd.DataFrame, commodity: str, s_info):
    return make_df(
        "demand",
        **df["Value"]
        .astype("pint[TWh]")
        .pint.to("GWa")
        .pint.magnitude.to_frame()
        .assign(unit="GWa", commodity=commodity)
        .reset_index()
        .query("Year in @s_info.Y")
        .rename(columns={"Region": "node", "Year": "year", "Value": "value"}),
        level="demand",
        time="year",
    )


def generate_demand(dc_scen, ssp, s_info: "ScenarioInfo") -> pd.DataFrame:
    pre_2030 = run_pre2030_distribution()
    post2030 = gen_post2030_proj(dc_scen, ssp)
    tc_2020 = pre_2030.loc[:, [2020], :].mul(0.91)
    tc_post_2020 = pd.concat([post2030, pre_2030.loc[:, [2025, 2030], :]]).mul(
        tc_ratio(dc_scen)
    )
    tc_proj = pd.concat([tc_2020, tc_post_2020])
    dc_proj = pd.concat([pre_2030, post2030])
    demands = pd.concat(
        [
            convert_to_demand_par(dc_proj, "data_centre_elec", s_info),
            convert_to_demand_par(tc_proj, "tele_comm_elec", s_info),
        ]
    )
    demands = pd.concat([demands, demands[demands["year"] == 2100].assign(year=2110)])
    return demands
