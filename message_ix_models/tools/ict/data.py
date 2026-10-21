from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import pint
from message_ix import make_df

from message_ix_models.tools.ict.data_center import (
    get_cisco_region_mapping,
    get_ignored_countries,
    missing,
    read_data_centre_count_shares,
    read_hist_data,
    read_regr_pars,
)
from message_ix_models.tools.ict.digtrans import aggregate_to_region, read_country_data
from message_ix_models.tools.ict.iea import (
    get_weo_iso_map,
    read_2035_world_projections,
    read_iea_data,
)
from message_ix_models.util import merge_data, pycountry

from .config import Config
from .util import compute_delta_ene, get_weu_eeu_share, run_regression

if TYPE_CHECKING:
    from message_ix_models import Context, ScenarioInfo, Spec
    from message_ix_models.types import ParameterData


def gen_data(ctx: "Context", sinfo: "ScenarioInfo", s: "Spec") -> "ParameterData":
    demand_map = {
        "Low": gen_ict_demand,
        "High": gen_ict_demand,
        "Mean": build_avg_distribution,
        "Central": build_weighted_distribution,
    }
    cfg: Config = ctx.ict
    demand = demand_map[cfg.scenario](ctx)
    par_data = format_to_parameter(
        demand, get_data_center_tec_code(s, "Data centers"), sinfo
    )
    merge_data(
        par_data,
        gen_infra_demand(
            par_data["fixed_activity"],
            cfg,
            get_data_center_tec_code(s, "Telecommunication infrastructure"),
        ),
    )
    merge_data(par_data, gen_input(par_data["fixed_activity"]))
    return par_data


def gen_ict_demand(ctx: "Context") -> pd.DataFrame:
    regression_cfg = {
        "Low": {"Sensitivity": "Efficiency Dominant", "Period": "2012-2017"},
        "High": {"Sensitivity": "Service Demand Dominant", "Period": "2020-2025"},
    }
    ict_scen = ctx.ict.scenario
    # - Read fixed trajectory until 2030 from IEA
    pre_2030 = get_pre2030_data()

    # - Read regression parameters from Fan et al. 2026 or compute from raw data
    pars = read_regr_pars()
    p2030 = pars.loc[
        regression_cfg[ict_scen]["Sensitivity"], regression_cfg[ict_scen]["Period"]
    ]

    # - Read dig trans data from Fan et al. 2025
    df = read_country_data(ctx.ict.ssp, full=True)
    ignored = get_ignored_countries()
    cmap = {k: v for k, v in get_cisco_region_mapping().items() if k not in ignored}
    pycountry.COUNTRY_NAME.update(missing)
    cmap = {pycountry.iso_3166_alpha_3(k): v for k, v in cmap.items()}
    if ict_scen == "High":
        cmap = {
            k: v.replace("Latin America", "Central and South America")
            for k, v in get_weo_iso_map().items()
            if k in cmap.keys()
        }
        pre_2030.rename({"Latin America": "Central and South America"}, inplace=True)
    dig_trans = aggregate_to_region(df, cmap).swaplevel(0, 1)

    # - Compute energy projection stepwise until 2050
    # (or model last period if earlier) iteratively starting from IEA 2030
    years = [
        i
        for i in dig_trans.index.get_level_values(0).unique()
        if (i >= 2030) & (i <= 2050)
    ]
    ene_proj = project_demand(
        dig_trans,
        years,
        (p2030["beta"], p2030["eps"]),
        pre_2030.swaplevel(0, 1).loc[2030, "Value"],
        ict_scen,
    )
    ts = pd.concat([pre_2030, ene_proj])

    # - Extrapolate beyond 2050 if requested
    if ctx.ict.lastyear > 2050:
        y_post2050 = [
            i for i in dig_trans.index.get_level_values(0).unique() if (i >= 2050)
        ]
        ene_post2050 = project_demand(
            dig_trans,
            y_post2050,
            (get_long_term_elasticity()),
            ene_proj.swaplevel(0, 1).loc[2050, "Value"],
            ict_scen,
        )
        ts = pd.concat([ts, ene_post2050])

    # - Concatenate IEA and projected data
    if ict_scen == "High":
        ts.rename({"Central and South America": "Latin America"}, inplace=True)
    # TODO: make disaggregation read node config
    # - Disaggregate to MESSAGEix regions (currently only R12 supported)
    dc_count = gen_disaggregation_shares(pre_2030)
    final = dc_count.mul(ts).droplevel(1).dropna()
    return final


def gen_infra_demand(df: pd.DataFrame, cfg: Config, tec_code: str) -> pd.DataFrame:
    tc2020 = df[df["year_act"] == 2020].assign(value=lambda x: x["value"] * 0.91)
    tc_post_2020 = df[df["year_act"] != 2020].assign(
        value=lambda x: x["value"] * cfg.tele_comm_ratio
    )
    tc = pd.concat([tc2020, tc_post_2020]).assign(technology=tec_code)
    return {"fixed_activity": tc.round(5)}


def format_to_parameter(
    df: pd.DataFrame, tec: str, sinfo: "ScenarioInfo"
) -> "ParameterData":
    par_data = (
        make_df(
            "fixed_activity",
            **df.reset_index()
            .rename(columns={"Value": "value", "R12": "node_loc", "Year": "year_act"})
            .assign(year_vtg=lambda x: x["year_act"]),
            time="year",
            mode="M1",
            technology=tec,
        )
        .assign(
            value=lambda x: pint.Quantity(x["value"].to_numpy(), "TWh").to("GW*a"),
            unit="GWa",
        )
        .query("year_act in @sinfo.Y")
    )
    return {"fixed_activity": par_data}


def gen_input(df: pd.DataFrame) -> "ParameterData":
    dims = dict(time_origin="year", commodity="electr", level="final")
    inp = (
        make_df("input", **df, **dims)
        .assign(node_origin=lambda x: x["node_loc"], value=1.0)
        .round(5)
    )
    return {"input": inp}


def build_avg_distribution(ctx: "Context") -> None:
    ctx.ict.scenario = "Low"
    low = gen_ict_demand(ctx)
    ctx.ict.scenario = "High"
    high = gen_ict_demand(ctx)
    return low.add(high).div(2)


def build_weighted_distribution(ctx: "Context") -> pd.DataFrame:
    iea_2035 = read_2035_world_projections().loc[
        "Electricity consumption (TWh)", "Total", "2035"
    ]
    iea_index = (iea_2035.loc["Base"] - iea_2035.loc["Headwinds"]) / (
        iea_2035.loc["Lift-Off"] - iea_2035.loc["Headwinds"]
    )
    iea_index = min(1.0, max(0.0, iea_index.iloc[0]))
    ctx.ict.scenario = "Low"
    low = gen_ict_demand(ctx)
    ctx.ict.scenario = "High"
    high = gen_ict_demand(ctx)
    return low.add(high.sub(low).mul(iea_index))


def get_data_center_tec_code(s: "Spec", name: str) -> str:
    codes = s.add.set["technology"]
    return [c for c in codes if c.name.localizations["en"] == name][0].id


def project_demand(
    dig_trans: pd.DataFrame, years: list[int], pars: tuple, anchor: pd.DataFrame, scen
) -> pd.DataFrame:
    d_ene = pd.DataFrame()
    weu_share = get_weu_eeu_share()
    for yr0, yr1 in zip(years[:-1], years[1:]):
        d_ene[yr1] = compute_delta_ene(dig_trans.loc[yr0], dig_trans.loc[yr1], *pars)
        if scen == "Low":
            d_ene.loc["Europe", yr1] = (
                d_ene.loc["Western Europe", yr1] * weu_share
                + (1 - weu_share) * d_ene.loc["Central and Eastern Europe", yr1]
            )
    ene_proj = (
        d_ene.cumprod(axis=1)
        .mul(anchor, axis=0)
        .melt(ignore_index=False, var_name="Year", value_name="Value")
        .dropna()
        .set_index("Year", append=True)
    )
    if scen == "High":
        ene_proj = pd.concat(
            [
                ene_proj,
                (ene_proj.loc["Middle East"] + ene_proj.loc["Africa"])
                .assign(Region="Middle East and Africa")
                .set_index("Region", append=True)
                .swaplevel(0, 1),
            ]
        )
    return ene_proj


def gen_disaggregation_shares(pre_2030: pd.DataFrame) -> pd.DataFrame:
    dc_count = read_data_centre_count_shares("R12", True)
    chn_shr = (
        pre_2030.loc["China"]
        .div(pre_2030.loc["China"].add(pre_2030.loc["Asia Pacific ex China"]))
        .sort_index()
        .iloc[-1, 0]
    )
    apac = "Asia Pacific"
    chn = "R12_CHN"
    dc_count.loc[
        (dc_count.index.get_level_values(0) != chn)
        & (dc_count.index.get_level_values(1) == apac)
    ] = (
        dc_count.loc[dc_count.index.get_level_values(0) != chn]
        .loc[:, [apac], :]
        .div(dc_count.loc[:, [apac], :].sum().sub(dc_count.loc[chn]))
    ).mul(1 - chn_shr)
    dc_count.loc[dc_count.index.get_level_values(0) == chn] = chn_shr
    return dc_count


def get_pre2030_data() -> pd.DataFrame:
    # read and merge IEA and historical data from Fan et al. 2026
    iea = read_iea_data()
    hist_yrs = iea.index.get_level_values("Year").unique()
    fan_et_al = read_hist_data()
    fan_et_al = (
        fan_et_al[~fan_et_al["Year"].isin(hist_yrs)]
        .set_index(["Region", "Year"])
        .rename(columns={"Overall (TWh)": "Value"})
    )
    pre_2030 = pd.concat([fan_et_al, iea])
    return pre_2030


def get_long_term_elasticity() -> tuple[float, float]:
    ene = get_pre2030_data()
    eu_aggr = (
        ene.loc["Western Europe"]
        .add(ene.loc["Central and Eastern Europe"])
        .assign(Region="Europe")
        .set_index("Region", append=True)
        .swaplevel(0, 1)
    )
    ene = pd.concat([ene, eu_aggr]).sort_index()
    ene = (
        ene.swaplevel(0, 1)
        .loc[[2010, 2030]]
        .reset_index()
        .pivot(columns="Year", index="Region", values="Value")
        .dropna()
    )
    # experiment with outdated IEA Latin America value
    # ene.loc["Latin America", 2030] = 3.3
    y = ene.apply(lambda x: np.log(x[2030] / x[2010]), axis=1)

    ict_scen = "Low"
    full = False
    df = pd.concat(
        [
            read_country_data("SSP2", full=full),
            read_country_data("Historical", full=full).query("Year == 2010"),
        ]
    )
    ignored = get_ignored_countries()
    cmap = {k: v for k, v in get_cisco_region_mapping().items() if k not in ignored}
    cmap = {
        k: v.replace("Central and Eastern ", "").replace("Western ", "")
        for k, v in cmap.items()
    }
    pycountry.COUNTRY_NAME.update(missing)
    cmap = {pycountry.iso_3166_alpha_3(k): v for k, v in cmap.items()}
    if ict_scen == "High":
        cmap = {
            k: v.replace("Latin America", "Central and South America")
            for k, v in get_weo_iso_map().items()
            if k in cmap.keys()
        }
        ene.rename({"Latin America": "Central and South America"}, inplace=True)
    dig_trans = aggregate_to_region(df, cmap).swaplevel(0, 1)
    x = (
        dig_trans.loc[[2010, 2030]]
        .reset_index()
        .pivot(columns="Year", index="Region", values=0)
        .dropna()
    ).apply(lambda x: np.log(x[2030] / x[2010]), axis=1)
    b, c, eps = run_regression(x, y)
    return b, c / 4
