import numpy as np
import pandas as pd
import pint
from message_ix import make_df

from message_ix_models import Context, ScenarioInfo, Spec
from message_ix_models.model.material.util import get_ssp_from_context
from message_ix_models.tools.ict.data_center import (
    get_cisco_region_mapping,
    get_dc_numbers,
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
from message_ix_models.types import ParameterData
from message_ix_models.util import merge_data, pycountry


def gen_data(ctx, sinfo: "ScenarioInfo", s: Spec) -> "ParameterData":
    config = {
        "Low": {"func": gen_ict_demand},
        "High": {"func": gen_ict_demand},
        "Medium": {"func": build_avg_distribution},
        "Central": {"func": build_weighted_distribution},
    }
    final = config[ctx.model.ict_scenario]["func"](ctx)
    par_data = format_to_parameter(
        final, get_data_center_tec_code(s, "Data centers"), sinfo
    )
    merge_data(
        par_data,
        gen_infra_demand(
            par_data["fixed_activity"],
            ctx.model.ict_scenario,
            get_data_center_tec_code(s, "Telecommunication infrastructure"),
        ),
    )
    merge_data(par_data, gen_input(par_data["fixed_activity"]))
    return par_data


def gen_ict_demand(ctx) -> pd.DataFrame:
    regression_cfg = {
        "Low": {"Sensitivity": "Efficiency Dominant", "Period": "2012-2017"},
        "High": {"Sensitivity": "Service Demand Dominant", "Period": "2020-2025"},
    }
    ict_scen = ctx.model.ict_scenario
    # - Read fixed trajectory until 2030 from IEA
    pre_2030 = get_pre2030_data()

    # - Read regression parameters from Fan et al. 2026 or compute from raw data
    pars = read_regr_pars()
    p2030 = pars.loc[
        regression_cfg[ict_scen]["Sensitivity"], regression_cfg[ict_scen]["Period"]
    ]

    # - Read dig trans data from Fan et al. 2025
    df = read_country_data(get_ssp_from_context(ctx), full=True)
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

    # - Extrapolate beyond 2050 if needed
    # - Disaggregate to MESSAGEix regions (currently only R12 supported)
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
    # - Concatenate IEA and projected data
    all = pd.concat([pre_2030, ene_proj, ene_post2050])
    if ict_scen == "High":
        all.rename({"Central and South America": "Latin America"}, inplace=True)
    # TODO: make disaggregation read node config
    dc_count = gen_disaggregation_shares(pre_2030)
    final = dc_count.mul(all).droplevel(1).dropna()
    return final


def build_avg_distribution(ctx: "Context") -> None:
    ctx.model.ict_scenario = "Low"
    low = gen_ict_demand(ctx)
    ctx.model.ict_scenario = "High"
    high = gen_ict_demand(ctx)
    return low.add(high).div(2)


def build_weighted_distribution(ctx: "Context") -> pd.DataFrame:
    low, high, central = read_2035_world_projections()
    iea_index = (central - low) / (high - low)
    iea_index = min(1.0, max(0.0, iea_index))
    ctx.model.ict_scenario = "Low"
    low = gen_ict_demand(ctx)
    ctx.model.ict_scenario = "High"
    high = gen_ict_demand(ctx)
    return low.add(high.sub(low).mul(iea_index))


def gen_infra_demand(df, dc_scen, tec_code) -> pd.DataFrame:
    def tc_ratio(tc_scen: str) -> float:
        ratios = {"High": 0.91, "Low": 0.78, "Medium": 0.78}
        return ratios[tc_scen]

    tc2020 = df[df["year_act"] == 2020].assign(value=lambda x: x["value"] * 0.91)
    tc_post_2020 = df[df["year_act"] != 2020].assign(
        value=lambda x: x["value"] * tc_ratio(dc_scen)
    )
    tc = pd.concat([tc2020, tc_post_2020]).assign(technology=tec_code)
    return {"fixed_activity": tc.round(5)}


def get_data_center_tec_code(s, name) -> str:
    codes = s.add.set["technology"]
    return [c for c in codes if c.name.localizations["en"] == name][0].id


def project_demand(dig_trans: pd.DataFrame, years, pars, anchor, scen) -> pd.DataFrame:
    d_ene = pd.DataFrame()
    weu_share = get_weu_eeu_share()
    for yr0, yr1 in zip(years[:-1], years[1:]):
        d_ene[yr1] = compute_delta_ene(dig_trans.loc[yr0], dig_trans.loc[yr1], *pars)
        if scen == "Low":
            d_ene.loc["Europe", yr1] = (
                d_ene.loc["Western Europe", yr1] * weu_share
                + (1 - weu_share) * d_ene.loc["Central and Eastern Europe", yr1]
            )["Value"]
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


def format_to_parameter(df: pd.DataFrame, tec, sinfo) -> "ParameterData":
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


def compute_delta_ene(dig0: float, dig1: float, beta: float, eps: float):
    return np.exp(np.log(dig1 / dig0) * beta + eps)


def get_weu_eeu_share() -> float:
    dc_count = get_dc_numbers()

    # drop countries that lack data quality according to Fan et al. 2026
    ignored_isos = [
        pycountry.iso_3166_alpha_3(i)
        for i in get_ignored_countries()
        if isinstance(i, str)
    ]
    dc_count = dc_count[~dc_count["ISO"].isin(ignored_isos)]

    # add column with assigned region (Cisco mapping)
    dc_count["Region"] = dc_count["Country"].map(get_cisco_region_mapping())
    dc_count_reg = dc_count.groupby("Region").sum(numeric_only=True)
    return dc_count_reg.loc["Western Europe"].div(
        dc_count_reg.loc["Western Europe"].add(
            dc_count_reg.loc["Central and Eastern Europe"]
        )
    )


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


def gen_input(df) -> ParameterData:
    dims = dict(time_origin="year", commodity="electr", level="final")
    inp = (
        make_df("input", **df, **dims)
        .assign(node_origin=lambda x: x["node_loc"], value=1.0)
        .round(5)
    )
    return {"input": inp}


def run_regression(ln_dig, ln_ene) -> tuple[float, float, float]:
    from scipy import stats

    beta, global_constant, r, p, se = stats.linregress(ln_dig, ln_ene)
    resid = ln_ene.sub(beta * ln_dig + global_constant, axis=0)
    return beta, global_constant, resid


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
