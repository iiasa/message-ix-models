import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Literal, Union

import numpy as np
import pandas as pd
import scipy.optimize as opt
import yaml
from message_ix import make_df
from scipy.optimize import curve_fit

import message_ix_models.util
from message_ix_models import Context, ScenarioInfo
from message_ix_models.model.material.data_util import get_ssp_soc_eco_data
from message_ix_models.model.material.demand.math import (
    GIGA,
    MEGA,
    cement_function,
    gompertz,
    steel_function,
)
from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from message_ix import Scenario

file_gdp = "/iamc_db ENGAGE baseline GDP PPP.xlsx"
log = logging.getLogger(__name__)

mode_modifiers_dict = {
    "low": {
        "steel": {"a": 0.85, "b": 0.95},
        "cement": {"a": 0.8},
        "aluminum": {"a": 0.85, "b": 0.95},
    },
    "normal": {
        "steel": {"a": 1, "b": 1},
        "cement": {"a": 1},
        "aluminum": {"a": 1, "b": 1},
    },
    "high": {
        "steel": {"a": 1.3, "b": 1},
        "cement": {"a": 1.2, "b": 1.5},
        "aluminum": {"a": 1.4, "b": 1.3},
    },
    "highest": {
        "steel": {"a": 1.3, "b": 1},
        "cement": {"a": 1.8, "b": 4},
        "aluminum": {"a": 1.3, "b": 1},
    },
}

material_data = {
    "aluminum": {"dir": "aluminum", "file": "/demand_aluminum.xlsx"},
    "steel": {"dir": "steel", "file": "/STEEL_database_2012.xlsx"},
    "cement": {"dir": "cement", "file": "/CEMENT.BvR2010.xlsx"},
    "HVC": {"dir": "petrochemicals"},
    "NH3": {"dir": "ammonia"},
    "methanol": {"dir": "methanol"},
}

ssp_mode_map = {
    "SSP1": "low",
    "SSP2": "normal",
    "SSP3": "high",
    "SSP4": "normal",
    "SSP5": "highest",
    "LED": "low",
}

fitting_dict = {
    "steel": {
        "function": steel_function,
        "initial_guess": [600, -10000, 0],
        "x_data": ["gdp_pcap", "del_t"],
        "phi": 9,
        "mu": 0.1,
    },
    "cement": {
        "function": cement_function,
        "initial_guess": [500, -3000],
        "x_data": ["gdp_pcap"],
        "phi": 10,
        "mu": 0.1,
    },
    "aluminum": {
        "function": cement_function,
        "initial_guess": [600, -10000],
        "x_data": ["gdp_pcap"],
        "phi": 9,
        "mu": 0.1,
    },
}


def read_timer_pop(
    datapath: Union[str, Path], material: Literal["cement", "steel", "aluminum"]
):
    """Read population data for a given material from TIMER model.

    Parameters
    ----------
    datapath
    material

    Returns
    -------
    pd.DataFrame
        DataFrame containing population data with columns: [region, reg_no, year, pop]
    """
    df_population = pd.read_excel(
        f"{datapath}/{material_data[material]['dir']}{material_data[material]['file']}",
        sheet_name="Timer_POP",
        skiprows=[0, 1, 2, 30],
        nrows=26,
    )
    df_population = df_population.drop(columns=["Unnamed: 0"]).rename(
        {"Unnamed: 1": "reg_no", "Unnamed: 2": "region"}, axis=1
    )
    df_population = df_population.melt(
        id_vars=["region", "reg_no"], var_name="year", value_name="pop"
    )
    return df_population


def read_timer_gdp(
    datapath: Union[str, Path], material: Literal["cement", "steel", "aluminum"]
):
    """Read GDP per capita data for a given material from TIMER Excel files.

    Parameters
    ----------
    datapath
        Path to the directory containing the TIMER data files.
    material
        The material type for which GDP per capita data is being read.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing GDP per capita data with the following columns:
        - region: Region name.
        - reg_no: Region number.
        - year: Year of the data.
        - gdp_pcap: GDP per capita value.
    """
    # Read GDP per capita data
    df_gdp = pd.read_excel(
        f"{datapath}/{material_data[material]['dir']}{material_data[material]['file']}",
        sheet_name="Timer_GDPCAP",
        skiprows=[0, 1, 2, 30],
        nrows=26,
    )
    df_gdp = df_gdp.drop(columns=["Unnamed: 0"]).rename(
        {"Unnamed: 1": "reg_no", "Unnamed: 2": "region"}, axis=1
    )
    df_gdp = df_gdp.melt(
        id_vars=["region", "reg_no"], var_name="year", value_name="gdp_pcap"
    )
    return df_gdp


def project_demand(df: pd.DataFrame, phi: float, mu: float):
    """Project material demand over time using a convergence function.

    This function calculates the projected demand for materials by applying a Gompertz
    convergence function to historical demand data. It adjusts demand per capita and
    total demand based on population and other factors.

    Parameters
    ----------
    df
        Input DataFrame containing historical demand data with the following columns:
        - demand.tot.base: Base year total demand.
        - pop.mil: Population in millions.
        - demand_pcap0: Initial demand per capita.
        - year: Year of the data.
    phi
        Shape parameter for the Gompertz function.
    mu
        Rate parameter for the Gompertz function.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing projected demand data with the following columns:
        - region: Region name.
        - year: Year of the data.
        - demand_tot: Projected total demand.
    """
    df_demand = df.groupby("region", group_keys=False)[df.columns].apply(
        lambda group: group.assign(
            demand_pcap_base=group["demand.tot.base"].iloc[0]
            * GIGA
            / group["pop.mil"].iloc[0]
            / MEGA
        )
    )
    df_demand = df_demand.groupby("region", group_keys=False)[df_demand.columns].apply(
        lambda group: group.assign(
            gap_base=group["demand_pcap_base"].iloc[0] - group["demand_pcap0"].iloc[0]
        )
    )
    df_demand = df_demand.groupby("region", group_keys=False)[df_demand.columns].apply(
        lambda group: group.assign(
            demand_pcap=group["demand_pcap0"]
            + group["gap_base"] * gompertz(phi, mu, y=group["year"])
        )
    )
    df_demand = (
        df_demand.groupby("region", group_keys=False)[df_demand.columns]
        .apply(
            lambda group: group.assign(
                demand_tot=group["demand_pcap"] * group["pop.mil"] * MEGA / GIGA
            )
        )
        .reset_index(drop=True)
    )
    return df_demand[["region", "year", "demand_tot"]]


def read_base_demand(filepath: Union[str, Path]):
    """Read base year demand data from a YAML file.

    Parameters
    ----------
    filepath
        Path to the YAML file containing base demand data.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: region, year, value.
    """
    with open(filepath, "r") as file:
        yaml_data = file.read()

    data_list = yaml.safe_load(yaml_data)

    df = pd.DataFrame(
        [
            (key, value["year"], value["value"])
            for entry in data_list
            for key, value in entry.items()
        ],
        columns=["region", "year", "value"],
    )
    return df


def read_hist_mat_demand(material: Literal["cement", "steel", "aluminum"]):
    """Read historical material demand data for a specified commodity.

    This function retrieves historical data on material consumption, population, and GDP
    for the specified material. It processes the data from various sources and formats
    it into a consistent structure.

    Parameters
    ----------
    material
        Commodity for which historical demand data is being read.

    Returns
    -------
    pd.DataFrame
        DataFrame containing historical material demand data with the following columns:
        - region: Region name.
        - reg_no: Region number.
        - year: Year of the data.
        - consumption: Material consumption.
        - pop: Population data.
        - gdp_pcap: GDP per capita.
        - cons_pcap: Consumption per capita.
        - del_t: Time difference from the base year (2010).
    """
    datapath = message_ix_models.util.package_data_path("material")

    if material in ["cement", "steel"]:
        # Read population data
        df_pop = read_timer_pop(datapath, material)

        # Read GDP per capita data
        df_gdp = read_timer_gdp(datapath, material)

    if material == "aluminum":
        df_raw_cons = (
            pd.read_excel(
                f"{datapath}/{material_data[material]['dir']}{material_data[material]['file']}",
                sheet_name="final_table",
                nrows=378,
            )
            .drop(["cons.pcap", "del.t"], axis=1)
            .rename({"gdp.pcap": "gdp_pcap"}, axis=1)
        )

        df_cons = (
            df_raw_cons.assign(
                cons_pcap=lambda x: x["consumption"] / x["pop"],
                del_t=lambda x: x["year"].astype(int) - 2010,
            )
            .dropna()
            .query("cons_pcap > 0")
        )
    elif material == "steel":
        df_raw_cons = pd.read_excel(
            f"{datapath}/{material_data[material]['dir']}{material_data[material]['file']}",
            sheet_name="Consumption regions",
            nrows=26,
        )
        df_raw_cons = (
            df_raw_cons.rename({"t": "region"}, axis=1)
            .drop(columns=["Unnamed: 1"])
            .rename({"TIMER Region": "reg_no"}, axis=1)
        )
        df_raw_cons = df_raw_cons.melt(
            id_vars=["region", "reg_no"], var_name="year", value_name="consumption"
        )
        df_raw_cons["region"] = (
            df_raw_cons["region"]
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace(r"\d+", "")
            .str[:-1]
        )

        # Merge and organize data
        df_cons = (
            pd.merge(df_raw_cons, df_pop.drop("region", axis=1), on=["reg_no", "year"])
            .merge(df_gdp[["reg_no", "year", "gdp_pcap"]], on=["reg_no", "year"])
            .assign(
                cons_pcap=lambda x: x["consumption"] / x["pop"],
                del_t=lambda x: x["year"].astype(int) - 2010,
            )
            .dropna()
            .query("cons_pcap > 0")
        )
    elif material == "cement":
        df_raw_cons = pd.read_excel(
            f"{datapath}/{material_data[material]['dir']}{material_data[material]['file']}",
            sheet_name="Regions",
            skiprows=122,
            nrows=26,
        )
        df_raw_cons = df_raw_cons.rename(
            {"Region #": "reg_no", "Region": "region"}, axis=1
        )
        df_raw_cons = df_raw_cons.melt(
            id_vars=["region", "reg_no"], var_name="year", value_name="consumption"
        )
        # Merge  and organize data
        df_cons = (
            pd.merge(df_raw_cons, df_pop.drop("region", axis=1), on=["reg_no", "year"])
            .merge(df_gdp[["reg_no", "year", "gdp_pcap"]], on=["reg_no", "year"])
            .assign(
                cons_pcap=lambda x: x["consumption"] / x["pop"] / 10**6,
                del_t=lambda x: x["year"].astype(int) - 2010,
            )
            .dropna()
            .query("cons_pcap > 0")
        )
    else:
        log.error(
            "non-available material selected. must be one of [aluminum, steel, cement]"
        )
        df_cons = None
    return df_cons


def read_pop_from_scen(scen: "Scenario") -> pd.DataFrame:
    """Extract population data from a MESSAGEix scenario.

    Filters the data to include only years from 2020 onwards and renames columns.

    Parameters
    ----------
    scen : "Scenario"
        The MESSAGEix scenario containing population data.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing population data with the following columns:
        - region: Region name.
        - year: Year of the data.
        - pop.mil: Population in millions.
    """
    pop = scen.par("bound_activity_up", {"technology": "Population"})
    pop = pop.loc[pop.year_act >= 2020].rename(
        columns={"year_act": "year", "value": "pop.mil", "node_loc": "region"}
    )
    pop = pop.drop(["mode", "time", "technology", "unit"], axis=1)
    return pop


def read_gdp_ppp_from_scen(scen: "Scenario") -> pd.DataFrame:
    """Extract GDP (PPP) data from a MESSAGEix scenario.

    If GDP (PPP) data is not directly available, it calculates it using GDP (MER)
    and a MER-to-PPP conversion factor.

    Parameters
    ----------
    scen
        The MESSAGEix scenario containing GDP data.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing GDP (PPP) data with the following columns:
        - year: Year of the data.
        - region: Region name.
        - gdp_ppp: GDP (PPP) value.
    """
    if len(scen.par("bound_activity_up", filters={"technology": "GDP_PPP"})):
        gdp = scen.par("bound_activity_up", {"technology": "GDP_PPP"})
        gdp = gdp.rename({"value": "gdp_ppp", "year_act": "year"}, axis=1)[
            ["year", "node_loc", "gdp_ppp"]
        ]
    else:
        gdp_mer = scen.par("bound_activity_up", {"technology": "GDP"})
        mer_to_ppp = scen.par("MERtoPPP")
        if not len(mer_to_ppp):
            mer_to_ppp = pd.read_csv(
                message_ix_models.util.package_data_path(
                    "material", "other", "mer_to_ppp_default.csv"
                )
            )
        mer_to_ppp = mer_to_ppp.set_index(["node", "year"])
        gdp_mer = gdp_mer.merge(
            mer_to_ppp.reset_index()[["node", "year", "value"]],
            left_on=["node_loc", "year_act"],
            right_on=["node", "year"],
        )
        gdp_mer["gdp_ppp"] = gdp_mer["value_y"] * gdp_mer["value_x"]
        gdp = gdp_mer[["year", "node_loc", "gdp_ppp"]]
    gdp = gdp.loc[gdp.year >= 2020].rename(columns={"node_loc": "region"})
    return gdp


def read_socio_economic_projection(
    scen: "Scenario",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read socio-economic projections (population and GDP)"""
    datapath = message_ix_models.util.package_data_path("material")

    # read pop projection from scenario
    df_pop = read_pop_from_scen(scen)
    if df_pop.empty:
        log.info(
            "Scenario does not provide Population projections. Reading default "
            "timeseries instead"
        )
        ctx = Context.get_instance(-1)
        ctx.update(regions="R12")
        df_pop = (
            get_ssp_soc_eco_data(ctx, "IIASA", "POP", "Population")
            .rename(
                columns={"year_act": "year", "value": "pop.mil", "node_loc": "region"}
            )
            .drop(["mode", "time", "technology", "unit"], axis=1)
        )

    # read gdp (PPP) projection from scenario
    df_gdp = read_gdp_ppp_from_scen(scen)
    # if not retrievable, read default from exogenous file instead
    if df_gdp.empty:
        log.info(
            "Scenario does not provide GDP projections. Reading default"
            "timeseries instead"
        )
        df_gdp = pd.read_excel(f"{datapath}/other{file_gdp}", sheet_name="data_R12")
        df_gdp = (
            df_gdp[df_gdp["Scenario"] == "baseline"]
            .loc[:, ["Region", *[i for i in df_gdp.columns if isinstance(i, int)]]]
            .melt(id_vars="Region", var_name="year", value_name="gdp_ppp")
            .query('Region != "World"')
            .assign(
                year=lambda x: x["year"].astype(int),
                region=lambda x: "R12_" + x["Region"],
            )
        )
    return df_gdp, df_pop


def format_to_demand_par(df: pd.DataFrame, material: str):
    """Format given dataframe to MESSAGEix ``demand`` parameter.

    Parameters
    ----------
    df
        DataFrame containing columns: [region, year, demand_tot]
    material
        commodity code
    """
    df = df.rename({"region": "node", "demand_tot": "value"}, axis=1).assign(
        commodity=material, level="demand", time="year", unit="t"
    )
    # TODO: correct unit would be Mt but might not be registered on database
    df = make_df("demand", **df)
    return df


def prepare_model_input(
    df_pop: pd.DataFrame, df_gdp: pd.DataFrame, df_base_demand: pd.DataFrame
) -> pd.DataFrame:
    """Prepare input dataframe for applying regression model."""
    df_all = pd.merge(df_pop, df_base_demand.drop(columns=["year"]), how="left")
    df_all = pd.merge(df_all, df_gdp[["region", "year", "gdp_ppp"]], how="inner")
    df_all["del_t"] = df_all["year"] - 2010
    df_all["gdp_pcap"] = df_all["gdp_ppp"] * GIGA / df_all["pop.mil"] / MEGA
    df_all = df_all.rename(columns={"value": "demand.tot.base"})
    return df_all


def derive_demand(
    material: Literal["cement", "steel", "aluminum"],
    scen: "Scenario",
    ssp: Union[Literal["SSP1", "SSP2", "SSP3", "SSP4", "SSP5", "LED"], str] = "SSP2",
    new: bool = False,
    model: Literal["least-squares", "quantile"] = "least-squares",
):
    datapath = message_ix_models.util.package_data_path("material")

    df_gdp, df_pop = read_socio_economic_projection(scen)

    # get base year demand of material
    df_base_demand = read_base_demand(
        f"{datapath}/{material_data[material]['dir']}/demand_{material}.yaml"
    )

    # get historical data (material consumption, pop, gdp)
    if new:
        df_cons = pd.read_csv(
            "/Users/florianmaczek/PycharmProjects/IEA-statistics-analysis/input_data/steel/steel_consumption.csv"
        )
    else:
        df_cons = read_hist_mat_demand(material)
    x_data = tuple(pd.Series(df_cons[col]) for col in fitting_dict[material]["x_data"])

    # run regression on historical data
    if model == "least-squares":
        params_opt = curve_fit(
            fitting_dict[material]["function"],
            xdata=x_data,
            ydata=df_cons["cons_pcap"],
            p0=fitting_dict[material]["initial_guess"],
        )[0]
        mode = ssp_mode_map[ssp]
        log.info(f"adjust regression parameters according to mode: {mode}")
        log.info(f"before adjustment: {params_opt}")
        for idx, multiplier in enumerate(mode_modifiers_dict[mode][material].values()):
            params_opt[idx] *= multiplier
        log.info(f"after adjustment: {params_opt}")
    if model == "quantile":
        params_opt = perform_quantile_reg(
            x_data,
            df_cons["cons_pcap"],
            fitting_dict["steel"]["function"],
            fitting_dict["steel"]["initial_guess"],
            0.5,
        )

    # prepare df for applying regression model and project demand
    df_all = prepare_model_input(df_pop, df_gdp, df_base_demand)
    df_all["demand_pcap0"] = df_all.apply(
        lambda row: fitting_dict[material]["function"](
            tuple(row[i] for i in fitting_dict[material]["x_data"]), *params_opt
        ),
        axis=1,
    )

    # correct base year difference with convergence function
    df_final = project_demand(
        df_all, fitting_dict[material]["phi"], fitting_dict[material]["mu"]
    )

    # format to MESSAGEix standard
    df_par = format_to_demand_par(df_final, material)
    return df_par


def gen_demand_petro(
    scenario: "Scenario",
    chemical: Literal["HVC", "methanol", "NH3"],
    gdp_elasticity_2020: float,
    gdp_elasticity_2030: float,
):
    """Generate petrochemical demand projections based on GDP elasticity.

    This function calculates the demand for petrochemicals (HVC, methanol, NH3) using
    distinct GDP elasticity values for 2020-2030 and post-2030. GDP projections are
    queried from scenario if available or read from external data file.

    Parameters
    ----------
    scenario
        The MESSAGEix scenario containing input data.
    chemical
        The petrochemical for which demand is being projected.
    gdp_elasticity_2020
        GDP elasticity value for the period up to 2020.
    gdp_elasticity_2030
        GDP elasticity value for the period after 2030.

    Returns
    -------
    pd.DataFrame
        ``demand`` parameter data
    """
    chemicals_implemented = ["HVC", "methanol", "NH3"]
    if chemical not in chemicals_implemented:
        raise ValueError(
            f"'{chemical}' not supported. Choose one of {chemicals_implemented}"
        )
    s_info = ScenarioInfo(scenario)
    modelyears = s_info.Y  # s_info.Y is only for modeling years
    fy = scenario.firstmodelyear

    def get_demand_t1_with_income_elasticity(
        demand_t0, income_t0, income_t1, elasticity
    ):
        """
        Calculate demand for the next year using income elasticity.

        Parameters
        ----------
        demand_t0 : pd.Series
            Demand in the current year.
        income_t0 : pd.Series
            Income in the current year.
        income_t1 : pd.Series
            Income in the next year.
        elasticity : float
            Income elasticity value.

        Returns
        -------
        pd.Series
            Projected demand for the next year.
        """
        return (
            elasticity * demand_t0.mul(((income_t1 - income_t0) / income_t0), axis=0)
        ) + demand_t0

    if "GDP_PPP" in list(scenario.set("technology")):
        gdp_ppp = scenario.par("bound_activity_up", {"technology": "GDP_PPP"})
        df_gdp_ts = gdp_ppp.pivot(
            index="node_loc", columns="year_act", values="value"
        ).reset_index()
        num_cols = [i for i in df_gdp_ts.columns if isinstance(i, int)]
        hist_yrs = [i for i in num_cols if i < fy]
        df_gdp_ts = (
            df_gdp_ts.drop([i for i in hist_yrs if i in df_gdp_ts.columns], axis=1)
            .set_index("node_loc")
            .sort_index()
        )
    elif "GDP" in list(scenario.set("technology")):
        gdp_mer = scenario.par("bound_activity_up", {"technology": "GDP"})
        mer_to_ppp = pd.read_csv(
            package_data_path("material", "other", "mer_to_ppp_default.csv")
        ).set_index(["node", "year"])
        # TODO: might need to be re-activated for different SSPs
        gdp_mer = gdp_mer.merge(
            mer_to_ppp.reset_index()[["node", "year", "value"]],
            left_on=["node_loc", "year_act"],
            right_on=["node", "year"],
        )
        gdp_mer["gdp_ppp"] = gdp_mer["value_y"] * gdp_mer["value_x"]
        gdp_mer = gdp_mer[["year", "node_loc", "gdp_ppp"]].reset_index()
        gdp_mer["Region"] = gdp_mer["node_loc"]  # .str.replace("R12_", "")
        df_gdp_ts = gdp_mer.pivot(
            index="Region", columns="year", values="gdp_ppp"
        ).reset_index()
        num_cols = [i for i in df_gdp_ts.columns if isinstance(i, int)]
        hist_yrs = [i for i in num_cols if i < fy]
        df_gdp_ts = (
            df_gdp_ts.drop([i for i in hist_yrs if i in df_gdp_ts.columns], axis=1)
            .set_index("Region")
            .sort_index()
        )
    else:
        df_gdp = pd.read_excel(
            f"{message_ix_models.util.package_data_path('material')}/other{file_gdp}",
            sheet_name="data_R12",
        )
        df_gdp_ts = (
            df_gdp[df_gdp["Scenario"] == "baseline"]
            .loc[:, ["Region", *[i for i in df_gdp.columns if isinstance(i, int)]]]
            .melt(id_vars="Region", var_name="year", value_name="gdp_ppp")
            .query('Region != "World"')
            .assign(
                year=lambda x: x["year"].astype(int),
                region=lambda x: "R12_" + x["Region"],
            )
            .pivot(index="region", columns="year", values="gdp_ppp")
        )
    df_demand_2020 = read_base_demand(
        package_data_path()
        / "material"
        / f"{material_data[chemical]['dir']}/demand_{chemical}.yaml"
    )
    df_demand_2020 = df_demand_2020.rename({"region": "Region"}, axis=1)
    df_demand = df_demand_2020.pivot(index="Region", columns="year", values="value")
    dem_next_yr = df_demand

    for i in range(len(modelyears) - 1):
        income_year1 = modelyears[i]
        income_year2 = modelyears[i + 1]

        if income_year2 >= 2030:
            dem_next_yr = get_demand_t1_with_income_elasticity(
                dem_next_yr,
                df_gdp_ts[income_year1],
                df_gdp_ts[income_year2],
                gdp_elasticity_2030,
            )
        else:
            dem_next_yr = get_demand_t1_with_income_elasticity(
                dem_next_yr,
                df_gdp_ts[income_year1],
                df_gdp_ts[income_year2],
                gdp_elasticity_2020,
            )
        df_demand[income_year2] = dem_next_yr

    df_melt = df_demand.melt(ignore_index=False).reset_index()

    level = "demand" if chemical in ["HVC", "methanol"] else "final_material"

    return make_df(
        "demand",
        unit="t",
        level=level,
        value=df_melt.value,
        time="year",
        commodity=chemical,
        year=df_melt.year,
        node=df_melt["Region"],
    )


def perform_quantile_reg(
    x: tuple[pd.Series],
    y: pd.Series,
    func: Callable,
    initial_params: List[int],
    tau: float = 0.5,
):
    # Define quantile loss function
    def quantile_loss(params, x, y, tau):
        y_pred = func(x, *params)
        residuals = y - y_pred
        return np.sum(
            (tau * np.maximum(residuals, 0)) + ((1 - tau) * np.maximum(-residuals, 0))
        )

    res = opt.minimize(
        quantile_loss, initial_params, args=(x, y, tau), method="Nelder-Mead"
    )

    # Extract optimized parameters
    a_opt, b_opt, c_opt = res.x

    # Plot results
    # plt.scatter(x, y, alpha=0.5, label="Data")
    # plt.plot(x, func(x, *res.x), "r-", label=f"Quantile {tau} regression")
    # plt.legend()
    # plt.xlabel("x")
    # plt.ylabel("y")
    # plt.title("Non-linear Quantile Regression")
    # plt.show()
    return a_opt, b_opt, c_opt
