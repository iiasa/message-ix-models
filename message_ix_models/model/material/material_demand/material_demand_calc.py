import numpy as np
import pandas as pd
import yaml
from message_ix import make_df
from scipy.optimize import curve_fit

import message_ix_models.util
from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_infrastructure import get_inf_mat_demand
from message_ix_models.util import package_data_path

CASE_SENS = "mean"
INFRA_SCEN = "baseline"

print("Adding infrastructure demand with:")
print(CASE_SENS)
print(INFRA_SCEN)

file_cement = "/CEMENT.BvR2010.xlsx"
file_steel = "/STEEL_database_2012.xlsx"
file_al = "/demand_aluminum.xlsx"
file_gdp = "/iamc_db ENGAGE baseline GDP PPP.xlsx"
giga = 10**9
mega = 10**6

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
    "SSP5": "high",
    "LED": "low",
}

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
        "cement": {"a": 1.3},
        "aluminum": {"a": 1.3, "b": 1},
    },
}


def steel_function(x, a, b, m):
    gdp_pcap, del_t = x
    return a * np.exp(b / gdp_pcap) * (1 - m) ** del_t


def cement_function(x, a, b):
    gdp_pcap = x[0]
    return a * np.exp(b / gdp_pcap)


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


def gompertz(phi, mu, y, baseyear=2020):
    return 1 - np.exp(-phi * np.exp(-mu * (y - baseyear)))


def read_timer_pop(datapath, material):
    df_population = pd.read_excel(
        f'{datapath}/{material_data[material]["dir"]}{material_data[material]["file"]}',
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


def read_timer_gdp(datapath, material):
    # Read GDP per capita data
    df_gdp = pd.read_excel(
        f'{datapath}/{material_data[material]["dir"]}{material_data[material]["file"]}',
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


def project_demand(df, phi, mu):
    df_demand = df.groupby("region", group_keys=False).apply(
        lambda group: group.assign(
            demand_pcap_base=group["demand.tot.base"].iloc[0]
            * giga
            / group["pop.mil"].iloc[0]
            / mega
        )
    )
    df_demand = df_demand.groupby("region", group_keys=False).apply(
        lambda group: group.assign(
            gap_base=group["demand_pcap_base"].iloc[0] - group["demand_pcap0"].iloc[0]
        )
    )
    df_demand = df_demand.groupby("region", group_keys=False).apply(
        lambda group: group.assign(
            demand_pcap=group["demand_pcap0"]
            + group["gap_base"] * gompertz(phi, mu, y=group["year"])
        )
    )
    df_demand = (
        df_demand.groupby("region", group_keys=False)
        .apply(
            lambda group: group.assign(
                demand_tot=group["demand_pcap"] * group["pop.mil"] * mega / giga
            )
        )
        .reset_index(drop=True)
    )
    return df_demand[["region", "year", "demand_tot"]]


def read_base_demand(filepath):
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


def read_hist_mat_demand(material):
    datapath = message_ix_models.util.package_data_path("material")

    if material in ["cement", "steel"]:
        # Read population data
        df_pop = read_timer_pop(datapath, material)

        # Read GDP per capita data
        df_gdp = read_timer_gdp(datapath, material)

    if material == "aluminum":
        df_raw_cons = (
            pd.read_excel(
                f'{datapath}/{material_data[material]["dir"]}{material_data[material]["file"]}',
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
        print(
            "non-available material selected. must be one of [aluminum, steel, cement]"
        )
        df_cons = None
    return df_cons


def read_pop_from_scen(scen):
    pop = scen.par("bound_activity_up", {"technology": "Population"})
    pop = pop.loc[pop.year_act >= 2020].rename(
        columns={"year_act": "year", "value": "pop.mil", "node_loc": "region"}
    )
    pop = pop.drop(["mode", "time", "technology", "unit"], axis=1)
    return pop


def read_gdp_ppp_from_scen(scen):
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


def derive_demand(material, scen, old_gdp=False, ssp="SSP2"):
    datapath = message_ix_models.util.package_data_path("material")

    # read pop projection from scenario
    df_pop = read_pop_from_scen(scen)

    # read gdp (PPP) projection from scenario
    df_gdp = read_gdp_ppp_from_scen(scen)
    if old_gdp:
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

    # get base year demand of material
    df_base_demand_original = read_base_demand(
        f"{datapath}/{material_data[material]['dir']}/demand_{material}.yaml"
    )

    # In 2020 deduct the demand from infrastructure

    INPUTFILE = package_data_path(
        "material", "infrastructure", "stocks_forecast_MESSAGE.csv"
    )

    if material == "cement":
        infrastructure_demand = get_inf_mat_demand(
            "concrete",
            "2020",
            infra_scenario=INFRA_SCEN,
            case=CASE_SENS,
        )
        infrastructure_demand["value"] *= 0.15
    else:
        infrastructure_demand = get_inf_mat_demand(
            material,
            "2020",
            infra_scenario=INFRA_SCEN,
            case=CASE_SENS,
        )

    infrastructure_demand["year"] = infrastructure_demand["year"].astype("int64")

    infrastructure_demand.rename(columns={"node": "region"}, inplace=True)
    infrastructure_demand.drop(["commodity"], axis=1, inplace=True)

    # Merge DataFrames on 'node', 'year', and 'commodity'
    merged_df = pd.merge(
        df_base_demand_original,
        infrastructure_demand,
        on=["region", "year"],
        suffixes=("_df1", "_df2"),
    )

    # Subtract the 'value' columns
    merged_df["value"] = merged_df["value_df1"] - merged_df["value_df2"]

    # Select relevant columns to maintain the original format
    df_base_demand = merged_df[["region", "year", "value"]]

    # get historical data (material consumption, pop, gdp)
    df_cons = read_hist_mat_demand(material)
    x_data = tuple(pd.Series(df_cons[col]) for col in fitting_dict[material]["x_data"])

    # run regression on historical data
    params_opt = curve_fit(
        fitting_dict[material]["function"],
        xdata=x_data,
        ydata=df_cons["cons_pcap"],
        p0=fitting_dict[material]["initial_guess"],
    )[0]
    mode = ssp_mode_map[ssp]
    print(f"adjust regression parameters according to mode: {mode}")
    print(f"before adjustment: {params_opt}")
    for idx, multiplier in enumerate(mode_modifiers_dict[mode][material].values()):
        params_opt[idx] *= multiplier
    print(f"after adjustment: {params_opt}")

    # prepare df for applying regression model and project demand
    df_all = pd.merge(df_pop, df_base_demand.drop(columns=["year"]), how="left")
    df_all = pd.merge(df_all, df_gdp[["region", "year", "gdp_ppp"]], how="inner")
    df_all["del_t"] = df_all["year"] - 2010
    df_all["gdp_pcap"] = df_all["gdp_ppp"] * giga / df_all["pop.mil"] / mega
    df_all["demand_pcap0"] = df_all.apply(
        lambda row: fitting_dict[material]["function"](
            tuple(row[i] for i in fitting_dict[material]["x_data"]), *params_opt
        ),
        axis=1,
    )
    df_all = df_all.rename({"value": "demand.tot.base"}, axis=1)

    # correct base year difference with convergence function
    df_final = project_demand(
        df_all, fitting_dict[material]["phi"], fitting_dict[material]["mu"]
    )

    # Make sure 2020 demand is kept as in the original data file
    # Merge df_final (filtered for year 2020) with df_other on 'region' and 'year'
    merged_df = pd.merge(
        df_final[df_final["year"] == 2020],
        df_base_demand_original,
        on=["region", "year"],
    )

    # Update df_final's demand_tot for year 2020 with the merged values
    df_final.loc[df_final["year"] == 2020, "demand_tot"] = (
        df_final.loc[df_final["year"] == 2020]
        .set_index(["region", "year"])
        .index.map(merged_df.set_index(["region", "year"])["value"])
    )

    # format to MESSAGEix standard
    df_final = df_final.rename({"region": "node", "demand_tot": "value"}, axis=1)
    df_final["commodity"] = material
    df_final["level"] = "demand"
    df_final["time"] = "year"
    df_final["unit"] = "t"

    # TODO: correct unit would be Mt but might not be registered on database
    df_final = make_df("demand", **df_final)

    return df_final


def gen_demand_petro(scenario, chemical, gdp_elasticity_2020, gdp_elasticity_2030):
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
    else:
        gdp_mer = scenario.par("bound_activity_up", {"technology": "GDP"})
        mer_to_ppp = pd.read_csv(
            package_data_path("material", "other", "mer_to_ppp_default.csv")
        ).set_index(["node", "year"])
        # mer_to_ppp = scenario.par("MERtoPPP").set_index("node", "year")
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

    level = "demand" if chemical == "HVC" else "final_material"

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
