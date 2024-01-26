import message_ix_models.util
import pandas as pd
import numpy as np
import message_ix
from scipy.optimize import curve_fit
import yaml

file_cement = "/CEMENT.BvR2010.xlsx"
file_steel = "/STEEL_database_2012.xlsx"
file_al = "/demand_aluminum.xlsx"
file_gdp = "/iamc_db ENGAGE baseline GDP PPP.xlsx"

giga = 10**9
mega = 10**6

material_data = {
    "aluminum": {"dir": "aluminum", "file": "/demand_aluminum.xlsx"},
    "steel": {"dir": "steel_cement", "file": "/STEEL_database_2012.xlsx"},
    "cement": {"dir": "steel_cement", "file": "/CEMENT.BvR2010.xlsx"},
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


def read_timer_pop(datapath, filename, material):
    df_population = pd.read_excel(
        f'{datapath}/{material_data[material]["dir"]}{material_data["cement"]["file"]}',
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


def read_timer_gdp(datapath, filename, material):
    # Read GDP per capita data
    df_gdp = pd.read_excel(
        f'{datapath}/{material_data[material]["dir"]}{material_data["cement"]["file"]}',
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
    datapath = message_ix_models.util.private_data_path("material")

    if material in ["cement", "steel"]:
        # Read population data
        df_pop = read_timer_pop(datapath, file_cement, material)

        # Read GDP per capita data
        df_gdp = read_timer_gdp(datapath, file_cement, material)

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
            f'{datapath}/{material_data[material]["dir"]}{material_data[material]["file"]}',
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
            .str.replace("\d+", "")
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
            f'{datapath}/{material_data[material]["dir"]}{material_data[material]["file"]}',
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
    gdp_mer = scen.par("bound_activity_up", {"technology": "GDP"})
    mer_to_ppp = scen.par("MERtoPPP")
    if not len(mer_to_ppp):
        mer_to_ppp = pd.read_csv(
            message_ix_models.util.private_data_path(
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
    gdp_mer = gdp_mer[["year", "node_loc", "gdp_ppp"]]
    gdp = gdp_mer.loc[gdp_mer.year >= 2020].rename(columns={"node_loc": "region"})
    return gdp


def derive_demand(material, scen, old_gdp=False):
    datapath = message_ix_models.util.private_data_path("material")

    # read pop projection from scenario
    df_pop = read_pop_from_scen(scen)

    # read gdp (PPP) projection from scenario
    df_gdp = read_gdp_ppp_from_scen(scen)
    if old_gdp:
        df_gdp = pd.read_excel(f"{datapath}/other{file_gdp}", sheet_name="data_R12")
        df_gdp = (
            df_gdp[df_gdp["Scenario"] == "baseline"]
            .loc[:, ["Region", *[i for i in df_gdp.columns if type(i) == int]]]
            .melt(id_vars="Region", var_name="year", value_name="gdp_ppp")
            .query('Region != "World"')
            .assign(
                year=lambda x: x["year"].astype(int),
                region=lambda x: "R12_" + x["Region"],
            )
        )

    # get base year demand of material
    df_base_demand = read_base_demand(
        f'{datapath}/{material_data[material]["dir"]}/demand_{material}.yaml'
    )

    # get historical data (material consumption, pop, gdp)
    df_cons = read_hist_mat_demand(material)
    x_data = tuple(pd.Series(df_cons[col]) for col in fitting_dict[material]["x_data"])

    # run regression on historical data
    params_opt, _ = curve_fit(
        fitting_dict[material]["function"],
        xdata=x_data,
        ydata=df_cons["cons_pcap"],
        p0=fitting_dict[material]["initial_guess"],
    )

    # prepare df for applying regression model to project demand
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

    # apply regression model
    df_final = project_demand(
        df_all, fitting_dict[material]["phi"], fitting_dict[material]["mu"]
    )
    df_final = df_final.rename({"region": "node", "demand_tot": "value"}, axis=1)
    df_final["commodity"] = material
    df_final["level"] = "demand"
    df_final["time"] = "year"
    df_final["unit"] = "t"
    # TODO: correct unit would be Mt but might not be registered on database
    df_final = message_ix.make_df("demand", **df_final)
    return df_final