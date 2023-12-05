import message_ix_models.util
import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
import yaml

file_cement = "/CEMENT.BvR2010.xlsx"
file_steel = "/STEEL_database_2012.xlsx"
file_al = "/demand_aluminum.xlsx"
# file_petro = "/demand_petro.xlsx"
file_gdp = "/iamc_db ENGAGE baseline GDP PPP.xlsx"

giga = 10**9
mega = 10**6

material_data_dirname = {
    "aluminum": "aluminum",
    "steel": "steel_cement",
    "cement": "steel_cement"
}

def steel_function(x, a, b, m):
    gdp_pcap, del_t = x
    return a * np.exp(b / gdp_pcap) * (1 - m) ** del_t


def cement_function(x, a, b):
    gdp_pcap = x
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


def derive_steel_demand(df_pop, df_demand, datapath):
    # Read GDP data
    gdp_ppp = pd.read_excel(f"{datapath}/other{file_gdp}", sheet_name="data_R12")
    gdp_ppp = (
        gdp_ppp[gdp_ppp["Scenario"] == "baseline"]
        .loc[:, ["Region", *[i for i in gdp_ppp.columns if type(i) == int]]]
        .melt(id_vars="Region", var_name="year", value_name="gdp_ppp")
        .query('Region != "World"')
        .assign(
            year=lambda x: x["year"].astype(int), region=lambda x: "R12_" + x["Region"]
        )
    )

    # Read raw steel consumption data
    df_raw_steel_consumption = pd.read_excel(
        f"{datapath}/steel_cement{file_steel}",
        sheet_name="Consumption regions",
        nrows=26,
    )
    df_raw_steel_consumption = (
        df_raw_steel_consumption.rename({"t": "region"}, axis=1)
        .drop(columns=["Unnamed: 1"])
        .rename({"TIMER Region": "reg_no"}, axis=1)
    )
    df_raw_steel_consumption = df_raw_steel_consumption.melt(
        id_vars=["region", "reg_no"], var_name="year", value_name="consumption"
    )
    df_raw_steel_consumption["region"] = (
        df_raw_steel_consumption["region"]
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("\d+", "")
        .str[:-1]
    )

    # Read population data
    df_population = pd.read_excel(
        f"{datapath}/steel_cement{file_cement}",
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

    # Read GDP per capita data
    df_gdp = pd.read_excel(
        f"{datapath}/steel_cement{file_cement}",
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

    # Organize data
    df_steel_consumption = (
        pd.merge(
            df_raw_steel_consumption,
            df_population.drop("region", axis=1),
            on=["reg_no", "year"],
        )
        .merge(df_gdp[["reg_no", "year", "gdp_pcap"]], on=["reg_no", "year"])
        .assign(
            cons_pcap=lambda x: x["consumption"] / x["pop"],
            del_t=lambda x: x["year"].astype(int) - 2010,
        )
        .dropna()
        .query("cons_pcap > 0")
    )

    # Assuming df_steel_consumption contains the necessary columns (cons.pcap, gdp.pcap, del_t)
    x_data = df_steel_consumption["gdp_pcap"]
    y_data = df_steel_consumption["cons_pcap"]
    del_t_data = df_steel_consumption["del_t"]

    # Initial guess for parameters
    initial_guess = [600, -10000, 0]

    # Perform nonlinear least squares fitting
    params_opt, _ = curve_fit(
        steel_function, (x_data, del_t_data), y_data, p0=initial_guess
    )

    # Extract the optimized parameters
    a_opt, b_opt, m_opt = params_opt
    print(f"a: {a_opt}, b: {b_opt}, m: {m_opt}")

    # Merge with steel consumption data
    df_in = pd.merge(df_pop, df_demand.drop(columns=["year"]), how="left")
    df_in = pd.merge(df_in, gdp_ppp, how="inner")
    df_in["del_t"] = df_in["year"] - 2010
    df_in["gdp_pcap"] = df_in["gdp_ppp"] * giga / df_in["pop.mil"] / mega

    df_in["demand_pcap0"] = df_in.apply(
        lambda row: steel_function((row["gdp_pcap"], row["del_t"]), *params_opt), axis=1
    )

    # Demand Prediction and Calculations
    df_demand = df_in.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap_base=group["demand.tot.base"].iloc[0]
            * giga
            / group["pop.mil"].iloc[0]
            / mega
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            gap_base=group["demand_pcap_base"].iloc[0] - group["demand_pcap0"].iloc[0]
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap=group["demand_pcap0"]
            + group["gap_base"] * gompertz(9, 0.1, y=group["year"])
        )
    )
    df_demand = (
        df_demand.groupby("region")
        .apply(
            lambda group: group.assign(
                demand_tot=group["demand_pcap"] * group["pop.mil"] * mega / giga
            )
        )
        .reset_index(drop=True)
    )

    # Add 2110 placeholder
    # df_demand_components = pd.concat([df_demand_components, df_demand_components[df_demand_components['year'] == 2100].assign(year=2110)])

    # return df_demand_components[['node', 'year', 'demand_tot']].sort_values(by=['year', 'node']).reset_index(drop=True)
    return df_demand


def derive_cement_demand(df_pop, df_demand, datapath):
    # Read GDP data
    gdp_ppp = pd.read_excel(f"{datapath}/other{file_gdp}", sheet_name="data_R12")
    gdp_ppp = (
        gdp_ppp[gdp_ppp["Scenario"] == "baseline"]
        .loc[:, ["Region", *[i for i in gdp_ppp.columns if type(i) == int]]]
        .melt(id_vars="Region", var_name="year", value_name="gdp_ppp")
        .query('Region != "World"')
        .assign(
            year=lambda x: x["year"].astype(int), region=lambda x: "R12_" + x["Region"]
        )
    )

    # Read raw steel consumption data
    df_raw_cement_consumption = pd.read_excel(
        f"{datapath}/steel_cement{file_cement}",
        sheet_name="Regions",
        skiprows=122,
        nrows=26,
    )
    # print(df_raw_steel_consumption.columns)
    df_raw_cement_consumption = df_raw_cement_consumption.rename(
        {"Region #": "reg_no", "Region": "region"}, axis=1
    )
    df_raw_cement_consumption = df_raw_cement_consumption.melt(
        id_vars=["region", "reg_no"], var_name="year", value_name="consumption"
    )
    # df_raw_cement_consumption["region"] = df_raw_cement_consumption["region"].str.replace("(","").str.replace(")","").str.replace('\d+', '').str[:-1]

    # Read population data
    df_population = pd.read_excel(
        f"{datapath}/steel_cement{file_cement}",
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

    # Read GDP per capita data
    df_gdp = pd.read_excel(
        f"{datapath}/steel_cement{file_cement}",
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

    # Organize data
    df_cement_consumption = (
        pd.merge(
            df_raw_cement_consumption,
            df_population.drop("region", axis=1),
            on=["reg_no", "year"],
        )
        .merge(df_gdp[["reg_no", "year", "gdp_pcap"]], on=["reg_no", "year"])
        .assign(
            cons_pcap=lambda x: x["consumption"] / x["pop"] / 10**6,
            del_t=lambda x: x["year"].astype(int) - 2010,
        )
        .dropna()
        .query("cons_pcap > 0")
    )

    # Assuming df_steel_consumption contains the necessary columns (cons.pcap, gdp.pcap, del_t)
    x_data = df_cement_consumption["gdp_pcap"]
    y_data = df_cement_consumption["cons_pcap"]

    # Initial guess for parameters
    initial_guess = [500, -3000]
    # df_steel_consumption.to_csv("py_output.csv")
    # Perform nonlinear least squares fitting
    params_opt, _ = curve_fit(cement_function, x_data, y_data, p0=initial_guess)

    # Extract the optimized parameters
    a_opt, b_opt = params_opt
    print(f"a: {a_opt}, b: {b_opt}")

    # Merge with steel consumption data
    df_in = pd.merge(df_pop, df_demand.drop(columns=["year"]), how="left")
    df_in = pd.merge(df_in, gdp_ppp, how="inner")
    df_in["del_t"] = df_in["year"] - 2010
    df_in["gdp_pcap"] = df_in["gdp_ppp"] * giga / df_in["pop.mil"] / mega

    df_in["demand_pcap0"] = df_in.apply(
        lambda row: cement_function(row["gdp_pcap"], *params_opt), axis=1
    )

    # Demand Prediction and Calculations
    df_demand = df_in.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap_base=group["demand.tot.base"].iloc[0]
            * giga
            / group["pop.mil"].iloc[0]
            / mega
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            gap_base=group["demand_pcap_base"].iloc[0] - group["demand_pcap0"].iloc[0]
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap=group["demand_pcap0"]
            + group["gap_base"] * gompertz(10, 0.1, y=group["year"])
        )
    )
    df_demand = (
        df_demand.groupby("region")
        .apply(
            lambda group: group.assign(
                demand_tot=group["demand_pcap"] * group["pop.mil"] * mega / giga
            )
        )
        .reset_index(drop=True)
    )

    # Add 2110 placeholder
    # df_demand_components = pd.concat([df_demand_components, df_demand_components[df_demand_components['year'] == 2100].assign(year=2110)])

    # return df_demand_components[['node', 'year', 'demand_tot']].sort_values(by=['year', 'node']).reset_index(drop=True)
    return df_demand


def derive_alu_demand(df_pop, df_demand, datapath):
    # Read GDP data
    gdp_ppp = pd.read_excel(f"{datapath}/other{file_gdp}", sheet_name="data_R12")
    # print(gdp_ppp.columns)
    gdp_ppp = (
        gdp_ppp[gdp_ppp["Scenario"] == "baseline"]
        .loc[:, ["Region", *[i for i in gdp_ppp.columns if type(i) == int]]]
        .melt(id_vars="Region", var_name="year", value_name="gdp_ppp")
        .query('Region != "World"')
        .assign(
            year=lambda x: x["year"].astype(int), region=lambda x: "R12_" + x["Region"]
        )
    )

    # Read raw steel consumption data
    df_raw_alu_consumption = (
        pd.read_excel(
            f"{datapath}/aluminum{file_al}", sheet_name="final_table", nrows=378
        )
        .drop(["cons.pcap", "del.t"], axis=1)
        .rename({"gdp.pcap": "gdp_pcap"}, axis=1)
    )

    # Organize data
    df_alu_consumption = (
        df_raw_alu_consumption.assign(
            cons_pcap=lambda x: x["consumption"] / x["pop"],
            del_t=lambda x: x["year"].astype(int) - 2010,
        )
        .dropna()
        .query("cons_pcap > 0")
    )

    # Assuming df_steel_consumption contains the necessary columns (cons.pcap, gdp.pcap, del_t)
    x_data = df_alu_consumption["gdp_pcap"]
    y_data = df_alu_consumption["cons_pcap"]

    # Initial guess for parameters
    initial_guess = [600, -10000]

    # Perform nonlinear least squares fitting
    params_opt, _ = curve_fit(cement_function, x_data, y_data, p0=initial_guess)

    # Extract the optimized parameters
    a_opt, b_opt = params_opt
    print(f"a: {a_opt}, b: {b_opt}")

    # Merge with steel consumption data
    df_in = pd.merge(df_pop, df_demand.drop(columns=["year"]), how="left")
    df_in = pd.merge(df_in, gdp_ppp, how="inner")
    df_in["del_t"] = df_in["year"] - 2010
    df_in["gdp_pcap"] = df_in["gdp_ppp"] * giga / df_in["pop.mil"] / mega

    df_in["demand_pcap0"] = df_in.apply(
        lambda row: cement_function(row["gdp_pcap"], *params_opt), axis=1
    )

    # Demand Prediction and Calculations
    df_demand = df_in.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap_base=group["demand.tot.base"].iloc[0]
            * giga
            / group["pop.mil"].iloc[0]
            / mega
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            gap_base=group["demand_pcap_base"].iloc[0] - group["demand_pcap0"].iloc[0]
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap=group["demand_pcap0"]
            + group["gap_base"] * gompertz(9, 0.1, y=group["year"])
        )
    )
    df_demand = (
        df_demand.groupby("region")
        .apply(
            lambda group: group.assign(
                demand_tot=group["demand_pcap"] * group["pop.mil"] * mega / giga
            )
        )
        .reset_index(drop=True)
    )

    # Add 2110 placeholder
    # df_demand_components = pd.concat([df_demand_components, df_demand_components[df_demand_components['year'] == 2100].assign(year=2110)])

    # return df_demand_components[['node', 'year', 'demand_tot']].sort_values(by=['year', 'node']).reset_index(drop=True)
    return df_demand


def read_pop(datapath, filename):
    df_population = pd.read_excel(
        f"{datapath}/steel_cement{filename}",
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


def read_hist_gdp(datapath, filename):
    # Read GDP per capita data
    df_gdp = pd.read_excel(
        f"{datapath}/steel_cement{filename}",
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
    df_demand = df.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap_base=group["demand.tot.base"].iloc[0]
            * giga
            / group["pop.mil"].iloc[0]
            / mega
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            gap_base=group["demand_pcap_base"].iloc[0] - group["demand_pcap0"].iloc[0]
        )
    )
    df_demand = df_demand.groupby("region").apply(
        lambda group: group.assign(
            demand_pcap=group["demand_pcap0"]
            + group["gap_base"] * gompertz(phi, mu, y=group["year"])
        )
    )
    df_demand = (
        df_demand.groupby("region")
        .apply(
            lambda group: group.assign(
                demand_tot=group["demand_pcap"] * group["pop.mil"] * mega / giga
            )
        )
        .reset_index(drop=True)
    )
    return df_demand


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
        df_population = pd.read_excel(
            f"{datapath}/steel_cement{file_cement}",
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

        # Read GDP per capita data
        df_gdp = pd.read_excel(
            f"{datapath}/steel_cement{file_cement}",
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

    if material == "aluminum":
        df_raw_cons = (
            pd.read_excel(
                f"{datapath}/aluminum{file_al}", sheet_name="final_table", nrows=378
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
            f"{datapath}/steel_cement{file_steel}",
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
            pd.merge(
                df_raw_cons, df_population.drop("region", axis=1), on=["reg_no", "year"]
            )
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
            f"{datapath}/steel_cement{file_cement}",
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
            pd.merge(
                df_raw_cons, df_population.drop("region", axis=1), on=["reg_no", "year"]
            )
            .merge(df_gdp[["reg_no", "year", "gdp_pcap"]], on=["reg_no", "year"])
            .assign(
                cons_pcap=lambda x: x["consumption"] / x["pop"] / 10**6,
                del_t=lambda x: x["year"].astype(int) - 2010,
            )
            .dropna()
            .query("cons_pcap > 0")
        )
    else:
        print("non-available material selected. must be one of [aluminum, steel, cement]")
        df_cons = None
    return df_cons


def derive_demand(material, scen):

    datapath = message_ix_models.util.private_data_path("material")

    # read pop from scenario
    pop = scen.par("bound_activity_up", {"technology": "Population"})
    pop = pop.loc[pop.year_act >= 2020].rename(
        columns={"year_act": "year", "value": "pop.mil", "node_loc": "region"}
    )

    # read gdp from scenario
    gdp = scen.par("bound_activity_up", {"technology": "GDP"})
    gdp = gdp.loc[gdp.year_act >= 2020].rename(
        columns={"year_act": "year", "value": "gdp_ppp", "node_loc": "region"}
    )

    # get base year demand of material
    df_base_demand = read_base_demand(f"{datapath}/{material_data_dirname[material]}/demand_{material}.yaml")

    # get historical material data
    df_cons = read_hist_mat_demand(material)
    x_data = tuple(pd.Series(df_cons[col]) for col in fitting_dict[material]["x_data"])

    # run regression on historical data
    params_opt, _ = curve_fit(
        fitting_dict[material]["function"],
        xdata=x_data,
        ydata=df_cons["cons_pcap"],
        p0=fitting_dict[material]["initial_guess"]
    )
    print(f"{params_opt}")

    df_in = pd.merge(pop, df_base_demand.drop(columns=["year"]), how="left")
    df_in = pd.merge(df_in, gdp[["region", "year", "gdp_ppp"]], how="inner")
    df_in["del_t"] = df_in["year"] - 2010
    df_in["gdp_pcap"] = df_in["gdp_ppp"] * giga / df_in["pop.mil"] / mega

    df_in["demand_pcap0"] = df_in.apply(
        lambda row: fitting_dict[material]["function"](tuple(row[i] for i in fitting_dict[material]["x_data"]), *params_opt), axis=1
    )
    df_in = df_in.rename({"value":"demand.tot.base"}, axis=1)
    df_final = project_demand(df_in, fitting_dict[material]["phi"], fitting_dict[material]["mu"])

    return df_final.drop(['technology', 'mode', 'time', 'unit'], axis=1)
