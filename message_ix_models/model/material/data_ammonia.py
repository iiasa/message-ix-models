from collections import defaultdict
import logging
from pathlib import Path

import pandas as pd
from message_ix import make_df
from message_ix_models import ScenarioInfo
from message_ix_models.util import broadcast, same_node

from .util import read_config


log = logging.getLogger(__name__)

CONVERSION_FACTOR_CO2_C = 12 / 44
CONVERSION_FACTOR_NH3_N = 17 / 14
CONVERSION_FACTOR_PJ_GWa = 0.0317


def gen_data_ammonia(scenario, dry_run=False):
    """Generate data for materials representation of nitrogen fertilizers.

    .. note:: This code is only partially translated from
       :file:`SetupNitrogenBase.py`.
    """
    # Load configuration
    config = read_config()["material"]["fertilizer"]
    context = read_config()
    #print(config_.get_local_path("material", "test.xlsx"))
    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)
    nodes = s_info.N
    if (("World" in nodes) | ("R12_GLB" in nodes)):
        nodes.pop(nodes.index("World"))
        nodes.pop(nodes.index("R12_GLB"))
    # Techno-economic assumptions
    data = read_data()

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    input_commodity_dict = {
        "input_water": "freshwater_supply",
        "input_elec": "electr",
        "input_fuel": ""
    }
    output_commodity_dict = {
        "output_NH3": "NH3",
        "output_heat": "d_heat",
        "output_water": "wastewater"  # ask Jihoon how to name
    }
    commodity_dict = {
        "output": output_commodity_dict,
        "input": input_commodity_dict
    }
    input_level_dict = {
        "input_water": "water_supply",
        "input_fuel": "secondary",
        "input_elec": "secondary"
    }
    output_level_dict = {
        "output_water": "wastewater",
        "output_heat": "secondary",
        "output_NH3": "material_interim"
    }
    level_cat_dict = {
        "output": output_level_dict,
        "input": input_level_dict
    }

    # NH3 production processes
    common = dict(
        year_act=s_info.Y,  # confirm if correct??
        year_vtg=s_info.Y,
        commodity="NH3",
        level="material_interim",
        mode="all",
        time="year",
        time_dest="year",
        time_origin="year",
        emission="CO2"  # confirm if correct
    )

    # Iterate over new technologies, using the configuration
    for t in config["technology"]["add"]: # : refactor to adjust to yaml structure
        # Output of NH3: same efficiency for all technologies
        # the output commodity and level are different for
        #      t=NH3_to_N_fertil; use 'if' statements to fill in.

        for param in data['parameter'].unique():
            if (t == "electr_NH3") & (param == "input_fuel"):
                continue
            unit = "t"
            cat = data['param_cat'][data['parameter'] == param].iloc[0]
            if cat in ["input", "output"]:
                common["commodity"] = commodity_dict[cat][param]
                common["level"] = level_cat_dict[cat][param]
                if (t == "biomass_NH3") & (cat == "input"):
                    common["level"] = "primary"
            if (str(t) == "NH3_to_N_fertil") & (param == "output_NH3"):
                common['commodity'] = "Fertilizer Use|Nitrogen"
                common['level'] = "material_final"
            df = (
                make_df(cat, technology=t, value=1, unit=unit, **common)
                    .pipe(broadcast, node_loc=nodes)
                    .pipe(same_node)
            )

            row = data[(data['technology'] == t) &
                       (data['parameter'] == param)]
            df = df.assign(value=row[2010].values[0])

            if param == "input_fuel":
                comm = data['technology'][(data['parameter'] == param) &
                                          (data["technology"] == t)].iloc[0].split("_")[0]
                df = df.assign(commodity=comm)

            results[cat].append(df)

    # Historical activities/capacities - Region specific
    common = dict(
        commodity="NH3",
        level="material_interim",
        mode="all",
        time="year",
        time_dest="year",
        time_origin="year",
    )
    act2010 = read_demand()['act2010']
    df = (
        make_df("historical_activity",
                technology=[t for t in config["technology"]["add"]], #], TODO: maybe reintroduce std/ccs in yaml
                value=1, unit='t', years_act=s_info.Y, **common)
            .pipe(broadcast, node_loc=nodes)
            .pipe(same_node)
    )
    row = act2010

    # Unit is Tg N/yr
    results["historical_activity"].append(
        df.assign(value=row, unit="t", year_act=2010)
    )
    # 2015 activity necessary if this is 5-year step scenario
    # df['value'] = act2015 # total NH3 or N in Mt 2010 FAO Russia
    # df['year_act'] = 2015
    # Sc_nitro.add_par("historical_activity", df)

    df = (
        make_df("historical_new_capacity",
                technology=[t for t in config["technology"]["add"]], # ], refactor to adjust to yaml structure
                value=1, unit='t', years_act=s_info.Y, years_vtg=s_info.Y, **common)
            .pipe(broadcast, node_loc=nodes)
            .pipe(same_node)
    )

    # modifying act2010 values by assuming 1/lifetime (=15yr) is built each year and account for capacity factor
    capacity_factor = read_demand()['capacity_factor']
    row = act2010 * 1 / 15 / capacity_factor[0]

    # Unit is Tg N/yr
    results["historical_new_capacity"].append(
        df.assign(value=row, unit="t", year_act=2010)
    )

    # %% Secure feedstock balance (foil_fs, gas_fs, coal_fs)  loil_fs?

    # Adjust i_feed demand
    N_energy = read_demand()['N_energy']

    demand_fs_org = pd.read_excel(context.get_local_path('material','demand_i_feed_R12.xlsx'))

    df = demand_fs_org.loc[demand_fs_org.year == 2010, :].join(N_energy.set_index('node'), on='node')
    sh = pd.DataFrame({'node': demand_fs_org.loc[demand_fs_org.year == 2010, 'node'],
                       'r_feed': df.totENE / df.value})  # share of NH3 energy among total feedstock (no trade assumed)
    df = demand_fs_org.join(sh.set_index('node'), on='node')
    df.value *= 1 - df.r_feed  # Carve out the same % from tot i_feed values
    df = df.drop('r_feed', axis=1)
    df = df.drop('Unnamed: 0', axis=1)
    # TODO: refactor with a more sophisticated solution to reduce i_feed
    df.loc[df["value"] < 0, "value"] = 0  # tempoary solution to avoid negative values
    results["demand"].append(df)

    # Globiom land input
    df = pd.read_excel(context.get_local_path('material','GLOBIOM_Fertilizer_use_N.xlsx'))
    df = df.replace(regex=r'^R11', value="R12").replace(regex=r'^R12_CPA', value="R12_CHN")
    df["unit"] = "t"
    df.loc[df["node"] == "R12_CHN", "value"] *= 0.93 # hotfix to adjust to R12
    df_rcpa = df.loc[df["node"] == "R12_CHN"].copy(deep=True)
    df_rcpa["node"] = "R12_RCPA"
    df_rcpa["value"] *= 0.07
    df = df.append(df_rcpa)
    df = df.drop("Unnamed: 0", axis=1)
    results["land_input"].append(df)

    # add background parameters (growth rates and bounds)

    df = scenario.par('initial_activity_lo', {"technology": ["gas_extr_mpen"]})
    for q in config["technology"]["add"]:
        df['technology'] = q
        results["initial_activity_lo"].append(df)

    df = scenario.par('growth_activity_lo', {"technology": ["gas_extr_mpen"]})
    for q in config["technology"]["add"]:
        df['technology'] = q
        results["growth_activity_lo"].append(df)

    # TODO add regional cost scaling for ccs
    """
    # tec_scale = (newtechnames + newtechnames_ccs)
    tec_scale = [e for e in newtechnames if e not in ('NH3_to_N_fertil', 'electr_NH3')]

    # Scale all NH3 tecs in each region with the scaler
    for t in tec_scale:
        for p in ['inv_cost', 'fix_cost', 'var_cost']:
            df = Sc_nitro.par(p, {"technology": t})
            df = results[p][results[p]["technology"]==t]
            temp = df.join(scaler_cost.set_index('node_loc'), on='node_loc')
            df.value = temp.value * temp.scaler_std
            Sc_nitro.add_par(p, df)

    for t in newtechnames_ccs:
        for p in ['inv_cost', 'fix_cost', 'var_cost']:
            df = Sc_nitro.par(p, {"technology": t})
            temp = df.join(scaler_cost.set_index('node_loc'), on='node_loc')
            df.value = temp.value * temp.scaler_ccs
            Sc_nitro.add_par(p, df)
    """

    cost_scaler = pd.read_excel(
        context.get_local_path('material','regional_cost_scaler_R12.xlsx'), index_col=0).T

    scalers_dict = {
        "R12_CHN": {"coal_NH3": 0.75 * 0.91,  # gas/coal price ratio * discount
                    "fueloil_NH3": 0.66 * 0.91},  # gas/oil price ratio * discount
        "R12_SAS": {"fueloil_NH3": 0.59,
                    "coal_NH3": 1}
    }

    params = ["inv_cost", "fix_cost", "var_cost"]
    for param in params:
        for i in range(len(results[param])):
            df = results[param][i]
            if df["technology"].any() in ('NH3_to_N_fertil', 'electr_NH3'):  # skip those techs
                continue
            regs = df.set_index("node_loc").join(cost_scaler, on="node_loc")
            regs.value = regs.value * regs["standard"]
            regs = regs.reset_index()
            if df["technology"].any() in ("coal_NH3", "fueloil_NH3"):  # additional scaling to make coal/oil cheaper
                regs.loc[regs["node_loc"] == "R12_CHN", "value"] = \
                    regs.loc[regs["node_loc"] == "R12_CHN", "value"] * \
                    scalers_dict["R12_CHN"][df.technology[0]]
                regs.loc[regs["node_loc"] == "R12_SAS", "value"] = \
                    regs.loc[regs["node_loc"] == "R12_SAS", "value"] * \
                    scalers_dict["R12_SAS"][df.technology[0]]
            results[param][i] = regs.drop(["standard", "ccs"], axis="columns")

    # Concatenate to one dataframe per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results


def gen_data_ccs(scenario, dry_run=False):
    """Generate data for materials representation of nitrogen fertilizers.

    .. note:: This code is only partially translated from
       :file:`SetupNitrogenBase.py`.
    """
    config = read_config()["material"]["set"]
    context = read_config()

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)
    nodes = s_info.N
    if "World" in nodes:
        nodes.pop(nodes.index("World"))

    # Techno-economic assumptions
    data = read_data_ccs()

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # NH3 production processes
    common = dict(
        year_vtg=s_info.Y,
        year_act=s_info.Y, # confirm if correct??
        commodity="NH3",
        level="material_interim",
        # TODO fill in remaining dimensions
        mode="all",
        time="year",
        time_dest="year",
        time_origin="year",
        emission="CO2" # confirm if correct
        # node_loc='node'
    )

    input_commodity_dict = {
        "input_water": "freshwater_supply",
        "input_elec": "electr",
        "input_fuel": ""
    }
    output_commodity_dict = {
        "output_NH3": "NH3",
        "output_heat": "d_heat",
        "output_water": ""  # ask Jihoon how to name
    }
    commodity_dict = {
        "output": output_commodity_dict,
        "input": input_commodity_dict
    }
    input_level_dict = {
        "input_water": "water_supply",
        "input_fuel": "secondary",
        "input_elec": "secondary"
    }
    output_level_dict = {
        "output_water": "wastewater",
        "output_heat": "secondary",
        "output_NH3": "material_interim"
    }
    level_cat_dict = {
        "output": output_level_dict,
        "input": input_level_dict
    }

    # Iterate over new technologies, using the configuration
    for t in config["technology"]["add"]["ccs"]:
        # Output of NH3: same efficiency for all technologies
        # TODO the output commodity and level are different for
        #      t=NH3_to_N_fertil; use 'if' statements to fill in.

        for param in data['parameter'].unique():
            unit = "t"
            cat = data['param_cat'][data['parameter'] == param].iloc[0]
            if cat in ["input", "output"]:
                common["commodity"] = commodity_dict[cat][param]
                common["level"] = level_cat_dict[cat][param]
            df = (
                make_df(cat, technology=t, value=1, unit=unit, **common)
                    .pipe(broadcast, node_loc=nodes)
                    .pipe(same_node)
            )
            row = data[(data['technology'] == str(t)) &
                       (data['parameter'] == param)]
            df = df.assign(value=row[2010].values[0])

            if param == "input_fuel":
                comm = data['technology'][(data['parameter'] == param) &
                                          (data["technology"] == t)].iloc[0].split("_")[0]
                df = df.assign(commodity=comm)

            results[cat].append(df)


    # add background parameters (growth rates and bounds)

    df = scenario.par('initial_activity_lo', {"technology": ["gas_extr_mpen"]})
    for q in config["technology"]["add"]:
        df['technology'] = q
        results["initial_activity_lo"].append(df)

    df = scenario.par('growth_activity_lo', {"technology": ["gas_extr_mpen"]})
    for q in config["technology"]["add"]:
        df['technology'] = q
        results["growth_activity_lo"].append(df)

    cost_scaler = pd.read_excel(
        context.get_local_path('material','regional_cost_scaler_R12.xlsx'), index_col=0).T

    scalers_dict = {
        "R12_CHN": {"coal_NH3": 0.75 * 0.91,  # gas/coal price ratio * discount
                    "fueloil_NH3": 0.66 * 0.91},  # gas/oil price ratio * discount
        "R12_SAS": {"fueloil_NH3": 0.59,
                    "coal_NH3": 1}
    }

    params = ["inv_cost", "fix_cost", "var_cost"]
    for param in params:
        for i in range(len(results[param])):
            df = results[param][i]
            if df["technology"].any() in ('NH3_to_N_fertil', 'electr_NH3'):  # skip those techs
                continue
            regs = df.set_index("node_loc").join(cost_scaler, on="node_loc")
            regs.value = regs.value * regs["ccs"]
            regs = regs.reset_index()
            if df["technology"].any() in ("coal_NH3", "fueloil_NH3"):  # additional scaling to make coal/oil cheaper
                regs.loc[regs["node_loc"] == "R12_CHN", "value"] = \
                    regs.loc[regs["node_loc"] == "R12_CHN", "value"] * \
                    scalers_dict["R12_CHN"][df.technology[0].values[0].name]
                regs.loc[regs["node_loc"] == "R12_SAS", "value"] = \
                    regs.loc[regs["node_loc"] == "R12_SAS", "value"] * \
                    scalers_dict["R12_SAS"][df.technology[0].values[0].name]
            results[param][i] = regs.drop(["standard", "ccs"], axis="columns")

    # Concatenate to one dataframe per parameter
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results


def read_demand():
    """Read and clean data from :file:`CD-Links SSP2 N-fertilizer demand.Global.xlsx`."""
    # %% Demand scenario [Mt N/year] from GLOBIOM
    context = read_config()


    N_demand_GLO = pd.read_excel(context.get_local_path('material','CD-Links SSP2 N-fertilizer demand.Global_R12_adaption.xlsx'), sheet_name='data')

    # NH3 feedstock share by region in 2010 (from http://ietd.iipnetwork.org/content/ammonia#benchmarks)
    feedshare_GLO = pd.read_excel(context.get_local_path('material','Ammonia feedstock share.Global_R12.xlsx'), sheet_name='Sheet2', skiprows=14)

    # Read parameters in xlsx
    te_params = data = pd.read_excel(
        context.get_local_path("material", "n-fertilizer_techno-economic.xlsx"),
        sheet_name="Sheet1", engine="openpyxl", nrows=72
    )
    n_inputs_per_tech = 12  # Number of input params per technology

    input_fuel = te_params[2010][list(range(4, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)
    input_fuel[0:5] = input_fuel[0:5] * CONVERSION_FACTOR_PJ_GWa  # 0.0317 GWa/PJ, GJ/t = PJ/Mt NH3

    capacity_factor = te_params[2010][list(range(11, te_params.shape[0], n_inputs_per_tech))].reset_index(drop=True)

    # Regional N demaand in 2010
    ND = N_demand_GLO.loc[N_demand_GLO.Scenario == "NoPolicy", ['Region', 2010]]
    ND = ND[ND.Region != 'World']
    ND.Region = 'R12_' + ND.Region
    ND = ND.set_index('Region')

    # Derive total energy (GWa) of NH3 production (based on demand 2010)
    N_energy = feedshare_GLO[feedshare_GLO.Region != 'R12_GLB'].join(ND, on='Region')
    N_energy = pd.concat(
        [N_energy.Region, N_energy[["gas_pct", "coal_pct", "oil_pct"]].multiply(N_energy[2010], axis="index")], axis=1)
    N_energy.gas_pct *= input_fuel[2] * CONVERSION_FACTOR_NH3_N  # NH3 / N
    N_energy.coal_pct *= input_fuel[3] * CONVERSION_FACTOR_NH3_N
    N_energy.oil_pct *= input_fuel[4] * CONVERSION_FACTOR_NH3_N
    N_energy = pd.concat([N_energy.Region, N_energy.sum(axis=1)], axis=1).rename(
        columns={0: 'totENE', 'Region': 'node'})  # GWa

    # %% Process the regional historical activities

    fs_GLO = feedshare_GLO.copy()
    fs_GLO.insert(1, "bio_pct", 0)
    fs_GLO.insert(2, "elec_pct", 0)
    # 17/14 NH3:N ratio, to get NH3 activity based on N demand => No NH3 loss assumed during production
    fs_GLO.iloc[:, 1:6] = input_fuel[5] * fs_GLO.iloc[:, 1:6]
    fs_GLO.insert(6, "NH3_to_N", 1)

    # Share of feedstocks for NH3 prodution (based on 2010 => Assumed fixed for any past years)
    feedshare = fs_GLO.sort_values(['Region']).set_index('Region').drop('R12_GLB')

    # Get historical N demand from SSP2-nopolicy (may need to vary for diff scenarios)
    N_demand_raw = N_demand_GLO.copy()
    N_demand = N_demand_raw[(N_demand_raw.Scenario == "NoPolicy") & (N_demand_raw.Region != "World")].reset_index().loc[
               :, 2010]  # 2010 tot N demand
    N_demand = N_demand.repeat(6)

    act2010 = (feedshare.values.flatten() * N_demand).reset_index(drop=True)

    return {"feedshare_GLO": feedshare_GLO, "ND": ND, "N_energy": N_energy, "feedshare": feedshare, 'act2010': act2010,
            'capacity_factor': capacity_factor}


def read_data():
    """Read and clean data from :file:`n-fertilizer_techno-economic.xlsx`."""
    # Ensure config is loaded, get the context
    context = read_config()
    print(context.get_local_path())
    #print(Path(__file__).parents[3]/"data"/"material")
    context.handle_cli_args(local_data=Path(__file__).parents[3]/"data")
    print(context.get_local_path())
    # Shorter access to sets configuration
    sets = context["material"]["fertilizer"]

    # Read the file
    data = pd.read_excel(
        context.get_local_path("material", "n-fertilizer_techno-economic_new.xlsx"),
        sheet_name="Sheet1", engine="openpyxl", nrows=72
    )

    # Prepare contents for the "parameter" and "technology" columns
    # FIXME put these in the file itself to avoid ambiguity/error

    # "Variable" column contains different values selected to match each of
    # these parameters, per technology
    params = [
        "inv_cost",
        "fix_cost",
        "var_cost",
        "technical_lifetime",
        "input_fuel",
        "input_elec",
        "input_water",
        "output_NH3",
        "output_water",
        "output_heat",
        "emission_factor",
        "capacity_factor",
    ]

    param_values = []
    tech_values = []
    param_cat = [split.split('_')[0] if
                 (split.startswith('input') or split.startswith('output'))
                 else split for split in params]

    param_cat2 = []
    # print(param_cat)
    for t in sets["technology"]["add"]: # : refactor to adjust to yaml structure
        # print(t)
        param_values.extend(params)
        tech_values.extend([t] * len(params))
        param_cat2.extend(param_cat)

    # Clean the data
    data = (
        # Insert "technology" and "parameter" columns
        data.assign(technology=tech_values,
                    parameter=param_values,
                    param_cat=param_cat2)
            # Drop columns that don't contain useful information
            .drop(["Model", "Scenario", "Region"], axis=1)
        # Set the data frame index for selection
    )
    data.loc[data['parameter'] == 'emission_factor', 2010] = \
            data.loc[data['parameter'] == 'emission_factor', 2010] * CONVERSION_FACTOR_CO2_C
    data.loc[data['parameter'] == 'input_elec', 2010] = \
        data.loc[data['parameter'] == 'input_elec', 2010] * CONVERSION_FACTOR_PJ_GWa

    # TODO convert units for some parameters, per LoadParams.py
    return data


def read_data_ccs():
    """Read and clean data from :file:`n-fertilizer_techno-economic.xlsx`."""
    # Ensure config is loaded, get the context
    context = read_config()

    # Shorter access to sets configuration
    sets = context["material"]["set"]

    # Read the file
    data = pd.read_excel(
        context.get_local_path("material", "n-fertilizer_techno-economic.xlsx"),
        sheet_name="CCS",
    )

    # Prepare contents for the "parameter" and "technology" columns
    # FIXME put these in the file itself to avoid ambiguity/error

    # "Variable" column contains different values selected to match each of
    # these parameters, per technology
    params = [
        "inv_cost",
        "fix_cost",
        "var_cost",
        "technical_lifetime",
        "input_fuel",
        "input_elec",
        "input_water",
        "output_NH3",
        "output_water",
        "output_heat",
        "emission_factor",
        "emission_factor",
        "capacity_factor",
    ]

    param_values = []
    tech_values = []
    param_cat = [split.split('_')[0] if (split.startswith('input') or split.startswith('output')) else split for split
                 in params]

    param_cat2 = []

    for t in sets["technology"]["add"]["ccs"]:
        param_values.extend(params)
        tech_values.extend([t] * len(params))
        param_cat2.extend(param_cat)

    # Clean the data
    data = (
        # Insert "technology" and "parameter" columns
        data.assign(technology=tech_values, parameter=param_values, param_cat=param_cat2)
            # , param_cat=param_cat2)
            # Drop columns that don't contain useful information
            .drop(["Model", "Scenario", "Region"], axis=1)
        # Set the data frame index for selection
    )
    #unit conversions and extra electricity for CCS process
    data.loc[data['parameter'] == 'emission_factor', 2010] = \
        data.loc[data['parameter'] == 'emission_factor', 2010] * CONVERSION_FACTOR_CO2_C
    data.loc[data['parameter'] == 'input_elec', 2010] = \
        data.loc[data['parameter'] == 'input_elec', 2010] * CONVERSION_FACTOR_PJ_GWa + 0.005

    # TODO convert units for some parameters, per LoadParams.py
    return data
