from collections import defaultdict

import message_ix
import pandas as pd

from message_ix_models.util import package_data_path


def gen_data_power_sector(
    scenario: message_ix.Scenario, dry_run: bool = False
) -> dict[str, pd.DataFrame]:
    """Generate data for materials representation of power industry."""
    # Load configuration

    # paths to lca data
    data_path = package_data_path("material", "power_sector")

    # Information about scenario, e.g. node, year

    # read inv.cost data
    inv_cost = scenario.par("inv_cost")

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # for p in set(param_name):
    #     df = read_material_intensities(
    #         p, str(data_path), node, year, technology, commodity, level, inv_cost
    #     )
    #     print("type df:", type(df))
    #     print(df.head())
    #
    #     results[p].append(df)

    int_dict = read_material_intensities(str(data_path), inv_cost)
    for k, v in int_dict.items():
        results[k].append(v)

    # create new parameters input_cap_new, output_cap_new, input_cap_ret,
    # output_cap_ret, input_cap and output_cap if they don't exist
    maybe_init_pars(scenario)

    # Concatenate to one data frame per parameter
    return {par_name: pd.concat(dfs) for par_name, dfs in results.items()}


def read_material_intensities(
    data_path: str, inv_cost: pd.DataFrame
) -> dict[str, pd.DataFrame]:
    ####################################################################
    # read data
    ####################################################################

    # read LCA data from ADVANCE LCA tool
    data_path_lca = data_path + "/NTNU_LCA_coefficients.xlsx"
    data_lca = pd.read_excel(data_path_lca, sheet_name="environmentalImpacts")

    # For hydropower material intensity use "medium" from Kalt et al., 2021.
    # Unit: t/MW

    data_lca.loc[
        (
            (data_lca["technology"] == "Hydro")
            & (data_lca["technology variant"] == "mix")
            & (data_lca["phase"] == "Construction")
            & (data_lca["impact"] == "Iron")
        ),
        2010,
    ] = 45

    data_lca.loc[
        (
            (data_lca["technology"] == "Hydro")
            & (data_lca["technology variant"] == "mix")
            & (data_lca["phase"] == "Construction")
            & (data_lca["impact"] == "Iron")
        ),
        2030,
    ] = 45

    data_lca.loc[
        (
            (data_lca["technology"] == "Hydro")
            & (data_lca["technology variant"] == "mix")
            & (data_lca["phase"] == "Construction")
            & (data_lca["impact"] == "Aluminium")
        ),
        2010,
    ] = 0.572

    data_lca.loc[
        (
            (data_lca["technology"] == "Hydro")
            & (data_lca["technology variant"] == "mix")
            & (data_lca["phase"] == "Construction")
            & (data_lca["impact"] == "Aluminium")
        ),
        2030,
    ] = 0.572

    data_lca.loc[
        (
            (data_lca["technology"] == "Hydro")
            & (data_lca["technology variant"] == "mix")
            & (data_lca["phase"] == "Construction")
            & (data_lca["impact"] == "Cement")
        ),
        2010,
    ] = 787.5

    data_lca.loc[
        (
            (data_lca["technology"] == "Hydro")
            & (data_lca["technology variant"] == "mix")
            & (data_lca["phase"] == "Construction")
            & (data_lca["impact"] == "Cement")
        ),
        2030,
    ] = 787.5

    # read technology, region and commodity mappings
    data_path_tec_map = data_path + "/MESSAGE_global_model_technologies.xlsx"
    technology_mapping = pd.read_excel(data_path_tec_map, sheet_name="technology")

    data_path_reg_map = data_path + "/LCA_region_mapping.xlsx"
    region_mapping = pd.read_excel(data_path_reg_map, sheet_name="region")

    data_path_com_map = data_path + "/LCA_commodity_mapping.xlsx"
    commodity_mapping = pd.read_excel(data_path_com_map, sheet_name="commodity")

    ####################################################################
    # process data
    ####################################################################

    # filter relevant scenario, technology variant (residue for biomass,
    # mix for others) and remove operation phase (and remove duplicates)
    data_lca = data_lca.loc[
        (
            (data_lca["scenario"] == "Baseline")
            & (data_lca["technology variant"].isin(["mix", "residue"]))
            & (data_lca["phase"] != "Operation")
        )
    ]

    data_lca[2015] = None
    data_lca[2020] = None
    data_lca[2025] = None
    data_lca[2035] = None
    data_lca[2040] = None
    data_lca[2045] = None

    # add intermediate time steps and turn into long table format
    years = [2010, 2015, 2020, 2025, 2030, 2035, 2040, 2045]
    data_lca = pd.melt(
        data_lca,
        id_vars=[
            "source",
            "scenario",
            "region",
            "variable",
            "technology",
            "technology variant",
            "impact",
            "phase",
            "unit",
        ],
        value_vars=years,
        var_name="year",
    )
    # Make sure the values are numeric.
    data_lca[["value"]] = data_lca[["value"]].astype(float)

    # apply technology, commodity/impact and region mappings to MESSAGEix
    data_lca_merged_1 = pd.merge(
        data_lca, region_mapping, how="inner", left_on="region", right_on="THEMIS"
    )
    data_lca_merged_2 = pd.merge(
        data_lca_merged_1,
        technology_mapping,
        how="inner",
        left_on="technology",
        right_on="LCA mapping",
    )
    data_lca_merged_final = pd.merge(
        data_lca_merged_2,
        commodity_mapping,
        how="inner",
        left_on="impact",
        right_on="impact",
    )

    data_lca = data_lca_merged_final[
        ~data_lca_merged_final["MESSAGEix-GLOBIOM_1.1"].isnull()
    ]

    # Drop technology column that has LCA style names
    # Instead MESSAGE technology names will be used in the technology column
    data_lca = data_lca.drop(["technology"], axis=1)

    data_lca.rename(
        columns={
            "MESSAGEix-GLOBIOM_1.1": "node",
            "Type of Technology": "technology",
        },
        inplace=True,
    )
    keep_columns = [
        "node",
        "technology",
        "phase",
        "commodity",
        "level",
        "year",
        "unit",
        "value",
    ]
    data_lca = data_lca[keep_columns]

    data_lca_final = pd.DataFrame()

    for n in data_lca["node"].unique():
        for t in data_lca["technology"].unique():
            for c in data_lca["commodity"].unique():
                for p in data_lca["phase"].unique():
                    temp = data_lca.copy(deep=True).loc[
                        (
                            (data_lca["node"] == n)
                            & (data_lca["technology"] == t)
                            & (data_lca["commodity"] == c)
                            & (data_lca["phase"] == p)
                        )
                    ]
                    temp["value"] = temp["value"].interpolate(
                        method="linear", limit_direction="forward", axis=0
                    )
                    data_lca_final = pd.concat([temp, data_lca_final])

    # extract node, technology, commodity, level, and year list from LCA
    # data set
    node_list = data_lca_final["node"].unique()
    year_list = data_lca_final["year"].unique()
    tec_list = data_lca_final["technology"].unique()
    com_list = data_lca_final["commodity"].unique()
    lev_list = data_lca_final["level"].unique()
    # add scrap as commodity level
    lev_list = lev_list + ["end_of_life"]

    ####################################################################
    # create data frames for material intensity input/output parameters
    ####################################################################

    # new data frames for parameters
    input_cap_new = pd.DataFrame()
    input_cap_ret = pd.DataFrame()
    output_cap_ret = pd.DataFrame()

    for n in node_list:
        for t in tec_list:
            for c in com_list:
                year_vtg_list = inv_cost.loc[
                    ((inv_cost["node_loc"] == n) & (inv_cost["technology"] == t))
                ]["year_vtg"].unique()
                for y in year_vtg_list:
                    # for years after maximum year in data set use
                    # values for maximum year, similarly for years
                    # before minimum year in data set use values for
                    # minimum year
                    if y > max(year_list):
                        yeff = max(year_list)
                    elif y < min(year_list):
                        yeff = min(year_list)
                    else:
                        yeff = y
                    val_cap_new = data_lca_final.loc[
                        (data_lca_final["node"] == n)
                        & (data_lca_final["technology"] == t)
                        & (data_lca_final["phase"] == "Construction")
                        & (data_lca_final["commodity"] == c)
                        & (data_lca_final["year"] == yeff)
                    ]["value"].values[0]
                    val_cap_new = val_cap_new * 0.001

                    input_cap_new = pd.concat(
                        [
                            input_cap_new,
                            pd.DataFrame(
                                {
                                    "node_loc": n,
                                    "technology": t,
                                    "year_vtg": str(y),
                                    "node_origin": n,
                                    "commodity": c,
                                    "level": "product",
                                    "time_origin": "year",
                                    "value": val_cap_new,
                                    "unit": "t/kW",
                                },
                                index=[0],
                            ),
                        ]
                    )

                    val_cap_input_ret = data_lca_final.loc[
                        (data_lca_final["node"] == n)
                        & (data_lca_final["technology"] == t)
                        & (data_lca_final["phase"] == "End-of-life")
                        & (data_lca_final["commodity"] == c)
                        & (data_lca_final["year"] == yeff)
                    ]["value"].values[0]
                    val_cap_input_ret = val_cap_input_ret * 0.001

                    input_cap_ret = pd.concat(
                        [
                            input_cap_ret,
                            pd.DataFrame(
                                {
                                    "node_loc": n,
                                    "technology": t,
                                    "year_vtg": str(y),
                                    "node_origin": n,
                                    "commodity": c,
                                    "level": "product",
                                    "time_origin": "year",
                                    "value": val_cap_input_ret,
                                    "unit": "t/kW",
                                },
                                index=[0],
                            ),
                        ]
                    )

                    val_cap_output_ret = data_lca_final.loc[
                        (data_lca_final["node"] == n)
                        & (data_lca_final["technology"] == t)
                        & (data_lca_final["phase"] == "Construction")
                        & (data_lca_final["commodity"] == c)
                        & (data_lca_final["year"] == yeff)
                    ]["value"].values[0]
                    val_cap_output_ret = val_cap_output_ret * 0.001

                    output_cap_ret = pd.concat(
                        [
                            output_cap_ret,
                            pd.DataFrame(
                                {
                                    "node_loc": n,
                                    "technology": t,
                                    "year_vtg": str(y),
                                    "node_dest": n,
                                    "commodity": c,
                                    "level": "end_of_life",
                                    "time_dest": "year",
                                    "value": val_cap_output_ret,
                                    "unit": "t/kW",
                                },
                                index=[0],
                            ),
                        ]
                    )

    return {
        "input_cap_new": input_cap_new,
        "input_cap_ret": input_cap_ret,
        "output_cap_ret": output_cap_ret,
    }


def maybe_init_pars(scenario: message_ix.Scenario) -> None:
    if not scenario.has_par("input_cap_new"):
        scenario.init_par(
            "input_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap_new"):
        scenario.init_par(
            "output_cap_new",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
    if not scenario.has_par("input_cap_ret"):
        scenario.init_par(
            "input_cap_ret",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap_ret"):
        scenario.init_par(
            "output_cap_ret",
            idx_sets=[
                "node",
                "technology",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
    if not scenario.has_par("input_cap"):
        scenario.init_par(
            "input_cap",
            idx_sets=[
                "node",
                "technology",
                "year",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "year_act",
                "node_origin",
                "commodity",
                "level",
                "time_origin",
            ],
        )
    if not scenario.has_par("output_cap"):
        scenario.init_par(
            "output_cap",
            idx_sets=[
                "node",
                "technology",
                "year",
                "year",
                "node",
                "commodity",
                "level",
                "time",
            ],
            idx_names=[
                "node_loc",
                "technology",
                "year_vtg",
                "year_act",
                "node_dest",
                "commodity",
                "level",
                "time_dest",
            ],
        )
