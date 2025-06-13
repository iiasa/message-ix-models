from collections import defaultdict

import pandas as pd
from message_ix import Scenario, make_df

from message_ix_models import ScenarioInfo
from message_ix_models.model.material.data_util import (
    calculate_furnace_non_co2_emi_coeff,
    get_thermal_industry_emi_coefficients,
    read_timeseries,
)
from message_ix_models.model.material.util import read_config
from message_ix_models.util import (
    broadcast,
    nodes_ex_world,
    package_data_path,
    same_node,
)


def read_data_generic(scenario: Scenario) -> (pd.DataFrame, pd.DataFrame):
    """Read and clean data from :file:`generic_furnace_boiler_techno_economic.xlsx`."""

    # Read the file
    data_generic = pd.read_excel(
        package_data_path(
            "material", "other", "generic_furnace_boiler_techno_economic.xlsx"
        ),
        sheet_name="generic",
    )

    # Clean the data
    # Drop columns that don't contain useful information
    data_generic = data_generic.drop(["Region", "Source", "Description"], axis=1)
    data_generic_ts = read_timeseries(
        scenario, "other", None, "generic_furnace_boiler_techno_economic.xlsx"
    )

    # Unit conversion
    # At the moment this is done in the excel file, can be also done here
    # To make sure we use the same units

    return data_generic, data_generic_ts


def add_non_co2_emission_coefficients(scen, df_input, method="from_disk"):
    if method == "from_disk":
        df_emi = pd.read_csv(
            package_data_path(
                "material", "other", "industry_thermal_emi_coefficients.csv"
            )
        )
    else:
        df_emi = get_thermal_industry_emi_coefficients(scen)
    df_input = df_input[df_input["year_act"].ge(scen.firstmodelyear)]
    df_input = df_input.set_index(["node_loc", "year_act", "commodity", "technology"])
    df_furnace_emi = calculate_furnace_non_co2_emi_coeff(df_input, df_emi)
    return df_furnace_emi


def add_ind_therm_link_relations(tecs, years, nodes):
    col_val_dict = {
        "relation": "IndThermDemLink",
        "mode": ["high_temp", "low_temp"],
        "unit": "???",
        "value": 1,
    }
    df = (
        make_df("relation_activity", **col_val_dict)
        .pipe(broadcast, node_loc=nodes)
        .pipe(broadcast, year_rel=years)
        .pipe(same_node)
        .pipe(broadcast, technology=tecs)
    )
    df["year_act"] = df["year_rel"]
    df = df[
        ~(
            (
                (df["technology"].str.startswith("solar"))
                | (df["technology"].str.startswith("fc_h2_"))
                | (df["technology"].str.startswith("furnace_h2"))
                | (df["technology"].str.startswith("dheat"))
            )
            & (df["year_act"].isin([2020, 2025]))
        )
    ]
    return df


def gen_data_generic(scenario, dry_run=False):
def gen_data_generic(
    scenario: Scenario, dry_run: bool = False
) -> dict[str, pd.DataFrame]:
    # Load configuration

    config = read_config()["material"]["generic"]

    # Information about scenario, e.g. node, year
    s_info = ScenarioInfo(scenario)

    # Techno-economic assumptions
    data_generic, data_generic_ts = read_data_generic(scenario)

    # List of data frames, to be concatenated together at end
    results = defaultdict(list)

    # For each technology there are differnet input and output combinations
    # Iterate over technologies

    modelyears = s_info.Y  # s_info.Y is only for modeling years
    yv_ya = s_info.yv_ya

    # Do not parametrize GLB region the same way
    nodes = nodes_ex_world(s_info.N)
    global_region = [i for i in s_info.N if i.endswith("_GLB")][0]

    for t in config["technology"]["add"]:
        t = t.id
        # years = s_info.Y
        params = data_generic.loc[
            (data_generic["technology"] == t), "parameter"
        ].values.tolist()

        # Availability year of the technology
        av = data_generic.loc[(data_generic["technology"] == t), "availability"].values[
            0
        ]
        modelyears = [year for year in modelyears if year >= av]
        yva = yv_ya.loc[yv_ya.year_vtg >= av,]

        # Iterate over parameters
        for par in params:
            split = par.split("|")
            param_name = split[0]

            val = data_generic.loc[
                (
                    (data_generic["technology"] == t)
                    & (data_generic["parameter"] == par)
                ),
                "value",
            ].values[0]

            # Common parameters for all input and output tables
            # year_act is none at the moment
            # node_dest and node_origin are the same as node_loc

            common = dict(
                year_vtg=yva.year_vtg,
                year_act=yva.year_act,
                time="year",
                time_origin="year",
                time_dest="year",
            )

            if len(split) > 1:
                if (param_name == "input") | (param_name == "output"):
                    com = split[1]
                    lev = split[2]
                    mod = split[3]

                    df = (
                        make_df(
                            param_name,
                            technology=t,
                            commodity=com,
                            level=lev,
                            mode=mod,
                            value=val,
                            unit="t",
                            **common,
                        )
                        .pipe(broadcast, node_loc=nodes)
                        .pipe(same_node)
                    )

                    results[param_name].append(df)

                elif param_name == "emission_factor":
                    emi = split[1]

                    # TODO: Now tentatively fixed to one mode.
                    #  Have values for the other mode too
                    df_low = make_df(
                        param_name,
                        technology=t,
                        value=val,
                        emission=emi,
                        mode="low_temp",
                        unit="t",
                        **common,
                    ).pipe(broadcast, node_loc=nodes)

                    df_high = make_df(
                        param_name,
                        technology=t,
                        value=val,
                        emission=emi,
                        mode="high_temp",
                        unit="t",
                        **common,
                    ).pipe(broadcast, node_loc=nodes)

                    results[param_name].append(df_low)
                    results[param_name].append(df_high)

            # Rest of the parameters apart from input, output and emission_factor

            else:
                df = make_df(
                    param_name, technology=t, value=val, unit="t", **common
                ).pipe(broadcast, node_loc=nodes)

                results[param_name].append(df)

    # Special treatment for time-varying params

    tec_ts = set(data_generic_ts.technology)  # set of tecs in timeseries sheet

    for t in tec_ts:
        common = dict(
            time="year",
            time_origin="year",
            time_dest="year",
        )

        param_name = data_generic_ts.loc[
            (data_generic_ts["technology"] == t), "parameter"
        ]

        for p in set(param_name):
            val = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "value",
            ]
            regions = data_generic_ts.loc[
                (
                    (data_generic_ts["technology"] == t)
                    & (data_generic_ts["parameter"] == p)
                ),
                "region",
            ]
            # units = data_generic_ts.loc[
            #     (data_generic_ts["technology"] == t)
            #     & (data_generic_ts["parameter"] == p),
            #     "units",
            # ].values[0]
            mod = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "mode",
            ]
            yr = data_generic_ts.loc[
                (data_generic_ts["technology"] == t)
                & (data_generic_ts["parameter"] == p),
                "year",
            ]

            if p == "var_cost":
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    **common,
                ).pipe(broadcast, node_loc=nodes)
            else:
                rg = data_generic_ts.loc[
                    (data_generic_ts["technology"] == t)
                    & (data_generic_ts["parameter"] == p),
                    "region",
                ]
                df = make_df(
                    p,
                    technology=t,
                    value=val,
                    unit="t",
                    year_vtg=yr,
                    year_act=yr,
                    mode=mod,
                    node_loc=rg,
                    **common,
                )

            # Copy parameters to all regions
            if (
                (len(set(regions)) == 1)
                and len(set(df["node_loc"])) == 1
                and list(set(df["node_loc"]))[0] != global_region
            ):
                df["node_loc"] = None
                df = df.pipe(broadcast, node_loc=nodes)

            results[p].append(df)

    results["relation_activity"].append(
        add_non_co2_emission_coefficients(scenario, pd.concat(results["input"]))
    )
    results["relation_activity"].append(
        add_ind_therm_link_relations(
            config["technology"]["add"], yv_ya["year_act"].unique(), nodes
        )
    )
    results = {par_name: pd.concat(dfs) for par_name, dfs in results.items()}

    return results
