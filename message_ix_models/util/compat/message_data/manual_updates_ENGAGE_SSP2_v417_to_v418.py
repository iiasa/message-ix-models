import numpy as np
import pandas as pd
from message_ix_models import ScenarioInfo
from message_ix_models.util import private_data_path

from . import (
    calibrate_UE_gr_to_demand,
    calibrate_vre,
    change_technology_lifetime,
    check_scenario_fix_and_inv_cost,
    update_fix_and_inv_cost,  # TODO Why is this a module?
    update_h2_blending,
)


def _apply_npi_updates(scen):
    """Apply changes from ENGAGE 4.1.7 process applied.

    An error which was found during the ENGAGE Task3.1 process, and was applied
    only to post NPi scenarios for technical-purposes, is now applied to the
    baseline scenario.

    A change is required to fix issues related to `coal_ppl_u` and `coal_extr`.
    For `coal_ppl_u` both the `inv_cost` for vintages 2020-onwards, are adjusted to
    match the maximum vintage-year the `input`.

    A second change is made to `coal_extr` in `R12_SAS`. The dynamic lower growth
    constraint is changed from -.05 to -.15, therefore allowing a faster phase-out
    of coal-use.

    A third change is applied to correct the `relation_activity` entry of "R12_NAM"
    into the relation used to limit the export of oil from CPA.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """
    with scen.transact("Changes to coal_ppl_u and coal_extr applied"):
        # Apply changes to `coal_ppl_u`
        df_inp = scen.par("input", filters={"technology": "coal_ppl_u"})
        df_inp = df_inp[["node_loc", "technology", "year_vtg"]]
        df_inp = df_inp[df_inp["year_vtg"] >= 1990].drop_duplicates()

        df_inv = scen.par("inv_cost", filters={"technology": "coal_ppl_u"})
        df_inv = df_inv[["node_loc", "technology", "year_vtg"]]
        df_inv = df_inv[df_inv["year_vtg"] >= 1990].drop_duplicates()

        dfs = []
        for n in df_inv["node_loc"].unique():
            tmp = df_inv.loc[(df_inv["node_loc"] == n)]
            tmp = tmp.loc[
                tmp["year_vtg"] > max(df_inp.loc[df_inp["node_loc"] == n, "year_vtg"])
            ]
            dfs.append(tmp)
        df = pd.concat(dfs)
        df["value"] = 0
        df["unit"] = "-"
        scen.add_par("bound_new_capacity_up", df)

        # Apply changes to `coal_extr` in SAS.
        dfmpa = scen.par(
            "growth_activity_lo",
            filters={"node_loc": "R12_SAS", "technology": ["coal_extr"]},
        )
        dfmpa = dfmpa.query("year_act > 2020")
        dfmpa.value = -0.15
        scen.add_par("growth_activity_lo", dfmpa)

        # Correct oil export constraint
        df_oil_exp = scen.par(
            "relation_activity",
            filters={"relation": "lim_exp_cpa_oil", "node_loc": "R12_NAM"},
        )
        df_oil_exp.value = -1
        scen.add_par("relation_activity", df_oil_exp)


def _correct_balance_td_efficiencies(scen):
    """Correct efficiencies used for calibration pruposes.

    For the so-called `balance` technologies, which connect the primary
    and the secondary level commodities, the efficiencies which have been
    used for calibration purposes are removed.
    The reason for this is because the bottom-up and top-down CO2 emissions
    accounting-gap is widened by this, as there are no emissions associated
    with these technologies. In MESSAGEV there were comments indicating that
    the efficiencies, especially for `coal_bal` could be associated with
    "Conv.RTS", possbily Rotary Triboelectrostatic Separator; in other words
    a process applied for "Dry Cleaning of Pulverized Coal", but the fact
    that the numbers arent documented and no emissions are associated with
    this have resulted in the decision by OFR and VK to remove the losses.
    For select transmission and distribution technologies, biomass and coal
    related, the same logic is applied.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """

    def _update_efficiency(tecs, value):
        """Function to update efficiency.

        Retrieves the `input` values for a given technology-list and
        updates the values not eequal to the specified `value` to
        the `value`.

        Parameters
        ----------
        tecs : lst
            List of technologies for `input`-values should be updated.
        value : number
            Value to which the `input`-values are to be updated.
        """
        # Retrieve input for technologies and filter-out non-value values.
        df = scen.par("input", filters={"technology": tecs})
        df = df.loc[df.value != value]

        if df.empty:
            return

        # Reset values
        df["value"] = value

        with scen.transact("Update technology efficiencies"):
            print(f"Updating efficiencies for technologies: {df.technology.unique()}")
            scen.add_par("input", df)

    # Retrieve list of technologies which are used to link primary and secondary energy
    balance_tecs = [t for t in scen.set("technology") if t[-4:] == "_bal"]
    _update_efficiency(balance_tecs, value=1)

    # Define "transmission and distribution" technologies for which values are to be
    # updated.
    td_tecs = ["biomass_t_d", "coal_t_d"]
    _update_efficiency(td_tecs, value=1)


def _correct_hp_gas_i_CO2(scen):
    """Correct relation activity for hp_gas_i.

    The relation used for accounting CO2-emissions from the residential and commercial
    sector in the reporting is `CO2_r_c`. `hp_gas_i` should though be accounted for
    under the industry emissions, hence `CO2_ind`.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """
    with scen.transact("Update hp_gas_i CO2 relation_activity"):
        df = scen.par(
            "relation_activity",
            filters={"technology": "hp_gas_i", "relation": "CO2_r_c"},
        )
        scen.remove_par("relation_activity", df)
        df.relation = "CO2_ind"
        scen.add_par("relation_activity", df)


def _update_vre_dyncap(scen):
    """Rerun dynamic capacity constraint generation for VREs.

    Corrections were made to the workflow for calibrating renewables,
    specifically to the calibration of the initial capacity limit where some
    regions were omitted (AFR, wind_ppl). The setting of the dynamic ramp-up of
    renewables is therefore re-run.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """
    calibrate_vre(
        scen,
        private_data_path(),
        upd_init_cap_only=True,
    )


def _update_incorrect_lifetimes(scen):
    """Update lifetimes of technologies.

    Specifically for the regions `NAM`, `PAO` and `WEU`, the vintage
    2040 for `coal_ppl` had incorrect prametrization, allowing the 2040
    vintage to be built at no cost and with no in- or output, only contributing
    via the `relation_activity`. The activity therefore could be scaled up to
    help allow for more heat to be generated from the `po_turbine` without actually
    generating electricity.
    The correction process for this is automated via the search criteria below.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """
    ip = scen.par("input")
    tl = scen.par("technical_lifetime")
    dp = scen.par("duration_period").set_index("year")["value"]

    def _expected_lifetime(ip, dp):
        """Calculate expected lifetimes.

        The lifetime of each technology, for every region and vintage is derived
        from the input.

        Parameters
        ----------
        ip : :class:`pandas.DataFrame`
            Parameter `input` data from scenario.
        dp : :class:`pandas.DataFrame`
            Parameter `duration_period` data from scenario.
        """
        idx = ["node_loc", "technology", "mode", "commodity", "level"]
        max_life = (
            ip[idx + ["year_vtg", "year_act"]]
            .drop_duplicates()
            .groupby(["year_vtg"] + idx)
            .max()
            .reset_index()
        )
        max_life["value_check"] = (
            max_life["year_act"]
            - max_life["year_vtg"]
            + dp.loc[max_life["year_vtg"].values].values
        )
        max_life = max_life.drop("year_act", axis="columns")
        return max_life

    def _check_lifetimes(tl, exp):
        """
        tl : :class:`pandas.DataFrame`
            Parameter `technical_lifetime` data from scenario.
        exp : :class:`pandas.DataFrame`
            Calculated expected lifetimes based on the `input`.
        """
        check = exp.merge(tl, on=["node_loc", "technology", "year_vtg"])
        check = check[check.value_check < check.value]
        check = check[(check.year_vtg + check.value) <= 2110]
        return check

    # Retrieve and check lifetimes of technologies
    exp = _expected_lifetime(ip, tl, dp)
    check = _check_lifetimes(tl, exp)

    # Apply corrections of the lifetimes
    for i in check.index:
        row = check.loc[i]
        tec = row.technology
        y = row.year_vtg
        tl = row.value
        x = row.value_check
        n = row.node_loc
        print(
            f"Updating technical lifetime for {tec} in {n} from {x} to {tl}",
            " years for vintage {y}",
        )
        change_technology_lifetime(
            scen,
            tec=tec,
            year_vtg_start=y,
            year_vtg_end=y,
            lifetime=tl,
            nodes=n,
            remove_rest=False,
            par_exclude=[
                "historical_activity",
                "historical_new_capacity",
                "ref_activity",
                "ref_new_capacity",
            ],
        )


def _clean_soft_constraints(scen):
    """Remove obsolete soft constraints.

    It is ensured that there are no soft-constraints or related
    parameters for any given node/technology/year_act where there
    is not either a growth_ or initial_activity_xx parameter.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """

    idx = ["node_loc", "technology", "year_act"]

    with scen.transact("Remove obsolete soft constraint"):
        for x in ["lo", "up"]:
            # Retrieve all upper/lower dynamic growth constraints
            # and combine dataframes.
            df_gr = (
                scen.par(f"growth_activity_{x}")
                .drop(["time", "unit"], axis=1)
                .assign(value="y")
            )
            df_i = (
                scen.par(f"initial_activity_{x}")
                .drop(["time", "unit"], axis=1)
                .assign(value="y")
            )
            df_dyn = df_gr.merge(df_i, how="outer").drop("value", axis=1)

            # Retrieve soft constraints // abs-cost // rel-cost
            df_sft = scen.par(f"soft_activity_{x}")
            df_sftabs = scen.par(f"abs_cost_activity_soft_{x}")
            df_sftrel = scen.par(f"level_cost_activity_soft_{x}")

            # Merge dataframes and select those which are only in "soft_constraints"
            # and remove them
            df_sft_only = df_sft.merge(
                df_dyn, how="outer", on=idx, indicator=True
            ).query('_merge == "left_only"')
            df_sftabs_only = df_sftabs.merge(
                df_dyn, how="outer", on=idx, indicator=True
            ).query('_merge == "left_only"')
            df_sftrel_only = df_sftrel.merge(
                df_dyn, how="outer", on=idx, indicator=True
            ).query('_merge == "left_only"')

            scen.remove_par(f"soft_activity_{x}", df_sft_only)
            scen.remove_par(f"abs_cost_activity_soft_{x}", df_sftabs_only)
            scen.remove_par(f"level_cost_activity_soft_{x}", df_sftrel_only)


def _clean_ren_dyn_act_up(scen):
    """Remove dynamic constraints act up for renewables.

    The renewable technologies have capacity related dynamic constraints
    as we all matching soft constraints. These are calibrated based on
    historical trends. The upscaling constraints on activity are
    therefore removed so that these are not more binding than the
    new_capacity_constraints.
    The lower dynamic activitry constraints are left in place as there
    are no lower dynamic capacity constraints.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """

    tecs = [
        "solar_pv_ppl",
        "wind_ppl",
        "wind_ppf",
        "csp_sm1_ppl",
        "csp_sm3_ppl",
    ]

    with scen.transact("Remove renewable dynamic upper constraints"):
        for par in ["growth_activity_up", "initial_activity_up"]:
            df = scen.par(par, filters={"technology": tecs})
            scen.remove_par(par, df)


def _clean_bound_activity(scen):
    """Remove obsolete bound_activity_lo/up.

    Many of the lower and upper activity bounds are the result of
    importing "timeseries" values from MESSAGEV, which were actually
    parametrized using "cg" (constant-growth).
    Previously, "cg" was used in combination with a calibrated value
    in the base year, but because the calibration has changed most
    of the `bound_activity_lo` and `bound_activity_up` parameters
    are now obsolete.

    There are condtions applied while filtering out the parameters
    to be deleted:
    1. All bounds must be in the model time-horizon (2020-onwards)
    2. Mode != "P" (the mode "P" has been used to identify technologies,
       which were configured as so-called "variables" in MESSAGEV. Their
       bounds have been used to reflect exogenously derived trajectories.)
    3. Technology name is the specified list; further explanations are
       provided in the list.
    4. The timeseries do not contain "zero" values. This ensures that no
       phase-out or SSP specific restrictions are deleted.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """

    # Specify technology names or strings found in names to apply
    # condition 3.
    tecs_keep = [
        # The following technologies are mitigation technologies
        # for various species.
        # Activity bounds represent the technical applicability,
        # hence these should not be removed.
        "ammonia_secloop",
        "leak_repair",
        "leak_repairsf6",
        "recycling_gas1",
        "refrigerant_recovery",
        # Other technologies with fixed trajectories
        #   Flaring_CO2 has a fixed trajectory.
        "flaring_CO2",
        #   bda_up in some regions (increasing); CPA has phaseout constraint
        "coal_ppl_u",
        # Various strings for technology groups for which bounds should be removed.
        "TCE",
        "extr",
        "csp",
        "solar",
        "wind",
    ]

    # Specifiy hard-exemptions
    bda_up_exmptions = {
        "foil_exp": "SAS",
        "loil_exp": "SAS",
    }

    model_years = ScenarioInfo(scen).Y

    with scen.transact("Remove activity bounds"):
        for bound in ["bound_activity_lo", "bound_activity_up"]:
            df = scen.par(bound)
            # Apply conditions 1. and 2.
            loc_idx = (df.year_act.isin(model_years)) & (df["mode"] != "P")
            df = df.loc[loc_idx]

            # Apply condition 3.
            tecs_keep_func = lambda t: any(k in t for k in tecs_keep)
            variables_keep = [
                t for t in df.technology.unique().tolist() if not tecs_keep_func(t)
            ]
            loc_idx = df.technology.isin(variables_keep)
            df = df.loc[loc_idx]

            # Apply condition 4.
            df = df.pivot_table(
                index=["node_loc", "technology", "mode", "time", "unit"],
                columns="year_act",
                values="value",
            )
            df = df.loc[~(df[df.columns] == 0).any(axis=1)]
            df = (
                df.reset_index()
                .melt(id_vars=["node_loc", "technology", "mode", "time", "unit"])
                .dropna()
            )
            scen.remove_par(bound, df)

            # Special expemptions are made manually
            if bound == "bound_activity_up":
                for i in bda_up_exmptions.items():
                    df = scen.par(
                        bound,
                        filters={
                            "technology": i[0],
                            "node_loc": i[1],
                            "year_act": model_years,
                        },
                    )


def update_meth_coal_ccs_weu(scen):
    """Resolve WEU `meth_coal_ccs`

    Set value from -.85 to .1 which is used across most other regions.
    Initial_new_capacity_up is fine

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """
    df = scen.par(
        "growth_new_capacity_up",
        filters={"node_loc": "R12_WEU", "technology": "meth_coal_ccs"},
    )
    df.value = 0.1
    with scen.transact("update incorrect growth rate of meth_coal_ccs in WEU"):
        scen.add_par("growth_new_capacity_up", df)


def update_gas_cc_lam(scen):
    """Resolve LAM `gas_cc` and `gas_cc_ccs`

    Set the value from -.901 to -0.05 which is used across most other regions.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """
    df = scen.par(
        "growth_activity_lo",
        filters={"node_loc": "R12_LAM", "technology": ["gas_cc", "gas_cc_ccs"]},
    )
    df.value = -0.05
    with scen.transact("update incorrect growth rate of gas_cc and gas_cc_ccs in LAM"):
        scen.add_par("growth_activity_lo", df)


def _correct_coal_ppl_u_efficiencies(scen):
    """Correct efficiencies of `coal_ppl_u`.

    The efficiency of `coal_ppl_u` was distorted during the process of extending the
    technical lifetime. The Values were extended by applying an "extrapolation" method
    as opposed to forward- or back-filling, resulting in very high efficiencies being
    reached.
    The values below are taken directly from the original "SSP2" MESSAGEV version and
    are used to update the scenario values.

    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """

    # Define years/index matching the values of `efficiency_update`
    efficiency_years = [1990, 1995, 2000, 2005, 2010, 2020, 2030, 2040, 2050, 2060]

    # Define the efficiencies to be applied to `coal_ppl_u`
    # The values below are taken from the `SSP2` input files of MESSAGEV
    # (see `P:\ene.model\SSP_V3`)
    efficiency_update = {
        "R12_AFR": [0.34, 0.34, 0.34, 0.37, 0.38, 0.38, np.nan, np.nan, np.nan, np.nan],
        "R12_RCPA": [
            0.33,
            0.33,
            0.326,
            0.321,
            0.34,
            0.36,
            0.38,
            np.nan,
            np.nan,
            np.nan,
        ],
        "R12_CHN": [0.33, 0.33, 0.326, 0.321, 0.34, 0.36, 0.38, np.nan, np.nan, np.nan],
        "R12_EEU": [0.315, 0.33, 0.35, 0.35, 0.36, 0.37, 0.38, np.nan, np.nan, np.nan],
        "R12_FSU": [0.327, 0.34, 0.31, 0.3, 0.31, 0.35, 0.38, np.nan, np.nan, np.nan],
        "R12_LAM": [0.287, 0.3, 0.32, 0.34, 0.35, 0.37, 0.38, np.nan, np.nan, np.nan],
        "R12_MEA": [
            0.353,
            0.37,
            0.396,
            0.397,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ],
        "R12_NAM": [
            0.37,
            0.375,
            0.368,
            0.369,
            0.38,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ],
        "R12_PAO": [
            0.383,
            0.38,
            0.371,
            0.388,
            0.38,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ],
        "R12_PAS": [
            0.366,
            0.36,
            0.352,
            0.354,
            0.36,
            0.38,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ],
        "R12_SAS": [0.288, 0.31, 0.2723, 0.2615, 0.265, 0.29, 0.31, 0.33, 0.35, 0.38],
        "R12_WEU": [
            0.368,
            0.375,
            0.386,
            0.403,
            0.38,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
            np.nan,
        ],
    }

    # Create pandas-dataframe
    df_effupd = pd.DataFrame.from_dict(
        efficiency_update, orient="index", columns=efficiency_years
    )

    # Retrieve all activity years for "coal_ppl_u"
    yr_act = [
        y
        for y in scen.par("input", {"technology": "coal_ppl_u"}).year_act.unique()
        if y not in df_effupd.columns
    ]

    # Add missing activity years
    for y in yr_act:
        df_effupd[y] = np.nan

    # Sort columns
    df_effupd = df_effupd[sorted(df_effupd.columns)]

    # Forward and back-fill
    df_effupd = df_effupd.ffill(axis=1).bfill(axis=1)

    # Derive output normalized efficincies
    df_effupd = 1 / df_effupd

    # Create long-format
    df_effupd = (
        df_effupd.melt(ignore_index=False, var_name="year_act")
        .reset_index()
        .rename(columns={"index": "node_loc"})
        .set_index(["node_loc", "year_act"])
    )

    # Retrieve input parameter for coal_ppl_u and update values
    df = scen.par("input", {"technology": "coal_ppl_u"}).set_index(
        ["node_loc", "year_act"]
    )
    df["value"] = df_effupd["value"]

    with scen.transact("Update coal_ppl_u efficiencies"):
        scen.add_par("input", df.reset_index())


def _correct_td_co2cc_emissions(scen):
    """Correct CO2cc and CO2_trade emission factors.

    This script adds emission-factors for the bottom CO2-accounting. The fix is
    primarily applied for select technologies which have both an input and output.
    This means that the emissions accounted for are those representing the "losses"
    or the own energy requirements of the technology e.g. transmission-and-distribution
    technologies or trade technologies.
    The emission-factors are added using the relations. No emission are added for
    technologies where no losses have been assumed. In some cases e.g. gas-export
    technologies, the emission factors are added as these seem to be missing from
    earlier formulations.
    Some exceptions are made for; no emissions are tracked for these:
    - `LH2_trd`, `lh2_t_d`, `h2_t_d`
    - `elec_trd`, `elec_t_d`
    - `eth_trd`, `eth_t_d` (the latter is removed)
    - `heat_t_d`
    - `biomass_t_d`


    Parameters
    ----------
    scen : message_ix.Scenario
        Scenario to which changes should be applied.
    """

    # Define dataframe index
    idx = ["node_loc", "technology", "year_act"]
    # Define dataframe index to be dropped from parameter `input`
    idx_input_drop = [
        "year_vtg",
        "mode",
        "node_origin",
        "commodity",
        "level",
        "time",
        "time_origin",
        "unit",
    ]

    # Provide input data to update-routine.
    # For each `commodity`, specify a list of `techologies` and the emission-factor
    #  `factor` in "tC/kWyr"
    # The list of technologies contains:
    # 1: `<technology_name>`: `technology` in model for which correction is made.
    # 2: `<relation_name>`: `relation` in model which accounts for emsissions of the
    #                      `technology`.
    # 3: None: will simply update `relation_activity`
    # 3: `add` (optional): option whether to add a `relation_activity` entry.
    # 3: `remove` (optional): option whether the `relation_activity` should be removed.
    # 3: `non-throughput` (optional): option whether the `techology` has an `output`
    #    not equal to the input e.g. is a power-plant. Then the `relation-activity`
    #    coefficient will be calcalated differently.

    df = {
        # Natural gas
        "gas": {
            "technologies": [
                ["gas_t_d", "CO2_cc"],
                ["gas_t_d_ch4", "CO2_cc"],
                ["LNG_trd", "CO2_trade"],
                ["gas_exp_afr", "CO2_trade", "add"],
                ["gas_exp_cpa", "CO2_trade", "add"],
                ["gas_exp_eeu", "CO2_trade", "add"],
                ["gas_exp_nam", "CO2_trade", "add"],
                ["gas_exp_pao", "CO2_trade", "add"],
                ["gas_exp_sas", "CO2_trade", "add"],
                ["gas_exp_weu", "CO2_trade", "add"],
                ["gas_ct", "CO2_cc", "non-throughput"],
            ],
            "factor": 0.482,
        },
        # Fuel-oil
        "foil": {
            "technologies": [
                ["foil_t_d", "CO2_cc"],
                ["foil_trd", "CO2_trade"],
            ],
            "factor": 0.665,
        },
        # raw Oil
        "oil": {
            "technologies": [
                ["oil_trd", "CO2_trade"],
            ],
            "factor": 0.631,
        },
        # Light-oil
        "loil": {
            "technologies": [
                ["loil_t_d", "CO2_cc"],
                ["loil_trd", "CO2_trade"],
            ],
            "factor": 0.631,
        },
        # Hard-coal
        "coal": {
            "technologies": [
                ["coal_t_d", "CO2_cc"],  # has eff. of 1
                [
                    "coal_t_d-in-06p",
                    "CO2_cc",
                ],  # Removed in newer versions; has eff. of 1
                ["coal_t_d-in-SO2", "CO2_cc"],  # Removed in newer versions
                [
                    "coal_t_d-rc-06p",
                    "CO2_cc",
                ],  # Removed in newer versions; has eff. of 1
                ["coal_t_d-rc-SO2", "CO2_cc"],  # Removed in newer versions
                ["coal_trd", "CO2_trade", "add"],
                ["coal_ppl_u", "CO2_cc", "non-throughput"],
            ],
            "factor": 0.814,
        },
        # Methanol
        "methanol": {
            "technologies": [
                ["meth_t_d", "CO2_cc"],
                ["meth_trd", "CO2_trade"],
            ],
            "factor": 0.549,
        },
        # Ethanol
        "ethanol": {
            "technologies": [
                ["eth_t_d", "CO2_cc", "remove"],
            ],
            "factor": 0.549,
        },
    }
    with scen.transact("Revise bottom-up CO2 emission accounting"):
        for commodity in df:
            emission = df[commodity]["factor"]
            for item in df[commodity]["technologies"]:
                technology = item[0]
                relation = item[1]

                # Check for additional commands
                task = None if len(item) < 3 else item[2]

                if task != "remove":
                    # Retrieve input
                    inp = scen.par("input", {"technology": technology})
                    inp = inp.drop(
                        idx_input_drop,
                        axis=1,
                    )

                    # Filter out efficiencies of 100%
                    inp = inp.loc[inp.value != 1]

                    if task != "add" and inp.empty:
                        print(f"Skipping technology: {technology}; efficiency is 100%.")
                        continue

                    # Rounding is needed in oreder to ensure there are no duplicates
                    # based on numerical precision differences.
                    inp["value"] = round(inp["value"], 5)
                    inp = inp.drop_duplicates()
                    # Make exception for "non-throughput"
                    if task == "non-throughput":
                        inp["CO2_should"] = inp["value"] * emission
                    else:
                        inp["CO2_should"] = (inp["value"] - 1) * emission
                    inp = inp.set_index(idx)

                # Add parameter
                if task == "add":
                    print(
                        f"Adding relation_activity for technology: {technology},",
                        f" relation: {relation}.",
                    )
                    inp = inp.reset_index()
                    inp["value"] = inp["CO2_should"]
                    inp = inp.drop("CO2_should", axis=1)
                    inp["relation"] = relation
                    inp["year_rel"] = inp["year_act"]
                    inp["mode"] = "M1"
                    inp["unit"] = "Mt C/GWyr/yr"
                    # Ensure that node_rel is "RXX_GLB" for "CO2_trade" relaiton entries
                    if relation == "CO2_trade":
                        reg = [n for n in scen.set("node") if "GLB" in n][0]
                        inp["node_rel"] = reg
                    else:
                        inp["node_rel"] = inp["node_loc"]
                    scen.add_par("relation_activity", inp)
                    continue

                # Retrieve relation-activity
                rel = scen.par(
                    "relation_activity",
                    {"technology": technology, "relation": relation},
                )

                # Remove relation-activity and continue
                if task == "remove":
                    print(
                        f"Removing relation_activity for technology: {technology},",
                        f" relation {relation}.",
                    )
                    scen.remove_par("relation_activity", rel)
                    continue

                rel = rel.set_index(idx)
                rel = rel.join(inp["CO2_should"])

                # Filter out differences
                rel = rel.loc[rel["value"] != rel["CO2_should"]]

                print(
                    f"Updating relation_activity for technology: {technology},",
                    f" relation {relation}.",
                )

                rel["value"] = rel["CO2_should"]
                rel = rel.drop(["CO2_should"], axis=1).reset_index()
                # In some cases where the eff. is 1 (dropped earlier)
                # `relation_activity` entries are set to 0.
                rel = rel.fillna(0)
                scen.add_par("relation_activity", rel)


def main(context, scen):
    """Apply updates to scenario to correct issues of ENGAGE v4.1.7"""

    # Step1. Add Changes that were made to 4.1.7 NPi variants.
    _apply_npi_updates(scen)

    # Step2. Re-run UE calibration due to interpolation error
    # when adding additional time-steps to source-data.
    # Newly added timesteps had wrong values for exceptions
    calibrate_UE_gr_to_demand(scen, data_path=private_data_path(), ssp=context.ssp)

    # Step3. Correct CO2 entry for hp_gas_i.
    # hp_gas_i has and entry into CO2_r_c, this should be correct to CO2_ind
    # The mistake was backtracked to GEA scenarios; no documentation as to why
    # this is the case.
    _correct_hp_gas_i_CO2(scen)

    # Step4. Update dynamic capacity constraints for "R12_AFR"->"wind_ppl"
    _update_vre_dyncap(scen)

    # Step5. Correct lifetime for coal_ppl in NAM, PAO, WEU -> coal_ppl
    _update_incorrect_lifetimes(scen)

    # Step6. Remove upper dynamic activity constraint.
    _clean_ren_dyn_act_up(scen)

    # Step7. Remove obsolete up/lo soft-activity constraints.
    _clean_soft_constraints(scen)

    # Re-Add fix and investment costs
    update_fix_and_inv_cost.main(scen, private_data_path(), context.ssp)

    # Check remaining fix and inv_cost and correct these.
    check_scenario_fix_and_inv_cost(scen)

    # Remove all obsolete bound_activity_up/lo
    _clean_bound_activity(scen)

    # Revise h2 blending constraint
    update_h2_blending.main(scen)

    # Correct meth_coal_ccs parameters in WEU
    update_meth_coal_ccs_weu(scen)

    # Correct gas_cc and gas_cc_ccs parameters in LAM
    update_gas_cc_lam(scen)

    # Update efficiencies for `balance` and specific `t_d` technologies.
    _correct_balance_td_efficiencies(scen)

    # Update efficiencies for `coal_ppl_u`
    _correct_coal_ppl_u_efficiencies(scen)

    # Update CO2cc-emissions for t_d technologies
    _correct_td_co2cc_emissions(scen)
