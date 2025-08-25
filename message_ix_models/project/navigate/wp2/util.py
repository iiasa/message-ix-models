import logging
from collections.abc import Callable
from functools import lru_cache
from typing import TYPE_CHECKING, Literal, Union

import pandas as pd
from message_ix import Scenario, make_df

from message_ix_models.util import (
    ScenarioInfo,
    broadcast,
    nodes_ex_world,
    path_fallback,
)

if TYPE_CHECKING:
    from typing import Sequence, TypedDict

    MeltKwArgs = TypedDict(
        "MeltKwArgs", {"id_vars": Sequence[str], "var_name": str, "value_name": str}
    )


log = logging.getLogger(__name__)


def add_CCS_constraint(
    scen: Scenario, maximum_value: float, type_rel: Literal["lower", "upper"]
) -> None:
    """Add a CCS limit.

    The function assumes that `scen` uses the ``R12`` code list and has a set element
    n=``R12_GLB``.

    Parameters
    ----------
    maximum_value : str
        This should correspond to units of Gt CO₂ / year. Note that if `type_rel` is
        "lower", this is actually a *minimum* value.
    """
    log.info("Add CCS limits")

    # Add a new global relation to limit CCS activity. Obtain positive coefficients for
    # CCS technologies to put a limit to the overall CCS activity.
    node = "R12_GLB"
    relation = "global_co2_trans"
    info = ScenarioInfo(scen)

    # Prepare data
    # - Select values for some existing relations
    # - Exclude technologies that aggregate the CCS activity, which are not needed in
    #   this relation.
    # - Change negative values to positive
    df = (
        scen.par(
            "relation_activity",
            filters={"relation": ["bco2_trans_disp", "co2_trans_disp"]},
        )
        .where(lambda df: ~df["technology"].str.fullmatch("b?co2_tr_dis"))
        .dropna()
        .assign(node_rel=node, relation=relation, value=lambda df: df.value * -1)
        .astype({"year_rel": int, "year_act": int})
    )

    # Add the data
    with scen.transact("Global CCS relation set up"):
        scen.add_set("relation", relation)
        scen.add_par("relation_activity", df)

    # Prepare entry for the limit
    par = f"relation_{type_rel}"

    # Convert `maximum_value` from Gt CO₂ / year to Mt C / year. This assumes that the
    # activity of CCS technologies is expressed in the latter units.
    value = maximum_value * (10**3) * (12 / 44)

    years = filter(lambda x: x >= 2030, info.Y)

    with scen.transact(
        f"Add {type_rel} limit of {maximum_value} Gt CO₂ / year for CO2 emissions "
        f"accounted in relation {relation!r}"
    ):
        # FIXME the unit MtC does not exist in the database, so this incorrect label is
        #       used instead
        scen.add_par(
            par,
            make_df(
                par,
                node_rel=node,
                relation=relation,
                year_rel=years,
                value=value,
                unit="tC",
            ).astype({"year_rel": int}),
        )


#: List of technologies for :func:`add_electrification_share`
TECHS = {
    "non-elec": [
        # Base model / other industry technologies that output i_therm
        "foil_i",
        "loil_i",
        "biomass_i",
        "eth_i",
        "gas_i",
        "coal_i",
        "h2_i",
        # MESSAGEix-Materials technologies that output ht_heat
        # Cement industry
        "furnace_foil_cement",
        "furnace_loil_cement",
        "furnace_biomass_cement",
        "furnace_ethanol_cement",
        "furnace_methanol_cement",
        "furnace_gas_cement",
        "furnace_coal_cement",
        "furnace_h2_cement",
        # Aluminum industry
        "furnace_coal_aluminum",
        "furnace_foil_aluminum",
        "furnace_loil_aluminum",
        "furnace_ethanol_aluminum",
        "furnace_biomass_aluminum",
        "furnace_methanol_aluminum",
        "furnace_gas_aluminum",
        "furnace_h2_aluminum",
        # High Value Chemicals
        "furnace_coke_petro",
        "furnace_coal_petro",
        "furnace_foil_petro",
        "furnace_loil_petro",
        "furnace_ethanol_petro",
        "furnace_biomass_petro",
        "furnace_methanol_petro",
        "furnace_gas_petro",
        "furnace_h2_petro",
        # Resins
        "furnace_coal_resins",
        "furnace_foil_resins",
        "furnace_loil_resins",
        "furnace_ethanol_resins",
        "furnace_biomass_resins",
        "furnace_methanol_resins",
        "furnace_gas_resins",
        "furnace_h2_resins",
    ],
    "elec": [
        # Base model / other industry technologies that output i_therm
        "elec_i",
        # MESSAGEix-Materials technologies that output ht_heat
        "furnace_elec_cement",
        "furnace_elec_aluminum",
        "furnace_elec_petro",
        "furnace_elec_resins",
    ],
}

#: Data for :func:`add_electrification_share`. This is for kind="lo", that is, the
#: minimum share of the non-elec technologies in the total
ES = pd.DataFrame(
    [
        [2030, 0.4],
        [2035, 0.5],
        [2040, 0.6],
        [2045, 0.7],
        [2050, 0.8],
        [2055, 0.8],
        [2060, 0.8],
        [2070, 0.8],
        [2080, 0.8],
        [2090, 0.8],
        [2100, 0.8],
        [2110, 0.8],
    ],
    columns=["year_act", "value"],
)


def add_electrification_share(scen, kind=Literal["lo", "up"]):
    """Add share constraints for electrification in industry.

    There are no processes at the moment that use low temperature heat.

    This implementation includes Other Industry, Cement, Aluminum, High Value Chemicals
    and Resin production.

    Iron and Steel are not included since implementation is more complicated.

    Depends on scrap availability etc.

    Parameters
    ----------
    kind : str
        Type of constraint to implement. If "lo", then ``share_commodity_lo`` values are
        set to constrain the minimum share of "elec" outputs (per :data:`TECHS` in the
        total). If "up", then ``share_commodity_up`` values are set to constrain the
        maximum share of "non-elec" outputs in the total.
    """
    msg = "Add share constraints for electrification in industry"
    log.info(msg)

    info = ScenarioInfo(scen)
    nodes = nodes_ex_world(info.N)

    shares = "share_elec_ind"
    # type_tec labels
    tt_num = "elec_ind" if kind == "lo" else "non-elec_ind"
    tt_total = "all_ind"

    data = {
        tt_num: TECHS["elec"] if kind == "lo" else TECHS["non-elec"],
        tt_total: TECHS["elec"] + TECHS["non-elec"],
    }

    # Base data frame; all nodes, node_share == node
    # NB cannot use make_df(), since this is a set rather than a parameter
    base = (
        pd.DataFrame(
            None,
            index=[0],
            columns="shares node_share node type_tec mode commodity level".split(),
        )
        .pipe(broadcast, node=nodes)
        .eval("node_share = node")
        .assign(shares=shares)
    )
    # Re-used labels for commodity, level, and mode
    common = {
        "ht": dict(mode="high_temp", commodity="ht_heat"),
        "i": dict(mode="M1", commodity="i_therm", level="useful"),
    }
    # Levels for "ht" commodities
    levels = ["useful_cement", "useful_aluminum", "useful_petro", "useful_resins"]

    # Identify the technology types which appear in the numerator
    data["map_shares_commodity_share"] = pd.concat(
        [
            base.assign(**common["ht"]).pipe(broadcast, level=levels),
            base.assign(**common["i"]),
        ]
    ).assign(type_tec=tt_num)

    # Identify the technology types which appear in the denominator
    data["map_shares_commodity_total"] = pd.concat(
        [
            base.assign(**common["ht"]).pipe(broadcast, level=levels),
            base.assign(**common["i"]),
        ]
    ).assign(type_tec=tt_total)

    # Generate share constraint values
    name = f"share_commodity_{kind}"
    ya_value = ES.eval("value = 1.0 - value" if kind == "up" else "value = value")
    data[name] = (
        make_df(name, shares=shares, time="year", unit="-")
        .pipe(broadcast, ya_value, node_share=nodes)
        .astype({"year_act": int})
    )

    # log.info(data)  # For debugging

    with scen.transact(msg):
        scen.add_set("shares", shares)
        # NB(PNK 2023-05-07) This was not needed on the navigate_industry_backup
        # branch, but *is* needed in the WP6 application. This indicates that it is a
        # structure that should be added/ensured earlier, perhaps in .materials.
        scen.add_set("type_tec", [tt_num, tt_total])

        # Add all technologies which make up the numerator and denominator
        for cat in tt_num, tt_total:
            scen.add_cat("technology", cat, data[cat])

        # Add set and mapping set contents
        for name in "map_shares_commodity_share", "map_shares_commodity_total":
            scen.add_set(name, data[name])

        # Add parameter values
        name = f"share_commodity_{kind}"
        scen.add_par(name, data[name])

    # Remove the existing UE bounds if there are infeasibilities
    # Possible relations that can cause problem:
    # UE_industry_th
    # UE_industry_th_electric
    # FE_industry


def add_LED_setup(scen: Scenario):
    """Add LED setup to the scenario.

    This function is adjusted based on:

    https://github.com/volker-krey/message_data/blob/LED_update_materials/
    message_data/projects/led/LED_low_energy_demand_setup.R#L73

    Only relevant adjustments are chosen:

    - Cost adjustments for VREs.
    - Greater contribution of intermittent solar and wind to total electricity
      generation.
    - Adjust wind and solar PV operating reserve requirements.
    - Adjust wind and solar PV reserve margin requirements.
    - Increase the initial starting point value for activity growth bounds on the solar
      PV technology (centralized generation).
    """
    log.info("Add LED setup to the scenario")

    # Information about the scenario
    info = ScenarioInfo(scen)
    node_list = nodes_ex_world(info.N)

    # Common arguments for pd.DataFrame.melt
    melt_args: "MeltKwArgs" = dict(
        id_vars=["TECHNOLOGY", "REGION"], var_name="year_vtg", value_name="value"
    )
    # Common arguments for pd.DataFrame.rename
    rename_cols = {"TECHNOLOGY": "technology", "REGION": "node_loc"}

    data_path = path_fallback("alps", where="package local")

    scen.check_out()

    # Adjust the investment costs, fixed O&M costs and variable O&M costs for the
    # following technologies: solar_pv_ppl, stor_ppl, h2_elec, h2_fc_trp, solar_i,
    # h2_fc_I, h2_fc_RC.

    # Read technology investment costs from xlsx file and add to the scenario
    # Read technology fixed O&M costs from xlsx file and add to the scenario
    # Read technology variable O&M costs from xlsx file and add to the scenario
    filename = "granular-techs_cost_comparison_20170831_revAG_SDS_5year.xlsx"
    costs_file = pd.ExcelFile(data_path.joinpath(filename))

    common = dict(year_vtg=-1, year_act=-1, unit="USD/kWa", time="year", mode="M1")

    def _filter(df):
        return df[df.node_loc.isin(node_list) & df.year_vtg.isin(info.Y)]

    @lru_cache(maxsize=256)
    def _yv_ya(nl, t):
        try:
            return scen.vintage_and_active_years((nl, t))
        except ValueError:
            log.info(f"No vintage years for ({nl=}, {t=})")
            return pd.DataFrame([], columns=["year_vtg", "year_act"])

    for par_name, sheet_name in (
        ("inv_cost", "NewCosts_fixed"),
        ("fix_cost", "NewFOMCosts_fixed"),
        ("var_cost", "NewVOMCosts_fixed"),
    ):
        to_add = [make_df(par_name, **common)]

        data = (
            costs_file.parse(sheet_name)
            .melt(**melt_args)
            .dropna()
            .rename(columns=rename_cols)
            .pipe(_filter)
        )

        if par_name == "inv_cost":
            to_add.append(data)
        else:
            # Broadcast across the set of years_act appropriate for (nl, t)
            for (nl, t), group_data in data.groupby(["node_loc", "technology"]):
                to_add.append(group_data.merge(_yv_ya(nl, t), on="year_vtg"))

        scen.add_par(par_name, pd.concat(to_add).ffill().dropna())

    # Changing the renewable energy assumptions (steps) for the following technologies:
    # elec_t_d, h2_elec, relations: solar_step, solar_step2, solar_step3, wind_step,
    # wind_step2, wind_step3

    # Read solar and wind intermittency assumptions from xlsx file

    melt_args["id_vars"].append("RELATION")  # type: ignore [attr-defined]
    rename_cols.update(RELATION="relation", year_vtg="year_act")
    assign_args: dict[str, Union[str, Callable[[pd.DataFrame], pd.Series]]] = dict(
        unit="???",
        mode="M1",
        node_rel=lambda df: df.node_loc,
        year_rel=lambda df: df.year_act,
    )

    path_renew = data_path.joinpath("solar_wind_intermittency_20170831_5year.xlsx")
    # - steps_NEW: Adjust wind and solar PV resource steps (contribution to total
    #   electricity generation). These changes allow for greater contribution of
    #   intermittent solar and wind to total electricity generation.
    # - oper_NEW: adjust relation: oper_res, technologies: wind_cv1, windcv2, windcv3,
    #   windcv4, solar_cv1, solar_cv2, solar_cv3, solar_cv4, elec_trp.
    #
    #   Adjust wind and solar PV operating reserve requirements (amount of flexible
    #   generation that needs to be run for every unit of intermittent solar and wind
    #   => variable renewables <0, non-dispatchable thermal 0, flexible >0);
    #
    #   Also adjust the contribution of electric transport technologies to the operating
    #   reserves, increasing the amount they can contribute (vehicle-to-grid). These
    #   changes reduce the effective cost of building and running intermittent solar and
    #   wind plants, since the amount of back-up capacity built is less than before.
    # - resm_NEW: relation: res_marg, Technology:  wind_cv1, windcv2, windcv3, windcv4,
    #   solar_cv1, solar_cv2, solar_cv3, solar_cv4
    #
    #   Adjust wind and solar PV reserve margin requirements (amount of firm capacity
    #   that needs to be run to meet peak load and contingencies; intermittent solar and
    #   wind do not contribute a full 100% to the reserve margin). These changes allow
    #   for greater contribution of intermittent solar and wind to total electricity
    #   generation.
    for sheet_name in ("steps_NEW", "oper_NEW", "resm_NEW"):
        df = (
            pd.read_excel(path_renew, sheet_name=sheet_name)
            .melt(**melt_args)
            .dropna()
            .rename(columns=rename_cols)
            .assign(**assign_args)
        )
        scen.add_par("relation_activity", df)

    # Increase the initial starting point value for activity growth bounds on the solar
    # PV technology (centralized generation).

    technology = "solar_pv_ppl"
    years_subset = [2025, 2030, 2035, 2040, 2045, 2050]

    # Only do this for a subset of the regions for which there are currently
    # "growth_activity_up" (formerly "mpa up") values defined. We don't want to specify
    # an "initial_activity_up" for a technology that does not have a
    # "growth_activity_up".
    node_subset = scen.par("growth_activity_up", filters={"technology": technology})[
        "node_loc"
    ].unique()
    # NB(PNK 2023-05-05) Unclear what this line was for. It was (1) in R, (2) commented,
    #    and (3) referred to R11 node codes whereas the function was being applied for
    #    R12.
    # node_subset = ["R11_CPA", "R11_FSU", "R11_LAM", "R11_MEA", "R11_NAM", "R11_PAS"]

    for par, magnitude, unit in (
        ("initial_activity_up", 90, "GWa"),
        # Increase the initial starting point value for capacity growth bounds on the
        # solar PV technology (centralized generation)
        ("initial_new_capacity_up", 10, "GW"),
    ):
        df = (
            make_df(
                par,
                value=magnitude,
                unit=unit,
                technology=technology,
                time="year",  # for initial_activity_up
                year_act=years_subset,  # for initial_activity_up
                year_vtg=years_subset,  # for initial_new_capacity_up
            )
            .pipe(broadcast, node_loc=node_subset)
            .dropna(subset="node_loc")
        )
        if not len(df):
            log.warning(f"No {par!r} data added for t={technology!r}; {node_subset = }")

        scen.add_par(par, df)

    # Read useful level fuel potential contribution assumptions from xlsx file
    #
    # Adjust limits to potential fuel-specific contributions at useful energy
    # level (in each end-use sector separately)
    #
    # path_useful_fuel = data_path.joinpath(
    #     "useful_level_fuel_potential_contribution_20170907_5year.xlsx",
    # )
    # useful_fuel = (
    #     pd.read_excel(path_useful_fuel, sheet_name="UE_constraints_NEW")
    #     .melt(**melt_args)
    #     .dropna()
    #     .rename(columns=rename_cols)
    #     .assign(**assign_args)
    # )
    # scen.add_par("relation_activity", useful_fuel)

    scen.commit("LED changes implemented.")


def limit_h2(scen: Scenario, type: str = "green") -> None:
    """Limit hydrogen technology activities.

    Entries are added to `scen` for ``bound_activity_up`` for technologies:

    - h2_bio, h2_coal, h2_smr ("grey hydrogen" technologies), plus
    - if `type` is "green" (the default and only allowed value): h2_coal_ccs and
      h2_smr_ccs ("blue hydrogen" technologies).

    The following "green hydrogen" technologies are *not* constrained: h2_bio_ccs and
    h2_elec.

    Parameters
    ----------
    type : str, optional
        Type of hydrogen. :class:`ValueError` is raised for any value other than
        "green", the default.

    """
    log.info("Add h2 limit")

    info = ScenarioInfo(scen)

    # Exclude grey hydrogen options
    # In Blue hydrogen: h2_bio_ccs, h2_coal_ccs, h2_elec, h2_smr_ccs allowed.

    technology_list = ["h2_bio", "h2_coal", "h2_smr"]

    if type == "green":
        # Exclude blue hydrogen options as well. h2_bio_ccs, h2_elec are allowed.
        technology_list.extend(["h2_smr_ccs", "h2_coal_ccs"])
    else:
        raise ValueError(f"No such type {type!r} is defined")

    par = "bound_activity_up"

    with scen.transact(message="Hydrogen limits added."):
        scen.add_par(
            par,
            make_df(par, mode="M1", time="year", value=0, unit="GWa").pipe(
                broadcast,
                # NB here we might use message_ix_models.util.nodes_ex_world(info.N),
                #    but it is unclear if entries for e.g. R12_GLB are important. So
                #    exclude only "World", as message_data.tools.utilities.get_nodes().
                node_loc=list(filter(lambda n: n != "World", info.N)),
                technology=technology_list,
                year_act=info.Y,
            ),
        )
