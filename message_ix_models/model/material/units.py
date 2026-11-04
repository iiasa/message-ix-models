"""This module can be used to generate parameter data for MESSAGEix-Materials with
corrected units."""

import os
import re
from typing import TYPE_CHECKING, Any

import pandas as pd

from message_ix_models.util import package_data_path

if TYPE_CHECKING:
    from message_ix_models import Scenario


dim_par_map = {
    "ACT": ["technology", "mode"],
    "CAP": ["technology", "mode"],
    "CAP_NEW": ["technology", "mode"],
    "ACT_cost": ["technology", "mode"],
    "CAP_cost": ["technology", "mode"],
    "CAP_NEW_cost": ["technology", "mode"],
    "DEMAND": ["commodity"],
    "REL": ["relation"],
    "dimensionless": ["technology", "mode"],
    "time": ["technology"],
}


def group_parameters(ps: list) -> dict[str, set[Any] | set[str | Any] | list[str]]:
    """Groups parameters into categories that must have the same unit
    assigned per unit-defining index dimensions.

    Parameters
    ----------
    ps :
        List of available parameters.
    """
    cost_pars = {i for i in ps if ("cost" in i)}
    tot_cap_pars = {
        i
        for i in ps
        if ("total_capacity" in i) & (("bound" in i) | ("hist" in i) | ("initial" in i))
    } | {"fixed_capacity"}
    new_cap_pars = {
        i
        for i in ps
        if ("new_capacity" in i)
        & (("bound" in i) | ("hist" in i) | ("initial" in i) | ("fixed" in i))
    }
    act_pars = {
        i
        for i in ps
        if ("activity" in i)
        & (("bound" in i) | ("hist" in i) | ("initial" in i) | ("fixed" in i))
    }
    land_pars = {i for i in ps if ("land" in i)}
    rel_pars = {i for i in ps if ("relation" in i)}

    act_cost = {i for i in cost_pars if ("activity" in i) & ("level" not in i)} | {
        "var_cost"
    }
    cap_cost = {
        i
        for i in cost_pars
        if ("capacity" in i) & ("new" not in i) & ("level" not in i)
    } | {"fix_cost"}
    cap_new_cost = {
        i for i in cost_pars if ("new_capacity" in i) & ("level" not in i)
    } | {"inv_cost"}
    lvl_cost = [i for i in cost_pars if ("cost" in i) & ("level" in i)]

    par_types = {
        "ACT": act_pars,
        "CAP": tot_cap_pars,
        "CAP_NEW": new_cap_pars,
        "ACT_cost": act_cost,
        "CAP_cost": cap_cost,
        "CAP_NEW_cost": cap_new_cost,
        "DEMAND": ["demand"],
        "REL": rel_pars,
    }
    # ps - cost_pars - tot_cap_pars - new_cap_pars - act_pars - land_pars - rel_pars
    excluded = {x for sub in par_types.values() for x in sub}
    ungrouped = [item for item in ps if (item not in excluded)]
    par_types["dimensionless"] = ungrouped
    return par_types


def get_parameter_indices() -> dict[str, pd.DataFrame]:
    """Get unique index dimensions for each parameter of MESSAGEix-Materials."""
    p = package_data_path("material", "other", "par_dimension_set")
    return {
        k.replace(".csv", ""): pd.read_csv(p.joinpath(k)).dropna()
        for k in os.listdir(p)
        if k.endswith(".csv")
    }


def get_dim_set(par_data_reduced, dims) -> pd.DataFrame:
    """Get unique combinations of given index dimensions.

    Mimics the computation of `map_<dim>` sets defined in the GAMS code of MESSAGEix.

    Parameters
    ----------
    par_data_reduced:
    dims
    """
    dim_pars = [
        p
        for p in par_data_reduced
        if all(dim in par_data_reduced[p].columns for dim in dims)
    ]
    result = pd.concat(
        [par_data_reduced[p][dims].drop_duplicates() for p in dim_pars]
    ).drop_duplicates()
    return result


def classify_tecs(df: pd.DataFrame) -> pd.DataFrame:
    """Classify technologies as either energy or mass type."""
    substrings = ["ref", "fur", "hp_", "solar", "dheat", "fc_", "gas_pro", "loil_trp"]
    pattern = "|".join(re.escape(s) for s in substrings)

    t_GWyr = df[df["technology"].str.contains(pattern, na=False)].assign(type="energy")
    t_mt = df[~df["technology"].str.contains(pattern, na=False)].assign(type="mass")
    result = pd.concat([t_GWyr, t_mt])
    return result


def classify_rels(df: pd.DataFrame) -> pd.DataFrame:
    """Classify relations as either energy or mass type."""
    mass_rels = [
        "eaf_bound_2020",
        "bof_bound_2020",
        "bf_bound_2020",
        "max_global_recycling_steel",
        "max_regional_recycling_steel",
        "maximum_recycling_aluminum",
        "meth_exp_tot",
        "minimum_recycling_aluminum",
        "NH3_trd_cap",
        "NFert_trd_cap",
        "minimum_recycling_steel",
    ]
    mass = df[df["relation"].isin(mass_rels)].assign(type="mass")
    emi_units = {
        "BCA_Emission": "kt BC/yr",
        "CO_Emission": "kt CO/yr",
        "CO2_cc": "Mt C/yr",
        "CO2_Emission": "Mt C/yr",
        "CO2_Emission_Global_Total": "Mt C/yr",
        "CO2_feedstocks": "Mt C/yr",
        "CO2_ind": "Mt C/yr",
        "CO2_trade": "Mt C/yr",
        "CO2_trp": "Mt C/yr",
        "NH3_Emission": "kt NH3/yr",
        "NOx_Emission": "kt NOx/yr",
        "OCA_Emission": "kt OC/yr",
        "VOC_Emission": "kt VOC/yr",
        "HFC_Emission": "kt HFC-134a-equiv./yr",
        "HFC_mobile_red": "t HFC-134a-equiv./yr",
        "h2_scrub_limit": "Mt C/yr",
    }
    ene = [
        "coal-exp",
        "h2_exp_limit",
        "lim_exp_afr_meth",
        "lim_exp_eeu_meth",
        "lim_exp_fsu_meth",
        "lim_exp_pao_meth",
        "lim_exp_weu_meth",
        "meth_exp_limit",
        "oper_res",
        "PE_total_traditional",
        "RES_variable_limt",
        "solar_curtailment_1",
        "solar_curtailment_2",
        "solar_curtailment_3",
        "solar_step",
        "solar_step2",
        "solar_step3",
        "wind_curtailment_1",
        "wind_curtailment_2",
        "wind_curtailment_3",
        "wind_step",
        "wind_step2",
        "wind_step3",
    ]
    ene = df[df["relation"].isin(ene)].assign(type="energy")
    emi = df[df["relation"].isin(emi_units.keys())].assign(
        type=lambda d: d["relation"].map(emi_units)
    )
    result = pd.concat([mass, ene, emi])
    return result


def classify_dim(df: pd.DataFrame, dim: list[str]) -> pd.DataFrame:
    """Classify dimension items as either energy or mass type."""
    if dim == ["technology", "mode"]:
        return classify_tecs(df)
    if dim == ["technology"]:
        return classify_tecs(df)
    elif dim == ["commodity"]:
        return df.assign(type="mass")
    elif dim == ["relation"]:
        return classify_rels(df)
    else:
        raise ValueError(f"Unknown dimension: {dim}")


def generate_lu_tables() -> pd.DataFrame:
    """Generate lookup table for parameter units."""
    ene_units = {
        "ACT": "GWyr/yr",
        "CAP_NEW": "GW/yr",
        "CAP": "GW",
        "ACT_cost": "USD_2005/kWyr",
        "CAP_cost": "USD_2005/kW/yr",
        "CAP_NEW_cost": "USD_2005/kW",
        "DEMAND": "GWyr/yr",
        "REL": "GWyr/yr",
        "time": "yr",
    }
    mass_units = {
        "ACT": "Mt/yr",
        "CAP_NEW": "Mt/yr/yr",
        "CAP": "Mt/yr",
        "ACT_cost": "USD_2005/t",
        "CAP_cost": "USD_2005/t",
        "CAP_NEW_cost": "USD_2005/t/yr",
        "DEMAND": "Mt/yr",
        "REL": "Mt/yr",
        "time": "yr",
    }
    r1 = pd.DataFrame(["energy"], columns=["type"]).assign(**ene_units)
    r2 = pd.DataFrame(["mass"], columns=["type"]).assign(**mass_units)
    return pd.concat([r1, r2]).assign(dimensionless="-")


def overwrite_units(
    scen: "Scenario",
    par: str,
    par_type,
    dim_filter: pd.DataFrame,
) -> pd.DataFrame:
    """Overwrite units for a given parameter.

    Parameters
    ----------
    scen :
        Scenario to retrieve parameter data from.
    par :
        Parameter to overwrite units for.
    dim_filter :
        DataFrame containing unique combinations of index dimensions used to filter parameter data.
    """
    # - Prepare unit mapping (index = columns of par that define unit)
    unit_map = generate_lu_tables().set_index("type")
    par_dims = scen.idx_names(par)
    if not any(dim in dim_filter.columns for dim in par_dims):
        return pd.DataFrame()
    # - Query parameter data from scenario
    query_filter = {
        col: dim_filter[col].unique() for col in dim_filter.columns if col in par_dims
    }
    df = dim_filter.merge(scen.par(par, filters=query_filter).drop(columns=["unit"]))
    if df.empty:
        return df
    dim_classes = classify_dim(df, dim_filter.columns.tolist())
    if "relation" in df.columns:
        return overwrite_relation_unit(par, dim_classes, unit_map)
    # - Retrieve units for this par from unit mapping table
    # - Merge with parameter data to get new unit values
    new = dim_classes.merge(unit_map[par_type].rename("unit").reset_index()).drop(
        "type", axis=1
    )
    return new


def overwrite_relation_unit(
    par: str, df: pd.DataFrame, unit_map: pd.DataFrame
) -> pd.DataFrame:
    """Overwrite units of given relation_* parameter data.

    Relation parameters need special treatment
    because the unit can be defined by two dimensions.
    E.g., The unit of a relation_activity data point is defined by the unit of the
    relation and the activity of the technology.

    Parameters
    ----------
    par :
        Parameter name
    df :
        DataFrame containing `par` data
    """
    units = unit_map["REL"]
    div_look_up = pd.read_csv(
        package_data_path("material", "other", "unit_division.csv")
    )
    if any(i in par for i in ["upper", "lower"]):
        result = df.merge(units.rename("unit").reset_index(), how="outer")
        result["unit"] = result["unit"].combine_first(result["type"])
        return result.drop("type", axis=1)
    unit2_map = {
        "relation_activity": "ACT",
        "relation_total_capacity": "CAP",
        "relation_new_capacity": "CAP_NEW",
    }
    unit2_type = unit2_map[par]
    par_data_idx = get_parameter_indices()

    unit2_map = (
        classify_dim(
            get_dim_set(par_data_idx, dim_par_map[unit2_type]), dim_par_map[unit2_type]
        )
        .merge(unit_map[unit2_type].rename("unit").reset_index())
        .drop("type", axis=1)
        .rename(columns={"unit": "denominator"})
    )
    df = df.merge(units.rename("numerator").reset_index(), how="outer")
    df["numerator"] = df["numerator"].combine_first(df["type"])
    df = df.drop("type", axis=1)
    new = (
        df.merge(unit2_map, how="inner")
        .merge(div_look_up)
        .rename(columns={"result": "unit"})
        .drop(columns=["numerator", "denominator"])
    )
    return new


def get_unit_corrected_par_data(scen: "Scenario") -> dict[str, pd.DataFrame]:
    par_data_idx = get_parameter_indices()
    # retrieve all parameters and group by "unit family"
    ps = set(scen.par_list())
    tec_ps_groups = group_parameters(ps)
    tec_ps_groups = {p: k for k, v in tec_ps_groups.items() for p in v}

    corrected = {}
    for par, typ in tec_ps_groups.items():
        if par == "emission_factor":
            continue
        par_unit_dims = dim_par_map[typ]
        dim_set = get_dim_set(par_data_idx, par_unit_dims)
        df = overwrite_units(scen, par, tec_ps_groups[par], dim_set)
        if not df.empty:
            corrected[par] = df
    return corrected


if __name__ == "__main__":
    # DEMO
    import ixmp
    import message_ix

    mp = ixmp.Platform("<platform_name>")
    scen = message_ix.Scenario(mp, "<model_name>", "<scenario_name>")
    corrected = get_unit_corrected_par_data(scen)
    with scen.transact():
        for par, df in corrected.items():
            scen.add_par(par, df)
