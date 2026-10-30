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
    substrings = ["ref", "fur", "hp_", "solar", "dheat", "fc_", "gas_pro"]
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
    ]
    mass = df[df["relation"].isin(mass_rels)].assign(type="mass")
    dimensionless_rels = [
        "max_regional_recycling_steel",
        "maximum_recycling_aluminum",
        "meth_exp_tot",
        "minimum_recycling_aluminum",
        "NH3_trd_cap",
        "NFert_trd_cap",
        "minimum_recycling_steel",
    ]
    dimless = df[df["relation"].isin(dimensionless_rels)].assign(type="dimensionless")
    result = pd.concat([mass, dimless])
    return result


def classify_dim(df: pd.DataFrame, dim: list[str]) -> pd.DataFrame:
    """Classify dimension items as either energy or mass type."""
    if dim == ["technology", "mode"]:
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
        "CAP_cost": "USD_2005/kW",
        "CAP_NEW_cost": "USD_2005/kW/yr",
        "DEMAND": "GWyr/yr",
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
    }
    r1 = pd.DataFrame(["energy"], columns=["type"]).assign(**ene_units)
    r2 = pd.DataFrame(["mass"], columns=["type"]).assign(**mass_units)
    return pd.concat([r1, r2])


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
        return df.merge(units.rename("unit").reset_index()).drop("type", axis=1)
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
    new = (
        df.merge(units.rename("numerator").reset_index(), how="left")
        .fillna("-")
        .merge(unit2_map, how="left")
        .drop("type", axis=1)
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
