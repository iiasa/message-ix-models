import fnmatch
import logging
from typing import List, Optional

import message_ix
import pandas as pd
import pyam
import yaml
from genno.core.exceptions import MissingKeyError
from iam_units import registry
from message_ix.report import Reporter

from message_ix_models.util import broadcast, package_data_path

from .config import Config

LOG = logging.getLogger(__name__)


def ensure_historical_keys(rep: Reporter) -> None:
    """Ensure historical reporter keys exist.

    Creates 'out_hist' and 'emi_hist' keys if they don't already exist.

    - out_hist = output × historical_activity (for historical production data)
    - emi_hist = emission_factor × historical_activity (for historical emissions)
      Note: emi (model emissions) = emission_factor × ACT
            emi_hist (historical emissions) = emission_factor × historical_activity

    Only creates keys if the base keys are available in the reporter.
    """
    # Check what keys are available - need to check for keys with any dimension combo
    # Keys in reporter often have dimensions like "historical_activity:nl-t-ya-m"
    # So we need to check if the base key name exists (before the colon)
    has_output = any("output" == str(k).split(":")[0] for k in rep.keys())
    has_historical_activity = any(
        "historical_activity" == str(k).split(":")[0] for k in rep.keys()
    )
    has_emission_factor = any(
        "emission_factor" == str(k).split(":")[0] for k in rep.keys()
    )

    # Check if historical keys already exist
    has_out_hist = any("out_hist" == str(k).split(":")[0] for k in rep.keys())
    has_emi_hist = any("emi_hist" == str(k).split(":")[0] for k in rep.keys())

    # Absent base keys are a data condition (scenario without historical data) and
    # stay a warning; an unexpected failure of rep.add itself must propagate, or every
    # _hist family downstream silently degrades to the quiet missing-key path.
    if not has_out_hist and has_output and has_historical_activity:
        rep.add("out_hist", "mul", "output", "historical_activity")
    elif not has_out_hist:
        LOG.warning(
            "Cannot create out_hist: output=%s historical_activity=%s",
            has_output,
            has_historical_activity,
        )

    if not has_emi_hist and has_emission_factor and has_historical_activity:
        rep.add("emi_hist", "mul", "emission_factor", "historical_activity")
    elif not has_emi_hist:
        LOG.warning(
            "Cannot create emi_hist: emission_factor=%s historical_activity=%s",
            has_emission_factor,
            has_historical_activity,
        )


# Cache attribute name for storing first model year on Reporter
_FIRST_MODEL_YEAR_ATTR = "_h2_first_model_year"


def get_first_model_year(rep: Reporter) -> Optional[int]:
    """Return the first model year defined in cat_year.

    Raises
    ------
    ValueError
        If ``cat_year`` carries no ``firstmodelyear`` entry. Without it the
        historical/model year split is undefined; a silent ``None`` would
        disable :func:`_filter_years` and let ``out``/``out_hist`` rows for
        the same year collide downstream.
    """
    cached = getattr(rep, _FIRST_MODEL_YEAR_ATTR, None)
    if cached is not None:
        return cached
    df = rep.get("cat_year")
    rows = df.loc[df["type_year"] == "firstmodelyear", "year"]
    if rows.empty:
        raise ValueError(
            "cat_year has no 'firstmodelyear' entry; cannot split historical "
            "vs model years for reporting"
        )
    fm = int(rows.astype(int).min())
    setattr(rep, _FIRST_MODEL_YEAR_ATTR, fm)
    return fm


def pyam_df_from_rep(
    rep: message_ix.Reporter, reporter_var: str, mapping_df: pd.DataFrame
) -> pd.DataFrame:
    """Queries data from Reporter and maps to IAMC variable names.

    Parameters
    ----------
    rep
        message_ix.Reporter to query
    reporter_var
        Registered key of Reporter to query, e.g. "out", "in", "ACT", "emi", "CAP"
    mapping_df
        DataFrame mapping Reporter dimension values to IAMC variable names
    """
    filters_dict = {
        col: list(mapping_df.index.get_level_values(col).unique())
        for col in mapping_df.index.names
    }
    if reporter_var == "historical_activity":
        valid_dims = {"nl", "t", "ya", "m", "h"}
        filters_dict = {k: v for k, v in filters_dict.items() if k in valid_dims}
    rep.set_filters(**filters_dict)

    # Capacity/cost quantities are not dimensioned by m-c-l-e, and CAP_NEW / inv
    # exist only at the construction year (yv), which is reported as the year.
    key_suffixes = {
        "historical_activity": "nl-t-ya-m-h",
        "CAP": "nl-t-ya",
        "CAP_NEW": "nl-t-yv",
        "inv": "nl-t-yv",
    }
    key_suffix = key_suffixes.get(reporter_var, "nl-t-ya-m-c-l-e")

    # The only tolerated retrieval failure is a MISSING KEY on a *_hist variable:
    # ensure_historical_keys deliberately skips creating out_hist/emi_hist when the
    # scenario carries no historical_activity, and that absence is a data condition,
    # not an error. Everything else — ComputationError from inside the graph (the
    # dimensionless-'-' unit class that once silently dropped the whole ammonia
    # family), or a missing key on a non-hist variable — is a config/engine defect
    # and must abort the report instead of degrading to an empty family.
    # try/finally so a raise cannot leak this call's filters into the next family.
    try:
        try:
            df_var = pd.DataFrame(rep.get(f"{reporter_var}:{key_suffix}"))
            if reporter_var in ("CAP_NEW", "inv"):
                df_var = df_var.rename_axis(index={"yv": "ya"})
        except MissingKeyError:
            if not reporter_var.endswith("_hist"):
                raise
            LOG.debug(
                "Historical data missing for %s (requires historical_activity)",
                reporter_var,
            )
            return pd.DataFrame(
                columns=["value"],
                index=pd.MultiIndex.from_tuples(
                    [],
                    names=[
                        "nl",
                        "ya",
                        "iamc_name",
                        "original_unit",
                        "stoichiometric_factor",
                    ],
                ),
            )

        # Use join to merge data - this allows partial index matching
        # (e.g. emissions only need t,m but output needs t,m,c,l)
        df = (
            df_var.join(
                mapping_df[
                    ["iamc_name", "unit", "original_unit", "stoichiometric_factor"]
                ]
            )
            .dropna()
            .reset_index()
        )

        # If the reporter is looking at historical data, we need to filter
        # all the values that have yv == ya
        # This is because the historical_activity only has ya, while output
        # has also yv and ya. When cartesian product happens, it multiplies the
        # output over all yv for each ya. So we get the same activity multiple times
        # (however many times there are unique yv values per ya)
        if reporter_var in {"out_hist", "emi_hist"} and {"yv", "ya"}.issubset(
            df.columns
        ):
            df = df[df["yv"] == df["ya"]]

        dim_candidates = ["nl", "t", "ya", "m", "c", "l", "e", "h", "yv"]
        dim_cols = [col for col in dim_candidates if col in df.columns]
        group_cols = dim_cols + ["iamc_name", "original_unit", "stoichiometric_factor"]

        df = df.groupby(group_cols, dropna=False).sum(numeric_only=True)
        df.index.names = group_cols
        return df
    finally:
        rep.set_filters()


def _load_unit_conversions(domain: str = "hydrogen") -> dict:
    """Load unit conversion factors for a reporting domain.

    Uses ``data/<domain>/reporting/unit_conversions.yaml``. If that domain ships no
    conversions file of its own, the shared hydrogen table is used as the common
    fallback (``GWa->EJ/yr`` etc.) — and the fallback is **logged with the domain
    name** so it is never silent. A missing *domain directory* is caught earlier and
    loudly by :func:`fetch_variables`.

    Returns
    -------
    dict
        Dictionary mapping (source_unit, target_unit) tuples to conversion factors
    """
    path = package_data_path(domain, "reporting", "unit_conversions.yaml")
    if not path.exists():
        if domain != "hydrogen":
            LOG.info(
                "Reporting domain %r ships no unit_conversions.yaml; using the shared "
                "hydrogen conversions table.",
                domain,
            )
        path = package_data_path("hydrogen", "reporting", "unit_conversions.yaml")

    # A malformed table must propagate: degrading to {} would push every pair onto
    # the pint fallback and turn config corruption into wrong reported values.
    with open(path) as f:
        data = yaml.safe_load(f)

    # Convert the YAML format to the expected dictionary format
    conversions = {}
    for key, factor in (data or {}).get("conversions", {}).items():
        # Parse keys like "GWa_to_EJ/yr" into ("GWa", "EJ/yr")
        if "_to_" in key:
            source_unit, target_unit = key.split("_to_", 1)
            conversions[(source_unit, target_unit)] = factor

    return conversions


def convert_units_from_mapping(
    df: pd.DataFrame, target_unit: str, domain: str = "hydrogen"
) -> pd.DataFrame:
    """Convert units in DataFrame using iam_units.registry.

    Conversion uses the ``original_unit`` column. Stoichiometric factors (when
    present) are applied after unit conversion to translate output commodity
    totals (e.g., ammonia) into hydrogen content.

    Parameters
    ----------
    df : pd.DataFrame
        Contains ``original_unit`` and optionally ``stoichiometric_factor`` in
        the index plus a ``value`` column.
    target_unit : str
        Target unit to convert to

    Returns
    -------
    pd.DataFrame
        DataFrame with converted values
    """
    if "original_unit" not in df.index.names:
        # No unit conversion needed if original_unit is not in the index
        return df

    # Create a copy to avoid modifying the original
    df_converted = df.copy()

    # Get unique original units from the index
    original_units = df.reset_index()["original_unit"].unique()

    # Load conversion factors from YAML file
    yaml_conversions = _load_unit_conversions(domain)

    for orig_unit in original_units:
        if orig_unit == target_unit:
            # No conversion needed
            continue

        # Create mask for rows with this original unit
        mask = df.reset_index()["original_unit"] == orig_unit
        indices = df.reset_index()[mask].set_index(df.index.names).index

        # Get the values to convert
        values_to_convert = df.loc[indices, "value"].values

        # Check if we have a YAML-defined conversion first
        conversion_key = (orig_unit, target_unit)
        if conversion_key in yaml_conversions:
            factor = yaml_conversions[conversion_key]
            df_converted.loc[indices, "value"] = values_to_convert * factor
            continue

        # Try using iam_units.registry for conversion
        try:
            converted_quantity = registry.Quantity(values_to_convert, orig_unit).to(
                target_unit
            )
        except Exception as err:
            # Breadth is safe here because we ALWAYS re-raise (pint's error
            # taxonomy is unstable across versions). The former behavior —
            # warn and keep the raw values stamped with the target unit —
            # published numbers silently off by e.g. a heating value.
            affected = sorted(
                set(df.loc[indices].reset_index().get("iamc_name", pd.Series()))
            )
            raise ValueError(
                f"No unit conversion from {orig_unit!r} to {target_unit!r}: not in "
                f"unit_conversions.yaml (domain {domain!r}) and iam_units cannot "
                f"convert it. Affected variables: {affected}. Set original_unit "
                "explicitly or add a conversion entry."
            ) from err

        # Update the values in the DataFrame
        df_converted.loc[indices, "value"] = converted_quantity.magnitude

    # Apply stoichiometric factors if present (after unit conversion)
    # This converts commodity totals (e.g., ammonia in EJ) to hydrogen (EJ H2)
    if "stoichiometric_factor" in df_converted.index.names:
        df_reset = df_converted.reset_index()
        # Apply stoichiometric factor to each row
        df_reset["value"] = df_reset["value"] * df_reset["stoichiometric_factor"]
        df_converted = df_reset.set_index(df_converted.index.names)

    return df_converted


def format_reporting_df(
    df: pd.DataFrame,
    variable_prefix: str,
    model_name: str,
    scenario_name: str,
    unit: str,
    mappings,
    year_filter=None,
    domain: str = "hydrogen",
) -> pyam.IamDataFrame:
    """Formats a DataFrame created with :func:pyam_df_from_rep to pyam.IamDataFrame."""
    # If DataFrame is empty, return empty pyam.IamDataFrame immediately
    if df.empty:
        return pyam.IamDataFrame(
            pd.DataFrame(
                columns=[
                    "model",
                    "scenario",
                    "region",
                    "variable",
                    "unit",
                    "year",
                    "value",
                ]
            )
        )

    df.columns = ["value"]

    # Apply unit conversions using iam_units.registry
    df = convert_units_from_mapping(df, unit, domain=domain)

    # Prepare list of columns to drop
    cols_to_drop = ["original_unit"]
    if "stoichiometric_factor" in df.index.names:
        cols_to_drop.append("stoichiometric_factor")

    df = (
        df.reset_index()
        .rename(columns={"iamc_name": "variable", "nl": "region", "ya": "Year"})
        .assign(
            variable=lambda x: variable_prefix + x["variable"],
            Model=model_name,
            Scenario=scenario_name,
            Unit=unit,  # Set target unit
        )
        .drop(
            columns=cols_to_drop
        )  # Remove original_unit and stoichiometric_factor columns
    )

    extra_dims = [
        col for col in ["t", "m", "c", "l", "e", "h", "yv"] if col in df.columns
    ]
    if extra_dims:
        df = df.drop(columns=extra_dims)

    if year_filter is not None:
        df = df[df["Year"].apply(year_filter)]

    df = (
        df.groupby(
            ["Model", "Scenario", "region", "variable", "Year", "Unit"], dropna=False
        )
        .sum(numeric_only=True)
        .reset_index()
    )

    py_df = pyam.IamDataFrame(df)

    if py_df.empty:
        return py_df
    # Strip only the leading prefix to recover the bare iamc_name. Must be a prefix
    # strip, not str.replace (which also removes the prefix as an interior substring,
    # e.g. "in|" inside "...|CH2O_to_resin|..." -> "...|CH2O_to_res|...", which would
    # mis-flag a present variable as missing and zero-fill a duplicate).
    present = [v.removeprefix(variable_prefix) for v in py_df.variable]
    missing = [
        variable_prefix + name
        for name in mappings.iamc_name.unique().tolist()
        if name not in present
    ]
    if missing:
        zero_ts = pyam.IamDataFrame(
            pd.DataFrame()
            .assign(
                variable=missing,
                region=None,
                unit=unit,
                value=0,
                scenario=scenario_name,
                model=model_name,
                year=None,
            )
            .pipe(broadcast, region=py_df.region, year=py_df.year)
        )
        py_df = pyam.concat([py_df, zero_ts])
    return py_df


def _filter_years(
    py_df: pyam.IamDataFrame, first_model_year: Optional[int], is_hist: bool
) -> pyam.IamDataFrame:
    """Filter pyam data by year based on first model year."""
    if first_model_year is None or py_df is None or py_df.empty:
        return py_df
    df = py_df.as_pandas()
    if df.empty:
        return py_df
    if is_hist:
        df = df[df["year"] < first_model_year]
    else:
        df = df[df["year"] >= first_model_year]
    if df.empty:
        # df already carries the IAMC schema columns (model/scenario/region/
        # variable/unit/year/value) — current pyam requires the data= arg even
        # for an empty DataFrame; old pyam accepted IamDataFrame() with no args.
        return pyam.IamDataFrame(df)
    return pyam.IamDataFrame(df)


def compute_aggregates_from_iamc(
    df: pyam.IamDataFrame, aggregates: dict, iamc_prefix: str, short_to_iamc: dict
) -> pyam.IamDataFrame:
    """Compute aggregate variables by summing already-processed IAMC variables.

    This function aggregates variables at the IAMC level (after unit conversion and
    stoichiometric factor application), rather than re-querying raw MESSAGE data.
    This ensures that variables with different stoichiometric factors (e.g., methanol
    from different feedstocks) are correctly summed.

    Parameters
    ----------
    df : pyam.IamDataFrame
        DataFrame with computed leaf variables (after unit conversion and
        stoichiometric factor application)
    aggregates : dict
        Aggregate definitions from Config.get_aggregate_definitions()
        Structure: {level: {iamc_name: {"short": str, "components": list}}}
    iamc_prefix : str
        IAMC variable prefix (e.g., "Production|" or "Production|Hydrogen|")
    short_to_iamc : dict
        Mapping from short_name to IAMC variable name (fragment after prefix)

    Returns
    -------
    pyam.IamDataFrame
        Combined DataFrame with both leaf variables and computed aggregates

    Raises
    ------
    ValueError
        If an aggregate references a component short that is not defined in
        ``short_to_iamc`` or an earlier aggregation level (a config typo would
        otherwise shrink the sum silently), or if the matched component rows
        carry more than one unit.
    """
    if not aggregates:
        return df

    # Convert to pandas for easier manipulation
    df_work = df.as_pandas().copy()

    # Build reverse mapping: short_name -> full variable name
    short_to_full_var = {
        short: iamc_prefix + iamc_name for short, iamc_name in short_to_iamc.items()
    }

    # Process aggregates level by level to handle hierarchical aggregation
    for level_key in sorted(
        aggregates.keys()
    ):  # Process in order: level_1, level_2, etc.
        level_aggregates = aggregates[level_key]
        level_rows = []

        for iamc_name, agg_def in level_aggregates.items():
            components = agg_def["components"]
            short_name = agg_def["short"]

            # An unresolvable short is always a config bug: shorts come from the
            # YAML itself, so silently dropping one would shrink the aggregate to
            # a plausible-looking wrong number.
            missing = [s for s in components if s not in short_to_full_var]
            if missing:
                raise ValueError(
                    f"Aggregate {iamc_name!r}: unknown component short(s) {missing}. "
                    "Components must be shorts defined in the sibling config file "
                    "or an earlier aggregation level."
                )
            component_vars = [short_to_full_var[s] for s in components]

            # Filter dataframe for these component variables
            df_components = df_work[df_work["variable"].isin(component_vars)]

            # Data-side absence is a condition, not a config error: _hist
            # variants legitimately lose rows to year filtering.
            absent = sorted(set(component_vars) - set(df_components["variable"]))
            if absent:
                LOG.warning(
                    "Aggregate %r: component variable(s) %s carry no data rows",
                    iamc_name,
                    absent,
                )
            if df_components.empty:
                continue

            units = df_components["unit"].unique()
            if len(units) > 1:
                raise ValueError(
                    f"Aggregate {iamc_name!r}: components carry mixed units "
                    f"{sorted(units)}; an aggregate must be unit-homogeneous "
                    "(one target unit per config file)."
                )

            # Sum the components grouped by model, scenario, region, year, unit
            df_agg = (
                df_components.groupby(["model", "scenario", "region", "year", "unit"])
                .agg({"value": "sum"})
                .reset_index()
            )

            # Assign the aggregate variable name
            full_var_name = iamc_prefix + iamc_name
            df_agg["variable"] = full_var_name

            # Add to this level's collection
            level_rows.append(df_agg)

            # Register this aggregate for use in higher-level aggregates
            short_to_full_var[short_name] = full_var_name

        # Add this level's aggregates to df_work so they're available for next level
        if level_rows:
            df_work = pd.concat([df_work] + level_rows, ignore_index=True)

    # Return the combined dataframe with all leaves and aggregates
    return pyam.IamDataFrame(df_work)


def compute_global_aggregates(
    py_df: pyam.IamDataFrame, specs: List[dict]
) -> pyam.IamDataFrame:
    """Append cross-file aggregate variables summed from leaf variables.

    Unlike :func:`compute_aggregates_from_iamc`, which sums explicit component
    lists within one config file, this stage matches glob patterns against the
    variable universe of an assembled report, so one aggregate can span leaves
    from several domains and files.

    Parameters
    ----------
    py_df : pyam.IamDataFrame
        Assembled leaf variables (after unit conversion and per-file aggregation).
    specs : list of dict
        Aggregate definitions ``{"name": str, "patterns": [glob, ...], "unit": str}``.
        Patterns are shell-style globs over full variable names. Matching runs
        against a snapshot of the input, so an aggregate never absorbs another
        aggregate from the same call. A leaf matched by several patterns of one
        spec is summed once.

    Returns
    -------
    pyam.IamDataFrame
        Input data plus one aggregate variable per matching spec.

    Raises
    ------
    ValueError
        If a spec's patterns match no variable at all (a renamed or removed
        leaf orphaning the aggregate), or if the leaves matched by one spec do
        not all carry the spec's ``unit`` (e.g. an EJ/yr energy flow and an
        Mt/yr material flow under one glob).
    """
    data = py_df.data
    leaf_variables = data["variable"].unique()

    frames = []
    for spec in specs:
        matched = {
            v
            for pattern in spec["patterns"]
            for v in leaf_variables
            if fnmatch.fnmatchcase(v, pattern)
        }
        if not matched:
            # Global aggregates only run on full-workflow reports
            # (add_global_aggregates=False for partial-domain runs), so a
            # dead pattern is a config error: a renamed leaf would silently
            # orphan the aggregate while its stale value lives on upstream.
            raise ValueError(
                f"Global aggregate {spec['name']!r}: no variable matches its "
                f"patterns {spec['patterns']}. A leaf was renamed or removed; "
                "update the spec in aggregates_global.yaml."
            )
        sub = data[data["variable"].isin(matched)]
        units = sorted(set(sub["unit"]))
        if units != [spec["unit"]]:
            raise ValueError(
                f"Aggregate {spec['name']!r} declares unit {spec['unit']!r} but "
                f"its matched leaves carry {units}; components: {sorted(matched)}"
            )
        agg = sub.groupby(["model", "scenario", "region", "year"], as_index=False)[
            "value"
        ].sum()
        agg["variable"] = spec["name"]
        agg["unit"] = spec["unit"]
        frames.append(agg)

    if not frames:
        return py_df
    return pyam.IamDataFrame(pd.concat([data, *frames], ignore_index=True))


def check_region_purity(py_df: pyam.IamDataFrame, glb: str = "R12_GLB") -> None:
    """Raise if any variable carries both native ``glb`` rows and regional rows.

    The World step classifies variables binarily: those with any ``glb`` row are
    taken as GLB-native (bunkers, trade-pool techs) and kept as-is; all others
    get a ``glb`` row summed from their regional rows. A variable holding BOTH
    kinds of rows — typically a global aggregate whose patterns mix a GLB-native
    leaf into regional components — would keep its partial ``glb`` row as the
    World value, silently understated.

    A variable is treated as GLB-native only if it carries a NON-ZERO ``glb``
    row. An all-zero ``glb`` row is a zero-fill artifact, not a native total:
    :func:`format_reporting_df` broadcasts an idle (missing) leaf over every
    region present in its FILE, so when that file also holds a GLB-native sibling
    (e.g. a ``*_bunker`` leaf contributing an ``R12_GLB`` row), the idle regional
    leaf picks up a spurious all-zero ``R12_GLB`` row. Counting that artifact as
    native would falsely flag the idle leaf as mixed-basis (the #439 failure on
    baselines where a regional tech goes idle beside a GLB-native leaf). A real
    mixed aggregate — the case this guard defends — carries a NON-ZERO ``glb``
    row, so it still raises. The World step keeps the idle leaf's zero ``glb``
    row as-is (World = 0, correct for an idle tech), so values are unaffected.

    Raises
    ------
    ValueError
        Naming the mixed variables. Resolutions: drop the GLB-native pattern
        from the aggregate spec (report that piece as its own variable, e.g.
        under ``in|Shipping|all``), or aggregate after the World step (an
        engine extension, deliberately not built).
    """
    glb_df = py_df.filter(region=glb)
    if glb_df.empty:
        return
    glb_rows = glb_df.as_pandas()
    native = set(glb_rows.loc[glb_rows["value"] != 0, "variable"])
    regional = set(py_df.filter(region=glb, keep=False).variable)
    mixed = sorted(native & regional)
    if mixed:
        raise ValueError(
            f"Variables mix native {glb} rows with regional rows; the World "
            f"step cannot classify them: {mixed}. A global-aggregate spec "
            "likely matches a GLB-native leaf (e.g. in|Shipping|*) alongside "
            "regional ones. One aggregate = one region basis: remove the "
            "GLB-native pattern from the spec, or aggregate after the World "
            "step."
        )


def load_config(name: str, domain: str = "hydrogen") -> "Config":
    """Load a config for a given reporting variable category from the YAML files.

    This is a thin wrapper around :meth:`.Config.from_files`.
    """
    return Config.from_files(name, domain=domain)


def load_global_aggregate_specs() -> List[dict]:
    """Load the specs from :file:`data/reporting/aggregates_global.yaml`.

    The file lives outside the per-domain reporting directories so that leaf
    auto-discovery (:func:`fetch_variables`) never picks it up.
    """
    path = package_data_path("reporting", "aggregates_global.yaml")
    doc = yaml.safe_load(path.read_text()) or {}
    return list(doc.get("aggregates") or [])


def run_lh2_prod_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for liquefied hydrogen production."""
    var = "lh2_prod"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    first_model_year = get_first_model_year(rep)
    return _filter_years(py_df, first_model_year, is_hist=False)


def run_h2_prod_reporting(
    rep: Reporter, model_name: str, scen_name: str
) -> pyam.IamDataFrame:
    """Generate reporting for hydrogen production."""
    var = "h2_prod"
    config = load_config(var)
    df = pyam_df_from_rep(rep, config.var, config.mapping)
    py_df = format_reporting_df(
        df, config.iamc_prefix, model_name, scen_name, config.unit, config.mapping
    )
    first_model_year = get_first_model_year(rep)
    return _filter_years(py_df, first_model_year, is_hist=False)


def run_reporting(
    var: str, rep: Reporter, model_name: str, scen_name: str, domain: str = "hydrogen"
) -> pyam.IamDataFrame:
    """Generate reporting for any given variable.

    This function now computes leaf variables first (applying unit conversion and
    stoichiometric factors), then aggregates them at the IAMC level to handle
    cases where different components have different stoichiometric factors.
    """
    # Ensure historical reporter keys are available
    ensure_historical_keys(rep)

    config = load_config(var, domain=domain)

    # Get leaf variables only (config.mapping only contains leaves now)
    df = pyam_df_from_rep(rep, config.var, config.mapping)

    # Format and convert units/factors for leaf variables
    py_df = format_reporting_df(
        df,
        config.iamc_prefix,
        model_name,
        scen_name,
        config.unit,
        config.mapping,
        domain=domain,
    )

    first_model_year = get_first_model_year(rep)
    is_hist = var.endswith("_hist")
    py_df = _filter_years(py_df, first_model_year, is_hist)

    if py_df.empty:
        return py_df

    # Build mapping from short_name to iamc_name for aggregation
    short_to_iamc = (
        config.mapping.reset_index()[["short_name", "iamc_name"]]
        .drop_duplicates()
        .set_index("short_name")["iamc_name"]
        .to_dict()
    )

    # Compute aggregates from processed IAMC variables
    aggregates = config.get_aggregate_definitions()
    if aggregates:
        py_df = compute_aggregates_from_iamc(
            py_df, aggregates, config.iamc_prefix, short_to_iamc
        )

    return py_df


def fetch_variables(domain: str = "hydrogen") -> List[str]:
    """Fetch all reporting variable categories from ``data/<domain>/reporting``.

    Fails loudly (naming the domain) if the directory is missing or contains no
    variable YAMLs, so a misspelled or unbuilt reporting domain raises instead of
    silently producing zero variables.
    """
    from message_ix_models.util import package_data_path

    path = package_data_path(domain, "reporting")
    if not path.is_dir():
        raise FileNotFoundError(
            f"No reporting directory for domain {domain!r}: {path} does not exist"
        )
    variables = [f.stem for f in path.glob("*.yaml") if f.stem != "unit_conversions"]
    # we need to remove the _aggregates files
    variables = [var for var in variables if not var.endswith("_aggregates")]
    if not variables:
        raise FileNotFoundError(
            f"Reporting domain {domain!r} has no variable YAMLs in {path}"
        )
    return variables


def run_h2_reporting(
    rep: Reporter, model_name: str, scen_name: str, add_world: bool = True
) -> pyam.IamDataFrame:
    """Generate all hydrogen reporting variables for a given scenario.

    This includes:
    - Hydrogen production by technology and fuel type

    All variables include aggregated totals as defined in the reporting
    configuration files.

    Parameters
    ----------
    rep
        message_ix.Reporter to query
    model_name
        Name of the model
    scen_name
        Name of the scenario
    add_world
        If True, add World region as sum of all regions (default: True)

    Returns
    -------
    pyam.IamDataFrame
        Combined dataframe with all hydrogen reporting variables
    """
    return run_sectoral_reporting(
        rep, model_name, scen_name, domains=["hydrogen"], add_world=add_world
    )


def run_sectoral_reporting(
    rep: Reporter,
    model_name: str,
    scen_name: str,
    domains: Optional[List[str]] = None,
    add_world: bool = True,
    add_global_aggregates: bool = False,
) -> pyam.IamDataFrame:
    """Run reporting across one or more ``data/<domain>/reporting`` directories.

    Each domain's variable YAMLs are discovered and run through the same generic
    pipeline as the hydrogen reporting (the engine is commodity-agnostic; the sector
    lives in the data, not the code). Results are concatenated and an optional World
    (R12_GLB) total is appended.

    Parameters
    ----------
    domains
        Reporting domains to sweep, e.g. ``["hydrogen", "power"]``. Defaults to
        ``["hydrogen"]``. A missing/misspelled domain raises in
        :func:`fetch_variables` (no silent fallback).
    add_world
        If True, append a World total under the R12_GLB technical node ID.
    add_global_aggregates
        If True, run :func:`compute_global_aggregates` over the assembled leaves
        (specs from :func:`load_global_aggregate_specs`) before the World step, so
        World rolls the aggregates up too. Leave False for partial-domain runs:
        the global specs assume the full workflow domain set, and a subset of
        domains would emit silently partial totals.
    """
    if domains is None:
        domains = ["hydrogen"]

    # Historical keys are created once on the shared Reporter (idempotent).
    ensure_historical_keys(rep)

    # MESSAGE stores dimensionless coefficients with unit "-", which pint cannot
    # parse. genno tolerates it while a quantity spans mixed units (it discards
    # units), but a per-family filter that narrows to a uniform "-" (e.g. the
    # ammonia producers biomass_NH3/coal_NH3/... output NH3 at coeff 1.0) makes
    # data_for_quantity call parse_units("-") and raise — which pyam_df_from_rep
    # swallows, silently dropping the whole family. Normalize "-" to dimensionless
    # up front so those families report. Value-preserving: "-" IS dimensionless.
    rep.configure(units={"replace": {"-": ""}})

    dfs = [
        run_reporting(var, rep, model_name, scen_name, domain=domain)
        for domain in domains
        for var in fetch_variables(domain)
    ]
    py_df = pyam.concat(dfs)

    if add_global_aggregates:
        py_df = compute_global_aggregates(py_df, load_global_aggregate_specs())

    # Sum regional nodes → R12_GLB only for variables without native GLB rows
    # (chemicals bunkers already report at R12_GLB).
    if add_world:
        glb = "R12_GLB"
        check_region_purity(py_df, glb)
        native = set(py_df.filter(region=glb).variable)
        to_sum = py_df.filter(variable=list(native), keep=False).filter(
            region=glb, keep=False
        )
        if not to_sum.empty:
            py_df = pyam.concat(
                [py_df, to_sum.aggregate_region(to_sum.variable, region=glb)]
            )

    return py_df
