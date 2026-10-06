"""Map national emission targets to MESSAGE regional ``bound_emission``.

Workflow
--------
1. Read one time-slice of national targets from a CSV (at least ``ISO3``,
   ``Value`` in Mt CO₂eq).
2. Read regional baseline ``EMISS`` (``TCE``) for ``year_act`` from a solved
   scenario (**MtC**, not Mt CO₂).
3. Map ISO → R12 via :func:`~message_ix_models.model.structure.get_codelist`.
4. Split countries in each region into those **with** a target and those
   **without**:

   - *with*: sum national targets (Mt CO₂eq → MtC).
   - *without*: keep the PE-weighted share of the regional baseline TCE at
     ``year_act``.

   Regional target::

       target = sum(Value_with) + pe_share_without × baseline_TCE(year_act)

5. Write PE shares and audit CSVs under ``local_data/anchor/``.
6. **Red flag** if, in a region::

       sum(Value_with) > pe_share_with × baseline_TCE(year_act)

   (all in MtC).

CSV format
----------
Required columns: ``ISO3``, ``Value``. Optional: ``Country name``, ``Unit``.
One file = one policy year; pass that year as ``year_act``.

EU27
----
If the CSV contains an ``EU`` aggregate and blank member rows, the EU total is
allocated to members by PE weight (equal split if PE is missing).

Example
-------
::

    from message_ix_models.tools.anchor import map_ndc_targets

    result = map_ndc_targets(
        scenario,
        policy_file="20260917pbl_ndc_2030.csv",  # under message_data/data/anchor/
        year_act=2030,
    )
    with scenario.transact("add NDC bound_emission"):
        scenario.add_par("bound_emission", result["bound_emission"])
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from message_ix import make_df

from message_ix_models.tools.iea.iso_share import (
    load_iso_mapping,
    retrieve_energy_shares,
)
from message_ix_models.util import local_data_path, nodes_ex_world, private_data_path

if TYPE_CHECKING:
    from message_ix import Scenario

log = logging.getLogger(__name__)

#: Mt CO₂ → MtC (MESSAGE ``EMISS`` / ``TCE`` unit).
CO2_TO_C = 12.0 / 44.0

#: EU27 members (CSV may have one ``EU`` row; country rows blank).
EU27_ISO = [
    "AUT",
    "BEL",
    "BGR",
    "HRV",
    "CYP",
    "CZE",
    "DNK",
    "EST",
    "FIN",
    "FRA",
    "DEU",
    "GRC",
    "HUN",
    "IRL",
    "ITA",
    "LVA",
    "LTU",
    "LUX",
    "MLT",
    "NLD",
    "POL",
    "PRT",
    "ROU",
    "SVK",
    "SVN",
    "ESP",
    "SWE",
]

R12_ORDER = [
    "R12_AFR",
    "R12_CHN",
    "R12_EEU",
    "R12_FSU",
    "R12_LAM",
    "R12_MEA",
    "R12_NAM",
    "R12_PAO",
    "R12_PAS",
    "R12_RCPA",
    "R12_SAS",
    "R12_WEU",
]


def region_targets_to_bound_emission(
    region_df: pd.DataFrame,
    *,
    type_emission: str = "TCE",
    type_tec: str = "all",
    type_year: int | str | None = None,
    unit: str = "Mt C/yr",
    value_col: str = "target_mtc",
) -> pd.DataFrame:
    """Build a ``bound_emission`` parameter table from regional targets (MtC)."""
    if type_year is None:
        if "year_act" in region_df.columns and len(region_df):
            type_year = int(region_df["year_act"].iloc[0])
        else:
            type_year = 2030

    frames = []
    for _, row in region_df.iterrows():
        val = row[value_col]
        if pd.isna(val):
            continue
        frames.append(
            make_df(
                "bound_emission",
                node=str(row["region"]),
                type_emission=type_emission,
                type_tec=type_tec,
                type_year=type_year,
                value=float(val),
                unit=unit,
            )
        )
    if not frames:
        return make_df("bound_emission")
    return pd.concat(frames, ignore_index=True)


def load_policy_targets(path: Path | str) -> pd.DataFrame:
    """Load a national policy-target CSV for one time slice.

    Required columns: ``ISO3``, ``Value`` (Mt CO₂eq). Optional: ``Country name``,
    ``Unit``.

    Returns
    -------
    pandas.DataFrame
        Columns: ``country``, ``iso``, ``value_mtco2``.
    """
    path = Path(path)
    if path.suffix.lower() != ".csv":
        raise ValueError(f"policy_file must be a CSV, got {path.suffix!r}: {path}")
    if not path.is_file():
        # Bare filename → message_data/data/anchor/, then local_data/anchor/
        candidates = [
            private_data_path("anchor", path.name),
            local_data_path("anchor", path.name),
        ]
        for alt in candidates:
            if alt.is_file():
                path = alt
                break
        else:
            raise FileNotFoundError(
                f"{path} not found; looked in {[str(c) for c in candidates]}"
            )

    df = pd.read_csv(path, encoding="utf-8-sig")
    # Accept common header variants
    rename = {}
    for c in df.columns:
        key = c.strip().lower().replace(" ", "_")
        if key in {"iso3", "iso"}:
            rename[c] = "iso"
        elif key in {"value", "target", "ndc"}:
            rename[c] = "value_mtco2"
        elif key in {"country_name", "country", "name"}:
            rename[c] = "country"
    df = df.rename(columns=rename)

    need = {"iso", "value_mtco2"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(
            f"{path.name} missing required columns {sorted(missing)}; "
            f"have {list(df.columns)}. Need at least ISO3 and Value."
        )

    df["iso"] = df["iso"].astype(str).str.strip().replace({"nan": pd.NA})
    df["value_mtco2"] = pd.to_numeric(df["value_mtco2"], errors="coerce")
    if "country" not in df.columns:
        df["country"] = df["iso"]
    else:
        df["country"] = df["country"].astype(str)
    return df[["country", "iso", "value_mtco2"]].reset_index(drop=True)


def baseline_tce(
    scenario: "Scenario",
    year: int,
    emission: str = "TCE",
) -> pd.Series:
    """Return regional baseline ``EMISS`` for `year` in **MtC**."""
    if not scenario.has_solution():
        raise ValueError("scenario has no solution; solve the baseline first")

    df = scenario.var(
        "EMISS",
        filters={"emission": [emission], "year": [year], "type_tec": ["all"]},
    )
    if df.empty:
        df = scenario.var("EMISS", filters={"emission": [emission], "year": [year]})
        if df.empty:
            raise ValueError(f"No EMISS/{emission} data for year={year}")
        if "type_tec" in df.columns and (df["type_tec"] == "all").any():
            df = df.loc[df["type_tec"].eq("all")]
        elif "type_tec" in df.columns:
            df = (
                df.loc[~df["type_tec"].eq("all")]
                .groupby("node", as_index=False)["lvl"]
                .sum()
            )

    nodes = set(map(str, nodes_ex_world(df["node"].unique().tolist())))
    return (
        df.loc[df["node"].isin(nodes)]
        .groupby("node")["lvl"]
        .sum()
        .rename("baseline_tce_mtc")
    )


def _allocate_eu27(
    targets: pd.DataFrame,
    pe_weight: pd.Series,
) -> pd.DataFrame:
    """Replace an ``EU`` aggregate with member rows allocated by PE weight."""
    eu = targets.loc[targets["iso"].eq("EU")]
    rest = targets.loc[~targets["iso"].eq("EU")].copy()
    if eu.empty or pd.isna(eu.iloc[0]["value_mtco2"]):
        rest["eu27_allocated"] = False
        return rest

    total = float(eu.iloc[0]["value_mtco2"])
    weights = pe_weight.reindex(EU27_ISO).fillna(0.0)
    if weights.sum() <= 0:
        weights = pd.Series(1.0 / len(EU27_ISO), index=EU27_ISO)
    else:
        weights = weights / weights.sum()

    parts = []
    for iso, w in weights.items():
        name_rows = rest.loc[rest["iso"].eq(iso), "country"]
        name = name_rows.iloc[0] if len(name_rows) else iso
        parts.append(
            {
                "country": name,
                "iso": iso,
                "value_mtco2": total * float(w),
                "eu27_allocated": True,
            }
        )

    rest = rest.loc[~rest["iso"].isin(EU27_ISO)].copy()
    rest["eu27_allocated"] = False
    return pd.concat([rest, pd.DataFrame(parts)], ignore_index=True)


def _pe_share_table(
    pe_shares: pd.DataFrame | Path | str | None,
    region_id: str,
) -> pd.DataFrame:
    """Return PE share table with columns ``iso``, ``region``, ``pe_weight``."""
    if pe_shares is None:
        log.info("Retrieving IEA PE country-in-region weights via iso_share…")
        df = retrieve_energy_shares("Pe", region_id=region_id)
    elif isinstance(pe_shares, (str, Path)):
        df = pd.read_csv(pe_shares)
    else:
        df = pe_shares.copy()

    if "country_in_region_weight" in df.columns:
        df = df.rename(columns={"country_in_region_weight": "pe_weight"})
    if "iso" not in df.columns and df.index.name == "iso":
        df = df.reset_index()
    need = {"iso", "region", "pe_weight"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"pe_shares missing columns: {sorted(missing)}")
    return df[["iso", "region", "pe_weight"]].copy()


def map_ndc_targets(
    scenario: "Scenario",
    policy_file: Path | str,
    year_act: int,
    *,
    region_id: str = "R12",
    pe_shares: pd.DataFrame | Path | str | None = None,
    debug_dir: Path | str | None = None,
    type_emission: str = "TCE",
    type_tec: str = "all",
    unit: str = "Mt C/yr",
) -> dict[str, pd.DataFrame]:
    """Build R12 targets from a national policy CSV + baseline TCE + PE shares.

    Parameters
    ----------
    scenario :
        Solved **baseline** scenario (``EMISS`` / ``TCE`` for `year_act`, MtC).
    policy_file :
        CSV with at least ``ISO3`` and ``Value`` (Mt CO₂eq). One file = one
        time slice. Bare filenames are resolved under
        :file:`message_data/data/anchor/` (then ``local_data/anchor/``).
    year_act :
        Policy / bound year (also the baseline ``EMISS`` year and
        ``bound_emission`` ``type_year``).
    region_id :
        Node codelist (default ``R12``).
    pe_shares :
        Optional PE share table or CSV from
        :func:`~message_ix_models.tools.iea.iso_share.retrieve_energy_shares`.
        If :obj:`None`, shares are retrieved from the IEA DB.
    debug_dir :
        Debug CSV directory (default ``local_data/anchor/``).
    type_emission, type_tec, unit :
        Keys for the returned ``bound_emission`` table (values in MtC).

    Returns
    -------
    dict
        ``region``, ``bound_emission``, ``country``, ``pe_shares``.
    """
    debug_dir = Path(debug_dir) if debug_dir else local_data_path("anchor")
    debug_dir.mkdir(parents=True, exist_ok=True)

    mapping = load_iso_mapping(region_id).reset_index()
    iso_to_r12 = dict(zip(mapping["iso"], mapping["region"]))

    pe = _pe_share_table(pe_shares, region_id)
    pe_path = debug_dir / f"_debug_ndc_pe_shares_{year_act}.csv"
    pe.to_csv(pe_path, index=False)
    log.info("Wrote PE shares → %s", pe_path)

    pe_weight = pe.set_index("iso")["pe_weight"]
    targets = _allocate_eu27(load_policy_targets(policy_file), pe_weight)
    targets["region"] = targets["iso"].map(iso_to_r12)

    baseline = baseline_tce(scenario, year=year_act)

    countries = mapping.merge(pe[["iso", "pe_weight"]], on="iso", how="left")
    countries["pe_weight"] = countries["pe_weight"].fillna(0.0)

    t_cols = targets[["iso", "value_mtco2", "eu27_allocated"]].copy()
    countries = countries.merge(t_cols, on="iso", how="left")
    countries["eu27_allocated"] = countries["eu27_allocated"].fillna(False)

    extra = targets.loc[
        targets["region"].notna() & ~targets["iso"].isin(countries["iso"]),
        ["iso", "country", "region", "value_mtco2", "eu27_allocated"],
    ].copy()
    if not extra.empty:
        extra["pe_weight"] = 0.0
        countries = pd.concat([countries, extra], ignore_index=True)

    countries["has_target"] = countries["value_mtco2"].notna()
    countries["value_mtc"] = countries["value_mtco2"] * CO2_TO_C
    countries["baseline_tce_mtc"] = countries["region"].map(baseline)
    countries["year_act"] = year_act

    unmapped = targets.loc[targets["region"].isna() & targets["iso"].notna()]
    if not unmapped.empty:
        log.warning(
            "%d policy rows have no R12 mapping (skipped): %s",
            len(unmapped),
            ", ".join(unmapped["iso"].astype(str).head(20)),
        )

    countries = countries.dropna(subset=["region"])
    rows = []
    for region, g in countries.groupby("region"):
        bl = float(baseline.get(region, float("nan")))
        with_t = g.loc[g["has_target"]]
        without = g.loc[~g["has_target"]]

        w_sum = g["pe_weight"].sum()
        if w_sum > 0:
            share_with = float(with_t["pe_weight"].sum() / w_sum)
            share_without = float(without["pe_weight"].sum() / w_sum)
        else:
            share_with = share_without = float("nan")

        bottomup_mtc = (
            float(with_t["value_mtc"].sum(min_count=1)) if len(with_t) else 0.0
        )
        if pd.isna(bottomup_mtc):
            bottomup_mtc = 0.0

        implied_with_baseline = share_with * bl if pd.notna(bl) else float("nan")
        without_baseline = share_without * bl if pd.notna(bl) else float("nan")
        target_mtc = bottomup_mtc + (
            without_baseline if pd.notna(without_baseline) else 0.0
        )

        red_flag = bool(
            pd.notna(implied_with_baseline) and bottomup_mtc > implied_with_baseline
        )
        if red_flag:
            log.error(
                "RED FLAG %s: bottom-up %.4g MtC > pe_share_with×baseline "
                "%.4g×%.4g = %.4g MtC",
                region,
                bottomup_mtc,
                share_with,
                bl,
                implied_with_baseline,
            )

        rows.append(
            {
                "region": region,
                "year_act": year_act,
                "baseline_tce_mtc": bl,
                "n_with_target": int(with_t["iso"].nunique()),
                "n_without_target": int(without["iso"].nunique()),
                "pe_share_with": share_with,
                "pe_share_without": share_without,
                "bottomup_mtc": bottomup_mtc,
                "bottomup_mtco2": bottomup_mtc / CO2_TO_C if bottomup_mtc else 0.0,
                "without_as_baseline_mtc": without_baseline,
                "implied_with_baseline_mtc": implied_with_baseline,
                "target_mtc": target_mtc,
                "target_mtco2": (
                    target_mtc / CO2_TO_C if pd.notna(target_mtc) else float("nan")
                ),
                "red_flag": red_flag,
            }
        )

    region_df = pd.DataFrame(rows)
    region_df["region"] = pd.Categorical(
        region_df["region"], categories=R12_ORDER, ordered=True
    )
    region_df = region_df.sort_values("region").reset_index(drop=True)

    country_path = debug_dir / f"_debug_ndc_country_audit_{year_act}.csv"
    region_path = debug_dir / f"_debug_ndc_region_targets_{year_act}.csv"
    bound_path = debug_dir / f"_debug_ndc_bound_emission_{year_act}.csv"
    countries.sort_values(
        ["region", "has_target", "iso"], ascending=[True, False, True]
    ).to_csv(country_path, index=False)
    region_df.to_csv(region_path, index=False)

    bound_emission = region_targets_to_bound_emission(
        region_df,
        type_emission=type_emission,
        type_tec=type_tec,
        type_year=year_act,
        unit=unit,
    )
    bound_emission.to_csv(bound_path, index=False)

    log.info("Wrote country audit → %s", country_path)
    log.info("Wrote region targets → %s", region_path)
    log.info("Wrote bound_emission → %s", bound_path)

    n_flag = int(region_df["red_flag"].sum())
    if n_flag:
        log.error(
            "%d / %d regions raised a red flag (see %s)",
            n_flag,
            len(region_df),
            region_path,
        )
    else:
        log.info("No red flags: bottom-up ≤ pe_share_with × baseline for all regions")

    return {
        "region": region_df,
        "bound_emission": bound_emission,
        "country": countries,
        "pe_shares": pe,
    }
