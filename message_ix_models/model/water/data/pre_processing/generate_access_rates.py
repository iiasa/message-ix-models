"""Populate R12 x SSP x (urban, rural) drinking-water access rates.

Reads ``Improved water services`` rows from two source files under
``data/water/demands/drinking_water_access``:

- ``projections_people_UR_income_10_25.csv``: with urban/rural split, used
  for AFR, EEU, FSU, LAM, MEA, PAS, RCPA, SAS, WEU.
- ``projections_people_merge_countries_10_25(in).csv``: no split, used for
  NAM and CHN with the same rate assigned to both settings.

PAO is hard-coded to 0.99 for both urban and rural.

Source values are population-weighted to R12 regions, carried backward to fill
early target years, and capped forward at the 2090 value for 2100/2110. The R12
rate is then broadcast uniformly to every basin column in that region; the
basin set and column order come from ``connection_rate_basins_R12.csv``.
"""

# TODO: move this module's docstring content to DOCS when the docs update lands.

import pandas as pd
import yaml

from message_ix_models.util import package_data_path

HARMONIZED = package_data_path("water", "demands", "harmonized", "R12")
DRINKING_WATER_ACCESS = package_data_path("water", "demands", "drinking_water_access")
NODE_YAML = package_data_path("node", "R12.yaml")
BASINS_FILE = HARMONIZED / "connection_rate_basins_R12.csv"

VARIABLE = "Improved water services"
SSPS = [1, 2, 3, 4, 5]
SSP_RCP = {1: "rcp26", 2: "rcp60", 3: "rcp60", 4: "rcp60", 5: "rcp60"}
TARGET_YEARS = [2010, 2020, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110]

FILE2_REGIONS = ["AFR", "EEU", "FSU", "LAM", "MEA", "PAS", "RCPA", "SAS", "WEU"]
FILE1_REGIONS = ["NAM", "CHN"]
PAO_OVERRIDE = {"urban": 0.99, "rural": 0.99}


def load_iso3_to_r12() -> dict[str, str]:
    """Map ISO-3 code to R12 region suffix (e.g. 'CAN' -> 'NAM')."""
    with NODE_YAML.open() as f:
        y = yaml.safe_load(f)
    mapping: dict[str, str] = {}
    for key, value in y.items():
        if not (isinstance(value, dict) and key.startswith("R12_")):
            continue
        region = key.replace("R12_", "")
        for iso in value.get("child", []):
            mapping[iso] = region
    return mapping


def load_source(filename: str) -> pd.DataFrame:
    df = pd.read_csv(DRINKING_WATER_ACCESS / filename)
    return df[df["variable"] == VARIABLE].copy()


def weighted_rates(
    df: pd.DataFrame,
    iso2reg: dict[str, str],
    regions: list[str],
    group_cols: list[str],
    numerator: str,
    denominator: str,
) -> pd.DataFrame:
    """Return population-weighted rates by SSP/RCP/year/region."""
    df = df.assign(region=df["iso3"].map(iso2reg))
    agg = (
        df[df["region"].isin(regions)]
        .groupby(["SSP", "RCP", "year", "region", *group_cols])
        .agg(num=(numerator, "sum"), den=(denominator, "sum"))
        .reset_index()
    )
    agg["rate"] = (agg["num"] / agg["den"]).clip(0, 1)
    return agg.rename(columns={"tot_ur": "setting"})


def file2_regional_rates(df: pd.DataFrame, iso2reg: dict[str, str]) -> pd.DataFrame:
    """Pop-weighted urban/rural rate per (SSP, year, R12) from file 2."""
    return weighted_rates(
        df,
        iso2reg,
        FILE2_REGIONS,
        group_cols=["tot_ur"],
        numerator="pop_imp_acc_ur_inc",
        denominator="pop_ur_inc",
    )[["SSP", "RCP", "year", "region", "setting", "rate"]]


def file1_regional_rates(df: pd.DataFrame, iso2reg: dict[str, str]) -> pd.DataFrame:
    """Pop-weighted total rate per (SSP, year, R12) from file 1."""
    return weighted_rates(
        df,
        iso2reg,
        FILE1_REGIONS,
        group_cols=[],
        numerator="pop_acc",
        denominator="pop",
    )[["SSP", "RCP", "year", "region", "rate"]]


def align_to_target_years(wide: pd.DataFrame) -> pd.DataFrame:
    """Carry source values onto the target decadal grid."""
    if wide.empty:
        raise ValueError("no source data to align")

    source_years = wide.dropna(how="all").index
    decadal_cap = max(y for y in source_years if y % 10 == 0)
    all_years = sorted(set(source_years) | set(TARGET_YEARS))

    out = wide.reindex(all_years).ffill().bfill().loc[TARGET_YEARS]
    if later_years := [y for y in TARGET_YEARS if y > decadal_cap]:
        out.loc[later_years] = wide.loc[decadal_cap].to_numpy()
    return out.astype(float)


def region_rate_table(
    regional_long: pd.DataFrame,
    ssp: int,
    setting: str | None = None,
) -> pd.DataFrame:
    """Build target-year x region wide table for a given SSP."""
    mask = (regional_long["SSP"] == f"SSP{ssp}") & (
        regional_long["RCP"] == SSP_RCP[ssp]
    )
    if setting is not None:
        mask &= regional_long["setting"] == setting

    return align_to_target_years(
        regional_long[mask].pivot(index="year", columns="region", values="rate")
    )


def basin_columns() -> list[str]:
    """Canonical basin column order for connection-rate outputs."""
    return list(pd.read_csv(BASINS_FILE)["BCU_name"])


def broadcast_region_to_basins(
    region_rates: pd.DataFrame, columns: list[str]
) -> pd.DataFrame:
    """For each column `<basin_id>|<REGION>`, pick that region's rate."""
    regions = pd.Index(col.split("|")[-1] for col in columns)
    missing = regions.difference(region_rates.columns)
    if not missing.empty:
        raise KeyError(
            f"regions {list(missing)!r} not in rate table "
            f"{list(region_rates.columns)!r}"
        )

    out = region_rates.loc[:, regions].copy()
    out.columns = columns
    out.index.name = ""
    return out


def build_ssp_setting_csv(
    ssp: int,
    setting: str,
    file2_long: pd.DataFrame,
    file1_long: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Assemble the basin-wide table for one (ssp, setting) output."""
    file2_rates = region_rate_table(file2_long, ssp, setting=setting)
    file1_rates = region_rate_table(file1_long, ssp)

    region_rates = pd.concat(
        [
            file2_rates[FILE2_REGIONS],
            file1_rates[FILE1_REGIONS],
            pd.DataFrame({"PAO": PAO_OVERRIDE[setting]}, index=TARGET_YEARS),
        ],
        axis=1,
    )
    return broadcast_region_to_basins(region_rates, columns)


def main() -> None:
    iso2reg = load_iso3_to_r12()

    file2_long = file2_regional_rates(
        load_source("projections_people_UR_income_10_25.csv"), iso2reg
    )
    file1_long = file1_regional_rates(
        load_source("projections_people_merge_countries_10_25(in).csv"), iso2reg
    )
    columns = basin_columns()

    for ssp in SSPS:
        for setting in ("urban", "rural"):
            table = build_ssp_setting_csv(ssp, setting, file2_long, file1_long, columns)
            out = (
                HARMONIZED / f"ssp{ssp}_regional_{setting}_connection_rate_baseline.csv"
            )
            table.to_csv(out)
            print(
                f"ssp{ssp} {setting}: wrote {out.name} "
                f"(2020 mean={table.loc[2020].mean():.3f}, "
                f"2090 mean={table.loc[2090].mean():.3f})"
            )


if __name__ == "__main__":
    main()
