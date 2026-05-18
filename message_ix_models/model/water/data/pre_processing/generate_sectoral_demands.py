"""Generate R12 multi-SSP sectoral water demand files.

The generator writes 40 per-SSP CSVs into
``data/water/demands/harmonized/R12`` covering urban (domestic, combined),
rural, and manufacturing withdrawal and return for SSP1-SSP5.

Inputs:

- Sectoral basin-level withdrawal projections on pdrive at
  ``/mnt/p/ene.model/NEST/water_demands/Khan2022/withdrawals/annual/R12``
  (per-SSP per-sector ``ssp{N}_{Domestic_urban,Domestic_rural,Industry&Mining}
  _{value,total}_annual_ensemble_basin_level.csv``, km3/yr, 5-year cadence
  2010-2100).
- In-repo per-basin return/withdrawal ratio CSVs at
  ``data/water/demands/harmonized/R12/return_ratio_{urban_domestic,rural,
  manufacturing}.csv``. The same per-basin fractions apply to every SSP.

Transformation:

- Withdrawal: pdrive value x 1000 (km3/yr -> MCM/yr) at decadal years
  2010-2100. One CSV per sector x SSP.
- Combined urban withdrawal: domestic + manufacturing.
- Return: new withdrawal x shipped return ratio; zero where the ratio is
  undefined.
- Combined urban return: domestic + manufacturing.

Run with ``uv run --no-sync python -m`` and the module path
``message_ix_models.model.water.data.pre_processing.generate_sectoral_demands``.
"""

# TODO: move this module's docstring content to DOCS when the docs update lands.

from __future__ import annotations

from pathlib import Path

import pandas as pd

from message_ix_models.model.water.utils import KM3_TO_MCM
from message_ix_models.util import package_data_path

SOURCE_R12 = Path(
    "/mnt/p/ene.model/NEST/water_demands/Khan2022/withdrawals/annual/R12"
)

HARMONIZED_R12 = package_data_path("water", "demands", "harmonized", "R12")

DECADAL_YEARS: tuple[int, ...] = tuple(range(2010, 2110, 10))
SSPS: tuple[int, ...] = (1, 2, 3, 4, 5)

SOURCE_SECTOR_TO_VARIABLE: dict[str, str] = {
    "Domestic_urban_value_annual_ensemble_basin_level.csv": "urban_withdrawal_domestic",
    "Domestic_rural_value_annual_ensemble_basin_level.csv": "rural_withdrawal",
    "Industry&Mining_total_annual_ensemble_basin_level.csv": "manufacturing_withdrawal",
}

RETURN_PAIRS: dict[str, str] = {
    "urban_withdrawal_domestic": "urban_return_domestic",
    "rural_withdrawal": "rural_return",
    "manufacturing_withdrawal": "manufacturing_return",
}

RATIO_LABEL: dict[str, str] = {
    "urban_withdrawal_domestic": "urban_domestic",
    "rural_withdrawal": "rural",
    "manufacturing_withdrawal": "manufacturing",
}


def _shipped_path(ssp: int, variable: str) -> Path:
    return HARMONIZED_R12 / f"ssp{ssp}_regional_{variable}_baseline.csv"


def _ratio_path(withdrawal_var: str) -> Path:
    return HARMONIZED_R12 / f"return_ratio_{RATIO_LABEL[withdrawal_var]}.csv"


def _read_indexed(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, index_col=0)


def _load_withdrawal(ssp: int, source_file: str) -> pd.DataFrame:
    """Load source withdrawal, restrict to decadal years, convert to MCM."""
    raw = _read_indexed(SOURCE_R12 / f"ssp{ssp}_{source_file}")
    available = [year for year in DECADAL_YEARS if year in raw.index]
    if missing := [year for year in DECADAL_YEARS if year not in raw.index]:
        raise ValueError(
            f"ssp{ssp} {source_file}: missing decadal years {missing} "
            f"in source {SOURCE_R12}"
        )
    return raw.loc[list(available)] * KM3_TO_MCM


def _apply_return_ratio(
    withdrawal: pd.DataFrame, ratio: pd.DataFrame
) -> pd.DataFrame:
    """Multiply withdrawal x ratio, aligning to withdrawal columns.

    The shipped return CSVs use a blank index header (``,0|EEU,...``)
    distinct from the ``year,...`` header on withdrawal files. Clear the
    index name so the convention is preserved through ``to_csv``.
    """
    aligned = ratio.reindex(index=withdrawal.index, columns=withdrawal.columns)
    result = (withdrawal * aligned).fillna(0.0)
    result.index = result.index.set_names(None)
    return result


def _combine_urban(domestic: pd.DataFrame, manufacturing: pd.DataFrame) -> pd.DataFrame:
    """Sum domestic and manufacturing columns; align on the common basin set."""
    common = domestic.columns.intersection(manufacturing.columns)
    return domestic[common] + manufacturing[common]


def _write(df: pd.DataFrame, ssp: int, variable: str) -> Path:
    out = _shipped_path(ssp, variable)
    df.to_csv(out)
    return out


def build_ssp_outputs(
    ssp: int, ratios: dict[str, pd.DataFrame]
) -> dict[str, pd.DataFrame]:
    """Build the eight sectoral CSV frames for a single SSP."""
    outputs: dict[str, pd.DataFrame] = {}

    for source_file, withdrawal_var in SOURCE_SECTOR_TO_VARIABLE.items():
        outputs[withdrawal_var] = _load_withdrawal(ssp, source_file)

    for withdrawal_var, return_var in RETURN_PAIRS.items():
        outputs[return_var] = _apply_return_ratio(
            outputs[withdrawal_var], ratios[withdrawal_var]
        )

    outputs["urban_withdrawal"] = _combine_urban(
        outputs["urban_withdrawal_domestic"], outputs["manufacturing_withdrawal"]
    )
    outputs["urban_return"] = _combine_urban(
        outputs["urban_return_domestic"], outputs["manufacturing_return"]
    )

    return outputs


def main() -> None:
    if not SOURCE_R12.exists():
        raise FileNotFoundError(
            f"Source withdrawal directory not reachable at {SOURCE_R12}. "
            "Mount pdrive (likely /mnt/p) before running."
        )

    ratios = {var: _read_indexed(_ratio_path(var)) for var in RETURN_PAIRS}

    for ssp in SSPS:
        outputs = build_ssp_outputs(ssp, ratios)
        for variable, frame in outputs.items():
            path = _write(frame, ssp, variable)
            print(f"ssp{ssp}: wrote {path.name} ({frame.shape[0]}x{frame.shape[1]})")


if __name__ == "__main__":
    main()
