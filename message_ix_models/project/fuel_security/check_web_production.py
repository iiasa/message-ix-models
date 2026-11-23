# -*- coding: utf-8 -*-
"""
Check the IEA WEB production import used by rebase_extraction (no scenario needed)

    python check_web_production.py            # uses the cached WEB_PRODUCTION.csv if present
    python check_web_production.py --reimport # re-read the WORLDBIG files

Prints: labels found, year coverage, how much of WORLD the R12 mapping keeps,
R12 totals for 2020 and the latest years, and a few country spot checks.
"""
import argparse
import os

import pandas as pd

from message_ix_models.tools.bilateralize.historical_calibration import (
    WEB_PRODUCTION_PRODUCTS,
    import_iea_production,
    iea_production_by_region,
    setup_datapath,
)

PROJECT, CONFIG = "fuel_security", "config.yaml"
TJ_TO_GWA = 3.1712e-5
TJ_PER_BCM = 36_000  # approximate, for eyeballing against IEA bcm figures only

parser = argparse.ArgumentParser()
parser.add_argument("--reimport", action="store_true")
args = parser.parse_args()
pd.set_option("display.width", 200)

# 1. Raw import: labels and coverage
raw = import_iea_production(PROJECT, CONFIG, reimport=args.reimport)
print("== Raw rows:", len(raw))
print("Products found:", sorted(raw["PRODUCT"].unique()))
print("Years:", raw["YEAR"].min(), "-", raw["YEAR"].max())
print("Latest year by product:", raw.groupby("PRODUCT")["YEAR"].max().to_dict())
missing = [p for ps in WEB_PRODUCTION_PRODUCTS.values() for p in ps
           if p not in set(raw["PRODUCT"])]
print("Listed products with no rows (fine if it's the other code variant):", missing)

# 2. Coverage: mapped R12 total vs the WEB WORLD row
reg = iea_production_by_region("R12", PROJECT, CONFIG)
prod_to_fuel = {p: f for f, ps in WEB_PRODUCTION_PRODUCTS.items() for p in ps}
world = raw[raw["REGION"] == "WORLD"].assign(fuel=lambda d: d["PRODUCT"].map(prod_to_fuel))
world = world.groupby(["fuel", "YEAR"])["VALUE"].sum() * TJ_TO_GWA
mapped = reg.groupby(["fuel", "year"])["value"].sum()
cov = (mapped / world.rename_axis(["fuel", "year"])).unstack("fuel")
print("\n== Share of WEB WORLD kept by the R12 mapping (expect ~0.97-1.0):")
print(cov.loc[[y for y in [2015, 2020, 2022, 2023, 2024, 2025] if y in cov.index]].round(3))

# Countries in the WEB file that never mapped to a region (largest first)
data_paths = setup_datapath(project_name=PROJECT, config_name=CONFIG)
cw = pd.read_csv(os.path.join(data_paths["iea_web"], "country_iso3.csv"))
unmapped = raw[~raw["REGION"].isin(cw["REGION"])]
print("\n== Largest WEB labels with no ISO3 (aggregates are expected here):")
print((unmapped[unmapped["YEAR"] == 2020].groupby("REGION")["VALUE"].sum() * TJ_TO_GWA)
      .sort_values(ascending=False).head(15).round(0))

# 3. R12 totals (GWa)
print("\n== R12 production, GWa:")
print(reg[reg["year"].isin([2020, 2022, 2023, 2024, 2025])]
      .pivot_table(index="node", columns=["fuel", "year"], values="value").round(0))

# 4. Spot checks against known magnitudes (gas in bcm, approx.)
print("\n== Gas spot checks, bcm (approx. 36 PJ/bcm). IEA GMR: US 2024 ~1,060, Russia ~685")
spot = raw[raw["REGION"].isin(["USA", "UNITEDSTATES", "RUSSIA", "CANADA", "QATAR", "NORWAY"])
           & raw["PRODUCT"].isin(WEB_PRODUCTION_PRODUCTS["gas"])]
print((spot.pivot_table(index="REGION", columns="YEAR", values="VALUE") / TJ_PER_BCM)
      .iloc[:, -4:].round(0))
