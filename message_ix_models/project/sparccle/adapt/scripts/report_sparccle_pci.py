"""Legacy IAMC reporting (message_data.tools.post_processing) for a scenario on a
local sparccle HSQLDB platform.

Usage:
  python report_sparccle_pci.py PLATFORM MODEL SCENARIO [--run-config NAME.yaml]
                                [--version N] [--no-merge-ts] [--out-dir DIR]

Defaults: run_config=default_run_config.yaml (message_data/data/report), merge_hist
and merge_ts on, xlsx written to ~/message_data/reporting_output/.
"""

import argparse
import logging

import ixmp
import message_ix

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("report_sparccle_pci")

p = argparse.ArgumentParser()
p.add_argument("platform")
p.add_argument("model")
p.add_argument("scenario")
p.add_argument("--version", type=int, default=None)
p.add_argument("--run-config", default="default_run_config.yaml")
p.add_argument("--no-merge-ts", action="store_true")
p.add_argument("--out-dir", default=None)
a = p.parse_args()

from message_data.tools.post_processing.iamc_report_hackathon import (  # noqa: E402
    report,
)

mp = ixmp.Platform(a.platform)
scen = message_ix.Scenario(mp, a.model, a.scenario, version=a.version)
log.info(f"Reporting {scen.url} (has_solution={scen.has_solution()})")
report(
    mp=mp,
    scen=scen,
    merge_hist=True,
    merge_ts=not a.no_merge_ts,
    run_config=a.run_config,
    out_dir=a.out_dir,
)
log.info("Reporting done")
mp.close_db()
