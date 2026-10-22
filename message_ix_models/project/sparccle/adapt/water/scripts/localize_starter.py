"""Copy a starter scenario from a shared platform to its own local HSQLDB platform.

Usage: python localize_starter.py SOURCE_PLATFORM MODEL SCENARIO LOCAL_DB

Registers the platform sparccle_<MODEL>_<SCENARIO> at LOCAL_DB/<MODEL>/<SCENARIO>/db
in the user's ixmp config ($IXMP_DATA), copies the source platform's units and
regions, and clones the starter with its solution. Idempotent: an existing copy
is left alone.
"""

import argparse
import logging

import ixmp
from message_ix import Scenario

from message_ix_models.project.sparccle.workflow import (
    copy_platform_codelists,
    local_platform_name,
    register_local_platform,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("localize_starter")

p = argparse.ArgumentParser()
p.add_argument("source")
p.add_argument("model")
p.add_argument("scenario")
p.add_argument("local_db")
a = p.parse_args()

from pathlib import Path  # noqa: E402

name = local_platform_name(a.model, a.scenario)
path = Path(a.local_db).expanduser() / a.model / a.scenario
register_local_platform(name, path)
mp = ixmp.Platform(name=name)
existing = mp.scenario_list(default=False)
if ((existing["model"] == a.model) & (existing["scenario"] == a.scenario)).any():
    log.info(f"{a.model}/{a.scenario} already on {name} ({path}); nothing to do")
    mp.close_db()
    raise SystemExit(0)

mp_src = ixmp.Platform(name=a.source)
copy_platform_codelists(mp_src, mp)
src = Scenario(mp_src, a.model, a.scenario)
log.info(f"Copying {src.url} to {name} ({path})")
src.clone(platform=mp, keep_solution=True).set_as_default()
mp_src.close_db()
mp.close_db()
log.info("done")
