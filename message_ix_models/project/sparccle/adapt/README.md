# SPARCCLE adaptation scenarios (`sparccle.adapt`)

Power-sector adaptation and maladaptation scenarios on top of the SPARCCLE
cooling + power-plant climate-impact scenario (`..._cooling_CI_p`, see
`project/sparccle`). All scenarios are clones of that solved base on a local
HSQLDB platform; the build only adds constraints.

| key | what it adds | from |
|---|---|---|
| `BAU` | SSP2 cooling-share limits made live: `cat_tec` + `map_shares_commodity_*` for the four `share_cooling_*` shares (the water module stores `share_commodity_up` but, behind a FIXME, not the mappings) | 2050 (as in `ssp.yaml`) |
| `malad35` | no new once-through cooling capacity (`*__ot_fresh`, `*__ot_saline`) in R12_WEU, R12_EEU, R12_NAM only (EU IED / US 316(b) regulation) | 2035 |
| `adapt35` | no new once-through cooling anywhere; no new coal, gas, oil power-plant capacity (incl. CCS variants, not heat plants); near-zero activity growth for biomass and nuclear power; the base's activity-decline floors on the banned plants removed | 2035 |
| `adapt35h40` | as `adapt35`, half effect in 2035 (new-capacity bound = ½ base `CAP_NEW`, growth rates halved), full from 2040 | 2035 / 2040 |
| `aironly35` | power sector free; no new once-through *or* closed-loop cooling anywhere (`cooling_ban_types`), so new plants use dry cooling only; SSP share caps off (their air ≤ 50 % cap would contradict the rule) | 2035 |
| `nofresh35` | power sector free; no new *freshwater* cooling (once-through or closed-loop); seawater once-through and dry cooling stay available; SSP share caps off | 2035 |
| `sh` | the water module's seawater cap (10,000 MCM/yr in every region) replaced by the last historical year's saline withdrawal, 1:1 (`saline_hist_bound`, see below); corrected base | all years |
| `BAUsh`, `malad35sh`, `nofresh35sh`, `adapt35sh`, `adapt35h40sh` | the scenario of the same name on top of `sh` | as above |

Technologies, nodes, growth rates and years live in `config.yaml`; the logic in
`workflow.py` (pure functions `cooling_share_sets`, `no_new_capacity`,
`near_zero_growth`, `decline_floor_rows`, covered by
`tests/project/test_sparccle_adapt.py`).

## Why the decline floors go

The SSP2 base carries `growth_activity_lo` (e.g. −5 %/yr on `gas_cc`, `coal_ppl`)
from 2035 on. With no new capacity the fleet retires (2020 vintages by 2055), so
activity must reach zero, and the floor chains back year by year to 2035, where
historical activity fixes a positive minimum: CPLEX presolve reports
`ACTIVITY_CONSTRAINT_LO(R12_LAM,gas_cc,2035,year): 0 >= 28.1`. `drop_decline_floor`
removes `growth_activity_lo` / `initial_activity_lo` for the banned plants from
the first affected year; upper growth constraints and soft relaxations stay.

## Running (UniCC)

```sh
# build one scenario (clone + constrain; no solve)
mix-models sparccle_adapt run --config message_ix_models/project/sparccle/adapt/config.yaml adapt35 --go

# build, solve (MESSAGE, CPLEX lpmethod=4) and write cooling detail, serially
cd ~/job/out && sbatch <repo>/message_ix_models/project/sparccle/adapt/scripts/run_sparccle_adapt.sh BAU malad35 adapt35 adapt35h40

# re-solve one scenario with extra CPLEX options
SOLVE_OPTS="--solve-option scaind=1 --solve-option numericalemphasis=1" \
  sbatch .../scripts/run_solve_local.sh <platform> <model> <scenario>

# legacy IAMC reporting (message_data, branch ssp_sparccle)
sbatch -J report_adapt35 .../scripts/run_report_pci.sh <platform> <model> <scenario>
```

`scripts/` contents: `run_sparccle_adapt.sh` (driver), `solve_local.py` /
`run_solve_local.sh` (solve + `set_as_default`), `cooling_detail.py` (capacity,
activity, water inputs and applied impact parameters per plant × cooling type →
`cooling_grid.csv`, `cooling_mix.csv`), `report_sparccle_pci.py` / `run_report_pci.sh`
(IAMC xlsx). Scripts take `MSG_ENV`, `IXMP_DATA`, `COOLING_OUT`, `SOLVE_OPTS` from
the environment; defaults match the UniCC setup (`~/env/msg_pci`,
`~/.local/share/ixmp_pci`).

The HSQLDB file platform admits one process: scenarios of one platform build,
solve and report serially. For parallel reporting copy the `db.*` files and
register each copy as its own platform (`ixmp platform add`).

## Known behaviour

- `malad35` solves to the optimum but CPLEX may flag "optimal with unscaled
  infeasibilities" (1 row, ~1e-6), which ixmp rejects; `scaind=1
  numericalemphasis=1` gives a clean optimum with the same objective. `aironly35`
  needed scaling off instead (`scaind=-1 numericalemphasis=1`). Pass either via
  `SOLVE_OPTS` to `run_solve_local.sh`.
- The split between air and closed-loop cooling is weakly determined (near-equal
  cost): the base solution has no air cooling, every re-solved clone has 7–13 %
  at the same objective. Compare once-through vs recirculating, not air vs wet.
- Seawater once-through cooling is capped by the water module, not by this
  package: `extract_salinewater_cool` carries `bound_activity_up` from
  `_add_saline_bounds` in `water/data/water_for_ppl.py`. When the base was built
  that function compared historical activity (GWa of rejected heat) with the
  `default_saline_bound` of 10,000 MCM/yr, so the default won in every region and
  binds in the base from 2035, below the 2030 withdrawal in seven regions (CHN
  85,000 MCM, PAS 30,000, NAM 25,000, PAO 21,000, WEU 17,000, MEA 16,000, SAS
  15,000). The function now converts history to MCM via the `saline_ppl` input
  and bounds at the last historical year's withdrawal, one to one: no region
  withdraws more seawater for cooling than it does today, while retired coastal
  sites can be reused because the bound is on water, not on plants. The `sh`
  family applies that corrected bound on clones of the old base
  (`saline_hist_bound`), so the base itself need not be rebuilt; `nofresh35` is
  dry cooling plus the old fixed saline budget.
- Decisions: "EU" = R12_WEU + R12_EEU; once-through includes saline intakes;
  `soft_activity_up` (3–5 %) is left, so frozen technologies can still creep at a
  cost; `growth_new_capacity_up` is untouched.

Results and a comparison report are kept outside the package (`results/`, not
tracked).
