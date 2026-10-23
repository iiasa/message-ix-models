# SPARCCLE water adaptation scenarios (`sparccle.adapt.water`)

Water-sector BAU / maladaptation / +adaptation scenarios on a MESSAGEix-Nexus
build of the SPARCCLE starters (`SSP_SSPx_v6.6_sp / NPiREF_GDP_CI_50_ensemble_2`,
SSP2 and SSP3), RCP 7p0 hydrology. Each starter is copied once to its own
HSQLDB platform (`sparccle_<model>_<scenario>`, `~/sparccle/db/<model>/<scenario>`);
every scenario lives there.

| key | scenario | made by | levers |
|---|---|---|---|
| `base` | `CurrentPolicies_SSPx_W_base` | `water-ix nexus --sdgs baseline` on the starter (solved by the build) | none |
| `BAUfgd` | `CurrentPolicies_SSPx_W_BAUfgd` | clone of `base` | the BAU of record: environmental-flow lower bounds on `return_flow`; renewable-water growth cap 2 %/yr (SSP2) or 3 %/yr (SSP3); `var_cost` 0.4 USD/m³ on `extract_gw_fossil` (above levelised membrane desalination ~0.34); desalination freed (2025 `historical_new_capacity` copied to 2030, 10 %/yr `growth_new_capacity_up` removed); efficient distribution mode `Mf` bounded to zero. The 10-21 `BAU`/`BAUfg` runs (no pricing / no desalination fix, no `Mf` ban) were stepping stones |
| `maladapt` | `CurrentPolicies_SSPx_W_maladapt` | clone of `base` | same as `BAUfgd` (e-flows, growth cap, fossil groundwater priced, desalination freed, `Mf` banned) |
| `adapt` | `CurrentPolicies_SSPx_W_+adapt` | `water-ix nexus --sdgs SDG` on the starter, then growth cap and fossil groundwater priced; desalination left as the module builds it (no `desal_free`) | SDG package: `Mf`-only distribution with mid-tier costs, treatment targets (urban 0.95, rural 0.8), connection targets, SDG6 recycling, e-flow bounds |

Starters, hydrology inputs, naming and levers live in `config.yaml`; the logic
in `workflow.py` (pure functions `growth_cap_rows`, `mode_ban_rows` here,
`eflow_bound_rows` in `water/data/water_supply.py`, covered by
`tests/project/test_sparccle_adapt_water.py` and `tests/model/water/data/test_water_supply.py`).

## How the narrative maps onto the model

- **AU policies (every W scenario).** E-flows: the water module computes the
  environmental flow (VMF, GHM data of the RCP) but only writes the
  `bound_activity_lo` under an SDG build; `eflow` writes the same rows on a
  clone. Growth: PR #522 already caps `extract_surfacewater` and
  `extract_groundwater` at 2 %/yr in every build; `renewable_growth` overwrites
  the value with the starter's rate (0.03 for SSP3).
- **Maladaptation.** "No efficiency improvement": distribution technologies
  (`urban_t_d`, `rural_t_d`, sewerage, recycle, discharge) have a baseline mode
  `M1` and an efficient mode `Mf` (lower input per unit output, higher cost,
  `data/water/infrastructure/water_distribution.csv`); `ban_mode: Mf` removes
  the efficient one. Desalination and fossil groundwater are left at the
  module's costs: desalination is capped by the projection
  (`bound_total_capacity_up`) and 10 %/yr new-capacity growth; fossil
  groundwater is the unbounded backstop at 20 % above renewable groundwater.
- **+Adaptation.** The SDG package of the water module is the closest ready
  option: efficient distribution only, with the mid-tier costs, maximum
  treatment and recycling targets, e-flow bounds. Desalination and fossil
  groundwater close the remaining gap at their costs.

## Not represented (to add)

- **Irrigation efficiency.** No technology or coefficient exists: `ieff1..3`
  are set members only, irrigation withdrawals are exogenous demands routed
  through `irrigation_*`. Needs an input-coefficient reduction on those
  technologies at a cost, and a number for that cost.
- **Desalination before fossil groundwater.** The LP orders supply by cost.
  In the base, fossil groundwater costs ~0.03 USD/m³ (investment 0.065 USD per
  m³/yr over 20 years plus electricity) against ~0.34 for membrane desalination
  (1.90 investment over 30 years, 0.155 fixed, electricity and heat), so the
  backstop takes the gap and desalination stays at the historical plants.
  `BAUfg` tests the pricing answer; a cap at historical extraction is the
  alternative. In `BAUfg` the marginal water price reaches 0.44 USD/m³ in every
  basin, yet no desalination plant is built: the module's 10 %/yr
  `growth_new_capacity_up` on `membrane`/`distillation` chains from
  `historical_new_capacity`, which ends in 2025 while the first model year is
  2035, so the 2030 anchor is zero and the chain never starts (`desal_free`
  fixes this on a clone; to be reported upstream). `membrane` also takes
  0.001 GWa/MCM of district heat (~0.2 USD/m³ at the model's heat price) although
  reverse osmosis runs on electricity. Expansion is also bounded by the
  projected potential (`bound_total_capacity_up` on `extract_salinewater_basin`,
  from Marina Andrijevic's 2026 country projections, SSP1/3/5 modelled and the
  other SSPs assigned; see `data/pre_processing/generate_desalination.py`),
  which the backfill sets to **zero in 2110** because the projection ends in 2100.
  Even freed (`BAUfgd`, 10-22) no plant is built: membrane desalination costs
  ~0.54 USD/m³ once its energy inputs are priced (electricity 0.0003 and district
  heat 0.001 GWa/MCM, ~0.26 USD/m³ together) against ~0.43 for fossil groundwater
  at the 0.4 `var_cost`, and the existing plants retire by 2055. Reverse osmosis
  takes no heat in reality, so the heat input is the first thing to question;
  the alternative is a fossil-groundwater price of ~0.6 USD/m³.
- **"Up to 10 % loss reduction".** The `Mf` coefficients are what the CSV says;
  they are not calibrated to a 10 % cut.
- **Basin-level detail.** `water-ix report --water` gives regional IAMC
  variables (run by the job script, CSVs under `~/sparccle/out/reports/water/`);
  `scripts/water_report.py` compares scenarios into `report.md` plus figures. A
  basin-level script comparable to `../scripts/cooling_detail.py` is still
  to be written.

## Running (UniCC)

```sh
# one SSP per job, serial inside (one HSQLDB process); SSP2 and SSP3 in parallel
cd ~/sparccle/logs
sbatch <repo>/message_ix_models/project/sparccle/adapt/water/scripts/run_sparccle_water.sh SSP2
sbatch <repo>/message_ix_models/project/sparccle/adapt/water/scripts/run_sparccle_water.sh SSP3 base BAU   # keys: base BAU maladapt adapt

# build one scenario by hand (no solve)
mix-models sparccle_water run --config message_ix_models/project/sparccle/adapt/water/config.yaml "SSP2 BAU" --go
# platform / model / scenario of a key, and whether it exists / is solved
mix-models sparccle_water names SSP2 maladapt   # platform model scenario sdgs
mix-models sparccle_water status SSP2 maladapt

# compare the reporting CSVs of several scenarios (first = reference)
python .../water/scripts/water_report.py --out ~/sparccle/out/reports/water/ssp2 \n  base=~/sparccle/out/reports/water/SSP_SSP2_v6.6_sp_CurrentPolicies_SSP2_W_base_nexus.csv \n  BAU=.../CurrentPolicies_SSP2_W_BAU_nexus.csv BAUfg=.../CurrentPolicies_SSP2_W_BAUfg_nexus.csv
```

The job script skips a key whose scenario is already solved (`REBUILD=1` to
force) and runs the water reporting after each key (`WATER_REPORT=0` to skip).

`scripts/localize_starter.py` copies the starter from `source_platform`
(idempotent). Scripts take `MSG_ENV`, `IXMP_DATA`, `SOLVE_OPTS` from the
environment; defaults match the UniCC setup (`~/env/msg_pci`,
`~/.local/share/ixmp_pci`). Outputs, databases and logs live under `~/sparccle/`.

## Known behaviour

- A `water-ix nexus` build solves the scenario itself (`lpmethod=4`,
  `scaind=-1`); `adapt` is therefore solved twice when the growth cap changes
  it (SSP3), once by the build and once after the cap. SSP2's cap equals the
  module default, so nothing changes and no re-solve happens.
- `--output-scenario` on `water-ix` (added for this package) names the nexus
  scenario verbatim instead of `<input>_nexus`.
