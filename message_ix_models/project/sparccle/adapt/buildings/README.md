# SPARCCLE buildings scenarios (IMAGE)

Results of the IMAGE buildings runs for the SPARCCLE adaptation protocol
(`SSP2_M_CP_BAU`, `SSP2_M_CP_MALADAPT`, `SSP2_M_CP_POSITIVEADAPT2`), supplied as
IAMC xlsx files dropped in this folder (gitignored, ~7 MB each). They are not
MESSAGEix output; they sit here beside the power-sector (`..`) and water
(`../water`) scenarios so the three sectors can be compared from one place.

`buildings_report.py` reads the `data` sheets and writes `results/report_b.md`
(`--pdf` for a PDF) with key indicators, differences from BAU, space-cooling
electricity by appliance, appliance ownership (total and by income quintile),
regional cooling electricity and heat-vulnerable population. Reuses the table
and PDF helpers of `../water/scripts/water_report.py`.

```sh
python message_ix_models/project/sparccle/adapt/buildings/buildings_report.py --pdf
```

What the files carry: 27 regions (26 + World), 2005-2100, 1,375 variables.
World rows are missing for final energy and prices; the script sums (prices:
averages) the regions. No buildings investment variable exists; consumption and
GDP are identical across the three runs.
