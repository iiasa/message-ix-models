# EFC ribbon vetting plots

`vet_ribbon.R` renders one ribbon-plot page per variable group defined in
`variable_ribbon.yaml` (a world panel plus 12 regional small multiples per
page) from the workflow's reported Excel output, for side-by-side vetting of
scenario versions.

## Requirements

R with: `readr`, `dplyr`, `tidyr`, `ggplot2`, `readxl`, `yaml`, `gridExtra`,
`scales` (all CRAN).

## Inputs

A data directory containing a `check/` subfolder with one or more `.xlsx`
files as produced by the EFC workflow report step (IAMC-style columns:
Model / Scenario / Region / Variable / Unit / years). Each file is one
scenario; the filename becomes the scenario label.

## Running

```sh
VET_DATA_DIR=/path/to/data \
VET_OUTPUT_DIR=/path/to/output \
Rscript vet_ribbon.R
```

Both variables are optional and default to `data/vet/` and `output/vet/`
relative to the current working directory. The config `variable_ribbon.yaml`
is always resolved next to the script itself, so the script can be run from
any directory; when `source()`ing interactively instead of using `Rscript`,
set the working directory to this folder first.

Output is a single timestamped PDF (`vet_ribbon_YYYYMMDD_HHMM.pdf`) in the
output directory, one page per variable group.
