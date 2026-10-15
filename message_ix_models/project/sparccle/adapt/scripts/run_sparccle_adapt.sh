#!/bin/bash
#SBATCH --job-name=sparccle_adapt
#SBATCH --time=12:00:00
#SBATCH --mem=80G
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=END,FAIL
#SBATCH -o %x_%J.out

# Build, solve and post-process SPARCCLE adaptation scenarios (UniCC, SLURM).
#
# Usage: sbatch run_sparccle_adapt.sh KEY [KEY ...]
#   KEY: a scenario key of ../config.yaml (BAU malad35 adapt35 adapt35h40).
#
# For each KEY: clone + constrain via `mix-models sparccle_adapt run`, solve with
# MESSAGE (solve_local.py), then write the cooling-technology detail
# (cooling_detail.py) to $COOLING_OUT/<scenario>/. Serial on purpose: all
# scenarios share one HSQLDB file platform, which admits a single process.
# A failing scenario is logged ("!! ... failed: KEY") and the loop moves on.
#
# Environment (override before sbatch):
#   MSG_ENV      python venv with message_ix_models        (default ~/env/msg_pci)
#   IXMP_DATA    ixmp config dir holding the platform      (default ~/.local/share/ixmp_pci)
#   COOLING_OUT  output root for cooling_detail.py         (default ~/cooling_detail_out)
#   SOLVE_OPTS   extra CPLEX options for solve_local.py, e.g.
#                "--solve-option scaind=1 --solve-option numericalemphasis=1"
# Logs go to %x_%J.out in the submission directory.

[ $# -ge 1 ] || { echo "at least one scenario key required"; exit 2; }

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CFG=$SCRIPT_DIR/../config.yaml

source /opt/apps/lmod/8.7/init/bash
module load Python/3.11.5-GCCcore-13.2.0
module load Java
module load gams
source "${MSG_ENV:-$HOME/env/msg_pci}/bin/activate"
export IXMP_DATA="${IXMP_DATA:-$HOME/.local/share/ixmp_pci}"
COOLING_OUT="${COOLING_OUT:-$HOME/cooling_detail_out}"
cd "$HOME"

PLATFORM=$(grep -E "^platform:" "$CFG" | awk '{print $2}')
MODEL=$(grep -E "^model:" "$CFG" | awk '{print $2}')
BASE=$(grep -E "^base:" "$CFG" | awk '{print $2}')

status=0
for KEY in "$@"; do
  SCEN="${BASE}_${KEY}"
  echo "== $(date -u +%FT%TZ) build $KEY -> $MODEL/$SCEN"
  if ! srun mix-models sparccle_adapt run --config "$CFG" "$KEY" --go; then
    echo "!! build failed: $KEY"; status=1; continue
  fi
  echo "== $(date -u +%FT%TZ) solve $PLATFORM/$MODEL/$SCEN"
  # shellcheck disable=SC2086  # SOLVE_OPTS is a list of options on purpose
  if ! srun python "$SCRIPT_DIR/solve_local.py" "$PLATFORM" "$MODEL" "$SCEN" ${SOLVE_OPTS:-}; then
    echo "!! solve failed: $KEY"; status=1; continue
  fi
  mkdir -p "$COOLING_OUT"
  TARGETS=$COOLING_OUT/targets_${SCEN}.txt
  echo "$MODEL/$SCEN" > "$TARGETS"
  echo "== $(date -u +%FT%TZ) cooling_detail $SCEN"
  srun python "$SCRIPT_DIR/cooling_detail.py" --platform "$PLATFORM" --scenarios "$TARGETS" \
    --out "$COOLING_OUT/$SCEN" || status=1
done
echo "== $(date -u +%FT%TZ) done, status $status"
exit $status
