#!/bin/bash
#SBATCH --job-name=solve_local
#SBATCH --time=06:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=END,FAIL
#SBATCH -o %x_%J.out

# (Re-)solve one scenario on a local ixmp platform and write its cooling detail.
#
# Usage: sbatch run_solve_local.sh PLATFORM MODEL SCENARIO [targets file]
#   targets file: scenarios for cooling_detail.py, one MODEL/SCENARIO per line
#                 (default: SCENARIO alone).
#
# Environment: MSG_ENV, IXMP_DATA, COOLING_OUT, SOLVE_OPTS as in run_sparccle_adapt.sh.
# Example for a CPLEX "optimal with unscaled infeasibilities" rejection:
#   SOLVE_OPTS="--solve-option scaind=1 --solve-option numericalemphasis=1" sbatch ...

PLATFORM="${1:?platform name}"
MODEL="${2:?model name}"
SCEN="${3:?scenario name}"
TARGETS="${4:-}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

source /opt/apps/lmod/8.7/init/bash
module load Python/3.11.5-GCCcore-13.2.0
module load Java
module load gams
source "${MSG_ENV:-$HOME/env/msg_pci}/bin/activate"
export IXMP_DATA="${IXMP_DATA:-$HOME/.local/share/ixmp_pci}"
COOLING_OUT="${COOLING_OUT:-$HOME/cooling_detail_out}"
cd "$HOME"

echo "== solve $PLATFORM/$MODEL/$SCEN"
# shellcheck disable=SC2086
srun python "$SCRIPT_DIR/solve_local.py" "$PLATFORM" "$MODEL" "$SCEN" ${SOLVE_OPTS:-} || exit 1
mkdir -p "$COOLING_OUT"
if [ -z "$TARGETS" ]; then
  TARGETS=$COOLING_OUT/targets_${SCEN}.txt
  echo "$MODEL/$SCEN" > "$TARGETS"
fi
echo "== cooling_detail on $(tr '\n' ' ' < "$TARGETS")"
srun python "$SCRIPT_DIR/cooling_detail.py" --platform "$PLATFORM" --scenarios "$TARGETS" \
  --out "$COOLING_OUT/$SCEN"
