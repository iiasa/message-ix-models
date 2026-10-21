#!/bin/bash
#SBATCH --job-name=sparccle_water
#SBATCH --time=24:00:00
#SBATCH --mem=100G
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=END,FAIL
#SBATCH -o %x_%J.out

# Localise, build and solve SPARCCLE water adaptation scenarios (UniCC, SLURM).
#
# Usage: sbatch run_sparccle_water.sh SSP [KEY ...]
#   SSP: a starter key of ../config.yaml (SSP2, SSP3)
#   KEY: scenario keys of ../config.yaml; default: base BAU maladapt adapt
#
# Steps: copy the starter to its local HSQLDB platform (localize_starter.py,
# no-op when present); per KEY, `mix-models sparccle_water run` builds the
# scenario (a `water-ix nexus` build solves itself) and solve_local.py solves
# it when it has no solution. Serial on purpose: one HSQLDB platform admits a
# single process. One SSP per job; SSP2 and SSP3 can run in parallel.
#
# Environment (override before sbatch):
#   MSG_ENV     python venv with message_ix_models   (default ~/env/msg_pci)
#   IXMP_DATA   ixmp config dir holding the platforms (default ~/.local/share/ixmp_pci)
#   SOLVE_OPTS  extra CPLEX options for solve_local.py (default "--solve-option scaind=-1")
# Logs go to %x_%J.out in the submission directory (use ~/sparccle/logs).

[ $# -ge 1 ] || { echo "usage: $0 SSP [KEY ...]"; exit 2; }
SSP=$1; shift
KEYS=("$@"); [ ${#KEYS[@]} -gt 0 ] || KEYS=(base BAU maladapt adapt)

SELF=${BASH_SOURCE[0]}
[ -n "$SLURM_JOB_ID" ] && SELF=$(scontrol show job "$SLURM_JOB_ID" | sed -n 's/.*Command=\([^ ]*\).*/\1/p')
SCRIPT_DIR=$(cd "$(dirname "$SELF")" && pwd)
CFG=$SCRIPT_DIR/../config.yaml
SOLVE=$SCRIPT_DIR/../../adapt/scripts/solve_local.py

source /opt/apps/lmod/8.7/init/bash
module load Python/3.11.5-GCCcore-13.2.0
module load Java
module load gams
source "${MSG_ENV:-$HOME/env/msg_pci}/bin/activate"
export IXMP_DATA="${IXMP_DATA:-$HOME/.local/share/ixmp_pci}"
cd "$HOME"

SOURCE=$(grep -E "^source_platform:" "$CFG" | awk '{print $2}')
LOCAL_DB=$(grep -E "^local_db:" "$CFG" | awk '{print $2}')
read -r PLATFORM MODEL STARTER < <(mix-models sparccle_water names --config "$CFG" "$SSP" starter)

echo "== $(date -u +%FT%TZ) localize $SOURCE/$MODEL/$STARTER -> $PLATFORM"
srun python "$SCRIPT_DIR/localize_starter.py" "$SOURCE" "$MODEL" "$STARTER" "$LOCAL_DB" || exit 1

status=0
for KEY in "${KEYS[@]}"; do
  read -r PLATFORM MODEL SCEN < <(mix-models sparccle_water names --config "$CFG" "$SSP" "$KEY")
  echo "== $(date -u +%FT%TZ) build $SSP $KEY -> $PLATFORM/$MODEL/$SCEN"
  if ! srun mix-models sparccle_water run --config "$CFG" "$SSP $KEY" --go; then
    echo "!! build failed: $SSP $KEY"; status=1; continue
  fi
  echo "== $(date -u +%FT%TZ) solve $PLATFORM/$MODEL/$SCEN (if unsolved)"
  # shellcheck disable=SC2086  # SOLVE_OPTS is a list of options on purpose
  if ! srun python "$SOLVE" "$PLATFORM" "$MODEL" "$SCEN" --if-unsolved ${SOLVE_OPTS:---solve-option scaind=-1}; then
    echo "!! solve failed: $SSP $KEY"; status=1; continue
  fi
done
echo "== $(date -u +%FT%TZ) done, status $status"
exit $status
