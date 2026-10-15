#!/bin/bash
#SBATCH --job-name=report_pci
#SBATCH --time=06:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH --mail-type=END,FAIL
#SBATCH -o %x_%J.out

# Legacy IAMC reporting (message_data.tools.post_processing) of one scenario on a
# local ixmp platform; ~20 min and ~23 GB RSS for an R12 scenario.
#
# Usage: sbatch [-J report_<key>] run_report_pci.sh PLATFORM MODEL SCENARIO [report_sparccle_pci.py args]
#
# Needs message_data (branch ssp_sparccle) installed in $MSG_ENV. The xlsx lands in
# <message_data>/reporting_output/<MODEL>_<SCENARIO>.xlsx unless --out-dir is given.
# One process per HSQLDB file: to report several scenarios of one platform in
# parallel, copy the db files and register each copy as its own platform.

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

source /opt/apps/lmod/8.7/init/bash
module load Python/3.11.5-GCCcore-13.2.0
module load Java
source "${MSG_ENV:-$HOME/env/msg_pci}/bin/activate"
export IXMP_DATA="${IXMP_DATA:-$HOME/.local/share/ixmp_pci}"
cd "$HOME"

srun python "$SCRIPT_DIR/report_sparccle_pci.py" "$@"
