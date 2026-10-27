#!/bin/bash
#SBATCH --job-name=water_elec --time=01:00:00 --mem=72G --cpus-per-task=2 -o /home/vinca/sparccle/logs/%x_%J.out
source /opt/apps/lmod/8.7/init/bash; module load Python/3.11.5-GCCcore-13.2.0 Java
source ~/env/msg_pci/bin/activate; export IXMP_DATA=$HOME/.local/share/ixmp_pci
export IXMP_JVM_ARGS="-Xmx56G"
srun python ~/sparccle/tmp/elec_ssp2.py
