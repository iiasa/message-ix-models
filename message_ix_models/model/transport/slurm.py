from message_ix_models.util.slurm import Template

#: Template for use with :program:`mix-models sbatch`. Example usage::
#:
#:   mix-models sbatch -m model.transport --remote --username=foo --go -- \
#:     "--base=auto --fast --nodes=R12 --model-extra='debug' --from='' 'SSP2 reported'"
#:
#: The script performs the following actions, tailored for the IIASA 'UniCC' cluster and
#: SLURM:
#:
#: 1. Load necessary modules.
#: 2. Use :program:`uv` to install :mod:`message_ix_models` and dependencies.
#: 3. Run :program:`mix-models transport run --go` with additional arguments.
#: 4. Copy output files (e.g. reporting) to a shared drive.
TEMPLATE = Template(
    "sbatch",
    "--job-name={J}",
    "--mem=40G",
    "--output=/home/{username}/slurm/message-%J.out",
    "--time=2:00:00",
    r"""#!/bin/sh
set -e

date

# Paths
HHOME=/hdrive/u045/{username}
VENV=$SLURM_TMPDIR/venv/3.13
SHARED_SCRATCH=/sharedscratch
REPORT_DIR=/pdrive/projects/ene.model3/MESSAGEix-Transport

# Work around an issue in pint
export IAM_UNITS_CACHE=$SLURM_TMPDIR/cache/iam-units
export IXMP_DATA=$SLURM_TMPDIR/ixmp
export MESSAGE_LOCAL_DATA=$SLURM_TMPDIR/message-local-data
export MESSAGE_MODELS_CACHE=$SHARED_SCRATCH/cache/message-ix-models
STATIC_DATA=$SHARED_SCRATCH/message-static-data


# Activate lmod for the current shell
. /opt/apps/lmod/8.7/init/sh

# Load module(s)
module load gams git git-lfs Java

# git-lfs: install global configuration
git lfs install

# ssh: add host keys for gitlab.iiasa.ac.at
ssh-keyscan -p 2222 gitlab.iiasa.ac.at | tee -a ~/.ssh/known_hosts
ssh-keygen -Hf ~/.ssh/known_hosts

# uv: use UV from personal home directory
UV=$HHOME/.local/bin/uv
# Store cache in the shared-scratch disk mounted on runners
export UV_CACHE_DIR=$SHARED_SCRATCH/uv-cache
# Shared-scratch and the job work directory are on different filesystems; silence an uv
# warning about this
export UV_LINK_MODE=copy

# Clone message-static-data to shared scratch
# rm -rf $STATIC_DATA
[ -d $STATIC_DATA ] || GIT_CLONE_PROTECTION_ACTIVE=false git clone ssh://git@gitlab.iiasa.ac.at:2222/ece/message-static-data.git $STATIC_DATA
date

# Symlink message-static-data into local data
mkdir -p $MESSAGE_LOCAL_DATA
cp -frsv $STATIC_DATA/* $MESSAGE_LOCAL_DATA/
date

# - Create and activate a virtual environment.
# - Use the system Python. TODO Use Python 3.14
$UV --verbose venv --python=3.13 $VENV
. $VENV/bin/activate
date

# Install
# - message-ix-models from IIASA GitLab.
# - message-ix-buildings from GitHub (not on PyPI).
# - message-ix and other dependencies from PyPI.
# - dask to work around https://github.com/khaeru/genno/issues/171
$UV pip install \
  "dask < 2025.4.0" \
  "message-ix-buildings @ git+https://github.com/iiasa/message-ix-buildings.git" \
  "message-ix-models[buildings,tests,transport] @ git+https://gitlab.iiasa.ac.at/ece/message-ix-models.git@transport/2025-w44"
date

# Show what was installed
message-ix show-versions

# Copy config file for ixmp et al. to the worker node for possible modification
# This file contains connection details for the ixmp-dev platform
mkdir -p $IXMP_DATA
cp $HHOME/.local/share/ixmp/config.json $IXMP_DATA/config.json

# Create cache directory
mkdir -p $MESSAGE_MODELS_CACHE

# Run the workflow
date
mix-models --platform=ixmp-dev transport run \
  --base=auto --fast --nodes=R12 --model-extra=dev \
  {mix_models_args} \
  --go
date

# Transfer files from worker to shared/permanent disk
cp -rv $MESSAGE_LOCAL_DATA/report $MESSAGE_LOCAL_DATA/transport $REPORT_DIR/
date
""",  # noqa: E501
)
