#!/bin/sh
set -e

date

# Paths

# User's home directory (network drive)
# NB this is different from $HOME on the worker node
HHOME=/hdrive/u045/{{ username }}

# Python virtual environment
VENV=$SLURM_TMPDIR/venv/{{ python_version }}

# UnICC shared scratch (local disk)
SHARED_SCRATCH=/sharedscratch

# Directory to copy reporting output
# # Projects drive
# REPORT_DIR=/pdrive/projects/ene.model3/MESSAGEix-Transport
# User's home directory
REPORT_DIR=$HHOME/MESSAGEix-Transport

# Cache for iam-units, to work around an issue in pint
export IAM_UNITS_CACHE=$SLURM_TMPDIR/cache/iam-units

# Paths on the worker node
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

{% if source == "gitlab" %}

# ssh: add host keys for gitlab.iiasa.ac.at
mkdir -p $HOME/.ssh
ssh-keyscan -p 2222 gitlab.iiasa.ac.at | tee -a $HOME/.ssh/known_hosts
ssh-keygen -Hf $HOME/.ssh/known_hosts

# URL for 'git clone' of message-static-data
STATIC_DATA_URL="ssh://git@gitlab.iiasa.ac.at:2222/ece/message-static-data.git"

# URL for 'pip install' of message-ix-models
MESSAGE_MODELS_URL="git+https://gitlab.iiasa.ac.at/ece/message-ix-models.git@{{ branch }}"

{% elif source == "github" %}

STATIC_DATA_URL="ssh://git@github.com/iiasa/message-static-data.git"

MESSAGE_MODELS_URL="git+https://github.com/iiasa/message-ix-models.git@{{ branch }}"

{% endif %}

# uv: use a version installed in shared scratch
UV_UNMANAGED_INSTALL=$SHARED_SCRATCH/local/bin
UV=$UV_UNMANAGED_INSTALL/uv

# Store cache in the shared-scratch disk mounted on runners
export UV_CACHE_DIR=$SHARED_SCRATCH/uv-cache/$USER

# Shared-scratch and the job work directory are on different filesystems.
# Silence an uv warning about this.
export UV_LINK_MODE=copy

if [ ! -x $UV ]; then
  # Install
  curl -LsSf https://astral.sh/uv/install.sh | env UV_UNMANAGED_INSTALL=$UV_UNMANAGED_INSTALL sh
fi

# Clone message-static-data to shared scratch
# rm -rf $STATIC_DATA
[ -d $STATIC_DATA ] || GIT_CLONE_PROTECTION_ACTIVE=false git clone $STATIC_DATA_URL $STATIC_DATA
date

# Symlink message-static-data into local data
mkdir -p $MESSAGE_LOCAL_DATA
cp -frsv $STATIC_DATA/* $MESSAGE_LOCAL_DATA/
date

# Create and activate a virtual environment.
$UV --verbose venv --python={{ python_version }} $VENV
. $VENV/bin/activate
date

# Install
# - ixmp, message_ix: from GitHub, for PostgreSQL support
#   not in the latest release v3.11.0.
# - message-ix-buildings from GitHub (not on PyPI).
# - message-ix-models from either IIASA GitLab or GitHub.
$UV pip install \
  "ixmp @ git+https://github.com/iiasa/ixmp.git" \
  "message_ix @ git+https://github.com/iiasa/message_ix.git" \
  "message-ix-buildings @ git+https://github.com/iiasa/message-ix-buildings.git" \
  "message-ix-models[buildings,tests,transport] @ $MESSAGE_MODELS_URL"
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
mix-models \
  --platform=ixmp-dev \
  transport run \
  --platform-dest=message-jdbc-pg \
  --base=auto --fast --nodes=R12 --model-extra={{ model_extra }} \
  {# Format args one-by-one #}
  {% for arg in mix_models_args %}
    {% if arg.startswith("--") %}{# An option, maybe ending with ="" #}
  {{ arg }}{% if arg.endswith("=") %}""{% endif %}
      {% if not loop.last %} \
      {% endif %}
    {% else %}{# A parameter → re-add quotation marks #}
  "{{ arg }}"{% if not loop.last %} \
             {% endif %}
    {% endif %}
  {% endfor +%}
date

# Transfer files from worker to shared/permanent disk
mkdir -p $REPORT_DIR
cp -rv $MESSAGE_LOCAL_DATA/report $MESSAGE_LOCAL_DATA/transport $REPORT_DIR/
date
