#!/bin/sh

# Settings for bash

# Exit immediately on any error
set -e
# Echo commands for debugging
set -x

# Source package name in Git URLs and file paths
SRC=message_data
# Python name
S_PACKAGE=$SRC
# Remote for its repo
SRC_REPO=git@github.com:iiasa/$SRC.git
# Specific branch from which to migrate code
SRC_BRANCH=nexus_move

# Destination package name in Git URLs and file paths
DST=message-ix-models
# Python name
D_PACKAGE=message_ix_models
# Remote for its repo
DST_REPO=git@github.com:iiasa/$DST.git
# Branch to create in the destination. This is created
# from the HEAD of the default branch, e.g. "main".
BRANCH=migrate-nexus

# Path fragment for path renaming and filtering.
# Edit this together with 2-migrate.sh.
MOD=water
