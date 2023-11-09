#!/bin/sh
#
# Migrate a module from message_data to message_ix_models,
# preserving its git history.

. settings.sh

# Echo a message on how to restart
echo "To undo, run:"
echo "  $ rm -rf $DST && git clone $DST_REPO"

# Clone the destination repository
git clone $DST_REPO $DST

# Change directory to the clone
cd $DST

# commented: current process uses a manual rebase after this script
#
# # Store the initial commit of the destination repo
# DST_BASE_COMMIT=$(git rev-list --max-parents=0 HEAD)
# [ -n "$DST_BASE_COMMIT" ] || exit 1
#
# echo "Will rebase initial commits on to:"
# git show --no-patch $DST_BASE_COMMIT

# Add the *source* repo as a remote to the *destination* repo;
# also fetch
git remote add --fetch $SRC ../$SRC

# Copy LFS objects from source repo's .git directory.
#
# This ensures that when commit history is being rewritten in the current
# directory, references to LFS objects are resolved, because they are in the
# local cache. The script 1-prep.sh must have been run to populate the cache.
cp -r ../$SRC/.git/lfs/objects/* .git/lfs/objects/

# Check out the *source* branch as a new branch in the *destination* repo
git checkout -b $BRANCH $SRC/$SRC_BRANCH

# Use git-filter-repo
# --refs $BRANCH = only filter the source branch.
#
# --path-rename = rename several paths and files under them, i.e.:
# - Module data.
# - Module code. The "/model/" path fragment could also be "/project/", or
#   removed entirely.
# - Module tests.
#
# Add or remove lines here as necessary; not all modules have all the above
# pieces, and some modules have additional pieces.
git filter-repo \
  --refs $BRANCH --force --debug \
  --path-rename data/$MOD/:$D_PACKAGE/data/$MOD/ \
  --path-rename $S_PACKAGE/model/$MOD/:$D_PACKAGE/model/$MOD/ \
  --path-rename $S_PACKAGE/tests/model/$MOD/:$D_PACKAGE/tests/model/$MOD/

# --path = keep only a subset of files and directories.
#
# This has the effect of discarding the top-level message_data and data
# directories, keeping only message_ix_models. This operates on the paths
# renamed by the previous command. It would be possible to combine in a single
# command, but we would then need to specify the *original* paths to keep.
#
# NB can add lines to keep other files, e.g.:
# --path doc/$MOD/
git filter-repo \
  --refs $BRANCH --force --debug \
  --message-callback 'return re.sub(b"^[a-z]", message[:1].upper(), message)' \
  --path $D_PACKAGE/

# commented: unused
#
# # --invert-paths = *remove* some specific files e.g. non-reporting test data
# git filter-repo \
#  --refs $BRANCH --force --debug \
#  --invert-paths \
#  --path-regex "^$DST/tests/data/[^r].*$"

# commented: current process uses a manual rebase after this script. This method
# can fail if the commit history is non-linear, with conflicts.
#
# # Store the initial commit(s) of the source branch, after filtering.
# # If the history is not linear, there may be more than one.
# SRC_BASE_COMMIT=$(git rev-list --max-parents=0 $BRANCH)
# [ -n "$SRC_BASE_COMMIT" ] || exit 1
#
# # Graft the initial commit(s) of the source branch onto the first commit of the
# # destination repo, so the two share a common initial commit.
# #
# # If there are more than one, the git-replace operations must be done together
# # and filter-repo last.
# for SBC in $SRC_BASE_COMMIT;
# do
#   git replace --graft $SBC $DST_BASE_COMMIT
# done
#
# git filter-repo --refs $BRANCH --force --debug

# Generate a TODO list for "git rebase --interactive --empty=drop main"
git log --oneline | tac | sed -E "s/^/p /" >../rebase-todo.txt

# Generate a list to help identifying duplicate commits
git log --oneline | cut -d" " -f2- | sort | uniq -d >../duplicates.txt

# After this, see the instructions in doc/migrate.rst
