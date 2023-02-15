#!/bin/sh
#
# Prepare to migrate code

. settings.sh

# Clone the source repo
git clone -b $SRC_BRANCH $SRC_REPO $SRC

# Change directory to the clone
cd $SRC

# Fetch all LFS objects existing on the server; about 5.7 GB for message_data
git lfs fetch --all

# Make a list of missing LFS objects; possibly empty
#
# Per https://github.com/git-lfs/git-lfs/issues/4232#issuecomment-689967255
git lfs ls-files --all -l | \
  perl -n -e 'chomp; @x = split; next if $x[1] eq "*"; $o = $x[0]; $p = substr($o, 0, 2) . "/" . substr($o, 2, 2) . "/$o"; print "$o\n" unless -f ".git/lfs/objects/$p";' \
  >lfs-sha256s-to-remove.txt
