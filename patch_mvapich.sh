#!/bin/sh

# this script will update the pmgr_collective patch files
# to be used with mvapich

#mpidir=/usr/global/tools/mpi/checkouts/llnl/0.9.9-r1760-llnl
#channels="ch_gen2 ch_smp"

#mpidir=/usr/global/tools/mpi/checkouts/llnl/1.0-r2481-llnl
#channels="ch_gen2 ch_gen2_ud ch_smp"

mpidir=/usr/global/tools/mpi/checkouts/llnl/1.1-r3286-llnl
channels="ch_gen2 ch_smp ch_hybrid"

# pushd $mpidir; make distclean; make quilt; popd

rm -rf tempdir
mkdir tempdir
cd tempdir

# to add llnl pmgr_files to mvapich
for c in $channels ; do
  mkdir -p mvapich+chaos.orig/mpid/${c}/process
  mkdir -p mvapich+chaos/mpid/${c}/process
  cp -pr ../src/* mvapich+chaos/mpid/${c}/process
done
diff -Nuar mvapich+chaos.orig mvapich+chaos > pmgr_collective_add_files.patch

# clear out tempdir/mvapich+chaos dirs and start fresh
rm -rf mvapich+chaos.orig
rm -rf mvapich+chaos

# to remove pmgr_files from mvapich
for c in $channels ; do
  mkdir -p mvapich+chaos.orig/mpid/${c}/process
  mkdir -p mvapich+chaos/mpid/${c}/process
  cp -pr $mpidir/mvapich+chaos/mpid/${c}/process/pmgr_collective_{common,client,mpirun}* mvapich+chaos.orig/mpid/${c}/process
done
diff -Nuar mvapich+chaos.orig mvapich+chaos > pmgr_collective_remove_files.patch

# clear out tempdir/mvapich+chaos dirs and start fresh
rm -rf mvapich+chaos.orig
rm -rf mvapich+chaos

# to remove pmgr_files from mvapich
for c in $channels ; do
  mkdir -p mvapich+chaos.orig/mpid/${c}/process
  mkdir -p mvapich+chaos/mpid/${c}/process
  cp -pr $mpidir/mvapich+chaos/mpid/${c}/process/pmgr_collective_{common,client,mpirun}* mvapich+chaos.orig/mpid/${c}/process
  cp -pr ../src/* mvapich+chaos/mpid/${c}/process
done
diff -Nuar mvapich+chaos.orig mvapich+chaos > pmgr_collective_true_diff.patch
