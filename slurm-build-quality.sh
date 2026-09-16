#!/bin/bash
#SBATCH --job-name=ucc_quality_build
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:45:00
#SBATCH --partition=thor
#SBATCH --output=/global/home/users/faderholdt/ucc-staging/quality-build-%j.out
#SBATCH --error=/global/home/users/faderholdt/ucc-staging/quality-build-%j.out
#
# Build UCC (topic/shrink_v2_quality) on the cluster.
# Direct single-node build job (no benchmark submission).
#
# CPPFLAGS=-Wno-shadow works around a pre-existing -Werror shadow warning in
# core/ucc_progress_queue_mt.c (unrelated to this branch, tripped by hpcx gcc-11).

set -o pipefail
set -e

UCC_DIR=/global/home/users/faderholdt/ucc-staging/ucc-shrink
INSTALL=$UCC_DIR/install
HPCX_UCX=/global/software/rocky-9.x86_64/modules/gcc/11/hpcx/2.25.1/ucx/mt
HPCX_MPI=/global/software/rocky-9.x86_64/modules/gcc/11/hpcx/2.25.1/ompi
export LD_LIBRARY_PATH="$HPCX_MPI/lib:$HPCX_UCX/lib:$LD_LIBRARY_PATH"
# HPC-X ompi wrappers are built against a relocated /build-result prefix; OPAL_PREFIX
# lets mpicc/mpicxx find their wrapper-data/config files.
export OPAL_PREFIX="$HPCX_MPI"

echo "Job $SLURM_JOB_ID on $(hostname), node=$(scontrol show hostnames "$SLURM_JOB_NODELIST")"
echo "UCC_DIR=$UCC_DIR"
date

echo "gcc: $(gcc --version | head -1)"

cd "$UCC_DIR"

echo "=== clean prior autotools state ==="
make distclean >/dev/null 2>&1 || true
rm -f config.h ucc_version.h

echo "=== autogen ==="
if [ ! -x ./configure ]; then
    ./autogen.sh
else
    echo "configure already present; skipping autogen"
fi

echo "=== configure ==="
./configure \
    --prefix="$INSTALL" \
    --with-ucx="$HPCX_UCX" \
    --with-mpi="$HPCX_MPI" \
    CPPFLAGS="-Wno-shadow"

echo "=== make -j ==="
make -j8

echo "=== make install ==="
make install

echo "BUILD_OK install=$INSTALL"
echo "Done: $(date)"
