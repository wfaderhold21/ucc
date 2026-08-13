#!/usr/bin/env bash
#
# Four-node Gaia SRA topology-guard, RC/DC, and dual-port matrix.
#
# Submit from the ucc-sra checkout on gaia:
#   sbatch contrib/slurm_allreduce_sra_guard_gaia_4node.sh

#SBATCH --job-name=ucc-sra-guard-gaia
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=112
#SBATCH --time=06:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

export MATRIX=gaia
export SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(pwd)}}
export MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
export UCX_HOME=${UCX_HOME:-/usr}
export MODULES=${MODULES:-}
export BUILD_JOBS=${BUILD_JOBS:-32}

bash "$SRC_DIR/contrib/run_sra_guard_matrix_common.sh"
