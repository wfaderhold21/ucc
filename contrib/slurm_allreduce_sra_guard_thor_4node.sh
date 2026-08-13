#!/usr/bin/env bash
#
# Four-node Thor SRA topology-guard matrix on mlx5_0:1.
#
# Submit from the ucc-sra checkout on hpcac-internal:
#   sbatch contrib/slurm_allreduce_sra_guard_thor_4node.sh

#SBATCH --job-name=ucc-sra-guard-thor
#SBATCH --partition=thor
#SBATCH --nodes=4
#SBATCH --nodelist=thor[001-004]
#SBATCH --exclude=thorbf3a[001-016]
#SBATCH --ntasks-per-node=32
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

export MATRIX=thor
export SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(pwd)}}
export MODULES=${MODULES:-"gcc hpcx/2.25"}
export BUILD_JOBS=${BUILD_JOBS:-32}

bash "$SRC_DIR/contrib/run_sra_guard_matrix_common.sh"
