#!/bin/bash
# B3 sweep (task 582): offload-eligible sizes for CUDA host-offload reduce.
#
# Perftest `reducedt` (executor reduce — the path the 580 hook intercepts),
# -m cuda and -m cuda-mng, N in {2,8}, dt x op matrix, sizes at/below the
# default REDUCE_HOST_LIMIT (256 B) plus one above-limit control (512 B,
# where the policy must NOT route to host, so path 0 and path 1 must agree).
#
# path = UCC_EC_CUDA_USE_HOST_REDUCE (0 = GPU baseline, 1 = host offload).
# One data row per invocation (single -b/-e size); columns:
#   path memtype dt op n_bufs count time_us bus_bw
set -u
T=/home/faderholdt/ucc-570/repo
export LD_LIBRARY_PATH=$T/src/.libs:$T/src/components/ec/cpu/.libs:\
$T/src/components/ec/cuda/.libs:$T/test/gtest/ucm/test_dlopen/.libs
PT=$T/tools/perf/ucc_perftest
OUT=/home/faderholdt/tmp/ucc582/b3
mkdir -p $OUT
: > $OUT/values.tsv

run() { # path MT dt op N count
    local p=$1 MT=$2 dt=$3 op=$4 N=$5 c=$6
    UCC_EC_CUDA_USE_HOST_REDUCE=$p $PT -c reducedt -m $MT -d $dt -o $op \
        -b $c -e $c -N $N -F 2>&1 \
        | awk -v p=$p -v m=$MT -v d=$dt -v o=$op -v n=$N '
            $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ {
                printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p, m, d, o, n, $1, $3, $6
            }' >> $OUT/values.tsv
}

# counts per dt at the 256 B limit: f32/i32 (4 B) -> 1,16,64 (64*4=256);
# f64/i64 (8 B) -> 1,8,32 (32*8=256).
for MT in cuda cuda-mng; do
    for N in 2 8; do
        for spec in "float32:1 16 64" "float64:1 8 32" "int32:1 16 64" "int64:1 8 32"; do
            dt=${spec%%:*}; counts=${spec#*:}
            for op in sum prod min max; do
                for c in $counts; do
                    run 0 $MT $dt $op $N $c
                    run 1 $MT $dt $op $N $c
                done
            done
        done
    done
done

# above-limit control: 128 f32 = 512 B > 256 B -> policy stays GPU on both paths
run 0 cuda float32 sum 2 128
run 1 cuda float32 sum 2 128

echo DONE
