#!/bin/bash
# Thor-adapted smoke sequence for the UCC offline tuner.
#
# Derived from smoke_hpcac.sh, with the environment setup fixed for
# hpcac-internal/thor: the lmod path + `module load gcc hpcx/2.25.1` in the
# hpcx script do not exist on this host.  HPC-X is self-contained here, so we
# source hpcx-init-ompi.sh and call hpcx_load() instead.
#
# Run under a 2-node thor allocation:  salloc -N2 -p thor ./smoke_thor.sh
#
# Steps (unchanged from smoke_hpcac.sh): 0 dry-run cost model, 1 enumerator,
# 2 runner alone, 3 hand TUNE-string parse, 4 minimal full run + readback,
# 4b load emitted config, 4c proof-mode (paired validator), 4d known-bad
# 4k-16k anchor replay, 5 multi-collective x multi-team-size.
set -u

R=/global/home/users/faderholdt/build-staging/ucc-tuning
A=${UCC_TUNE_ARTIFACTS:-/global/home/users/faderholdt/thor-tune-2026-09-21}

HPCX=/global/software/rocky-9.x86_64/modules/gcc/11/hpcx/2.25.1
# hpcx-init-ompi.sh reads ${HPCX_ENABLE_NCCL_MRC_PLUGIN:-} implicitly; disable
# nounset while sourcing so `set -u` does not abort on that unset var.
set +u
cd "$HPCX" && . ./hpcx-init-ompi.sh && hpcx_load
set -u
# thor default UCX transport set makes ucp_rkey_pack fail (-22) during TL/UCP
# RMA-info gather at context create, which perftest turns into a SIGSEGV.
# Diagnosed in job 10466: rc/ud/sm all work when named explicitly.
export UCX_TLS=rc,sm,self
export LD_LIBRARY_PATH=$R/install/lib:$LD_LIBRARY_PATH
export PATH=$R/install/bin:$PATH
UI=$R/install/bin/ucc_info
PT=$R/install/bin/ucc_perftest
mkdir -p $A
cd $R/tools/tune

echo "### nodes: ${SLURM_JOB_NODELIST:-<no-salloc>}  nnodes=${SLURM_NNODES:-0}"
echo "### mpirun: $(mpirun --version 2>&1 | head -1)"
echo "### python: $(python3 --version)"

echo "=========== STEP 0: dry-run cost model (ROADMAP A2) ==========="
python3 ucc_offline_tune.py --dry-run \
    --collective allreduce,allgather,reduce_scatter --mem-type host,cuda-mng \
    --team-sizes 8,64 --n-reps 3 \
    --ucc-info $UI --ucx-info /usr/bin/ucx_info 2>&1 | tee $A/step0_dryrun.txt
echo "exit=${PIPESTATUS[0]}"

echo "=========== STEP 1: enumerator alone ==========="
python3 ucc_tune_space.py algs --ucc-info $UI > $A/step1_algs.txt 2>&1
echo "exit=$?"
echo "collective keys parsed:"
grep -E '^  [a-zA-Z_]+$' $A/step1_algs.txt | sort -u | tr -d ' ' | tr '\n' ' '; echo
echo "non-lowercase keys (must be empty):"
grep -E '^  [a-zA-Z_]+$' $A/step1_algs.txt | grep '[A-Z]' || echo "  (none)"

echo "=========== STEP 2: runner alone (np=8) ==========="
python3 ucc_tune_runner.py -c allreduce -b 1024 -m host -R 3 -n 200 -w 20 \
    --perftest $PT --launcher "mpirun -np 8" 2>&1 | tee $A/step2_runner.txt
echo "exit=${PIPESTATUS[0]}"

echo "=========== STEP 3: hand TUNE string, host mtype ==========="
UCC_LOG_LEVEL=info UCC_TL_UCP_TUNE='allreduce:host:0-inf:@ring' \
  mpirun -np 8 $PT -c allreduce -b 1024 -e 1024 -m host -n 50 -w 5 -p \
  > $A/step3_tune_host.txt 2>&1
echo "exit=$?"
echo "parse errors (must be empty):"
grep -i "failed to parse token" $A/step3_tune_host.txt || echo "  (none)"
echo "score/alg selection lines:"
grep -iE "ring|tune" $A/step3_tune_host.txt | head -6

echo "--- control: deliberately bad token, must be rejected ---"
UCC_LOG_LEVEL=info UCC_TL_UCP_TUNE='allreduce:cuda-managed:0-inf:@ring' \
  mpirun -np 8 $PT -c allreduce -b 1024 -e 1024 -m host -n 10 -w 2 -p \
  > $A/step3_tune_bad.txt 2>&1
echo "exit=$?"
grep -i "failed to parse token" $A/step3_tune_bad.txt | head -2 || echo "  (no rejection - unexpected)"

echo "--- cuda_managed token accepted by the parser (host run) ---"
UCC_LOG_LEVEL=info UCC_TL_UCP_TUNE='allreduce:cuda_managed:0-inf:@ring' \
  mpirun -np 8 $PT -c allreduce -b 1024 -e 1024 -m host -n 10 -w 2 -p \
  > $A/step3_tune_mng.txt 2>&1
echo "exit=$?"
grep -i "failed to parse token" $A/step3_tune_mng.txt || echo "  (none - token accepted)"

echo "=========== STEP 4: minimal full run (screening + readback) ==========="
rm -rf $A/step4
python3 ucc_offline_tune.py \
    --collective allreduce --component tl/ucp --mem-type host --team-sizes 8 \
    --min-bytes 1024 --max-bytes 16384 --factor 4 \
    --n-reps 3 --n-iter 200 --n-warmup 20 \
    --readback-level debug \
    --launcher "mpirun -np {team_size}" --perftest $PT --ucc-info $UI \
    --output-dir $A/step4 --no-validate 2>&1 | tail -25
echo "exit=${PIPESTATUS[0]}"
echo "--- emitted config ---"
cat $A/step4/ucc_tuned_provisional.conf
echo "--- findings (ROADMAP A3) ---"
cat $A/step4/findings.md
echo "--- readback in results.json (default_selected_alg must be non-null) ---"
python3 -c "import json,sys; d=json.load(open('$A/step4/results.json')); print([s.get('default_selected_alg') for r in d.get('results',[]) for s in r.get('size_decisions',[])])"

echo "=========== STEP 4b: load emitted config via UCC_CONFIG_FILE ==========="
UCC_CONFIG_FILE=$A/step4/ucc_tuned_provisional.conf UCC_LOG_LEVEL=info \
  mpirun -np 8 $PT -c allreduce -b 1024 -e 1024 -m host -n 50 -w 5 -p \
  > $A/step4b_load.txt 2>&1
echo "exit=$?"
echo "parse errors (must be empty):"
grep -iE "failed to parse|invalid|error" $A/step4b_load.txt || echo "  (none)"
tail -3 $A/step4b_load.txt

echo "=========== STEP 4c: proof-mode run (exercises paired validator) ==========="
rm -rf $A/step4c
python3 ucc_offline_tune.py \
    --collective allreduce --component tl/ucp --mem-type host --team-sizes 8 \
    --min-bytes 1024 --max-bytes 16384 --factor 4 \
    --n-reps 3 --n-iter 200 --n-warmup 20 \
    --proof-mode --min-pairs 10 --max-pairs 20 \
    --readback-level debug \
    --launcher "mpirun -np {team_size}" --perftest $PT --ucc-info $UI \
    --output-dir $A/step4c 2>&1 | tail -25
echo "exit=${PIPESTATUS[0]}"
echo "--- proof summary (validation table) ---"
grep -A40 "Stage 4 validation" $A/step4c/tuning_summary.txt

echo "=========== STEP 4d: replay known-bad allreduce 4k-16k anchor ==========="
rm -rf $A/step4d
python3 ucc_offline_tune.py \
    --collective allreduce --component tl/ucp --mem-type host --team-sizes 8 \
    --min-bytes 4096 --max-bytes 16384 --factor 4 \
    --n-reps 3 --n-iter 200 --n-warmup 20 \
    --readback-level debug \
    --launcher "mpirun -np {team_size}" --perftest $PT --ucc-info $UI \
    --output-dir $A/step4d --no-validate 2>&1 | tail -10
echo "exit=${PIPESTATUS[0]}"
echo "--- emitted config ---"
cat $A/step4d/ucc_tuned_provisional.conf
echo "--- bad token (must be absent): ---"
grep -E "allreduce:4k-16k" $A/step4d/ucc_tuned_provisional.conf && echo "  !! BAD RANGE RE-EMITTED" || echo "  (absent - inclusive semantics held)"

echo "=========== STEP 5: multi-collective x multi-team-size ==========="
rm -rf $A/step5
python3 ucc_offline_tune.py \
    --collective allreduce,allgather --mem-type host --team-sizes 8,16 \
    --min-bytes 1024 --max-bytes 16384 --factor 4 \
    --n-reps 3 --n-iter 200 --n-warmup 20 \
    --readback-level debug \
    --launcher "mpirun -np {team_size}" --perftest $PT --ucc-info $UI \
    --output-dir $A/step5 --no-validate 2>&1 | tail -30
echo "exit=${PIPESTATUS[0]}"
echo "--- emitted config ---"
cat $A/step5/ucc_tuned_provisional.conf
echo "--- findings (ROADMAP A3) ---"
cat $A/step5/findings.md

echo "=========== STEP 5b: load step5 config ==========="
UCC_CONFIG_FILE=$A/step5/ucc_tuned_provisional.conf UCC_LOG_LEVEL=info \
  mpirun -np 16 $PT -c allreduce -b 4096 -e 4096 -m host -n 50 -w 5 -p \
  > $A/step5b_load.txt 2>&1
echo "exit=$?"
grep -iE "failed to parse|invalid" $A/step5b_load.txt || echo "  (none)"
tail -3 $A/step5b_load.txt

echo "=========== SMOKE SEQUENCE COMPLETE ==========="
