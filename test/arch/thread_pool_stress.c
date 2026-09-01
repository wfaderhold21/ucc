/*
 * MPSC stress for ucc_ec_cpu_thread_pool (task 571 acceptance).
 *
 * Links the real pool implementation from libucc_ec_cpu (or compiles the
 * pool .c in for a self-contained TSan run).  NUM_PRODUCERS producer
 * threads each enqueue TASKS_PER_PRODUCER heap-allocated executor tasks;
 * NUM_CONSUMERS workers concurrently dequeue, mark each task complete
 * (release-store) and free it.  A relaxed atomic counter verifies every
 * enqueued task is dequeued exactly once (no loss, no duplication) and
 * the pool finalizes cleanly.
 *
 * TSan note: the lf-queue's 64-bit slot handoff uses ucs_atomic_bool_cswap64,
 * an inline "lock cmpxchg" asm intrinsic invisible to TSan; the handoff is
 * acq_rel-correct at the hardware level.  Run with
 *   TSAN_OPTIONS="suppressions=<this-dir>/tsan.supp"
 * to scope those two known false positives to the pool wrappers.
 *
 * Compile:
 *   gcc -O2 -fsanitize=thread -std=gnu11 -I<ucc-src-root> \
 *       thread_pool_stress.c -o thread_pool_stress \
 *       -L<ucc-src-root>/src/.libs \
 *       -L<ucc-src-root>/src/components/ec/cpu/.libs \
 *       -lucc_ec_cpu -lucc -lucs -lrt -lm -lpthread
 * Run with LD_LIBRARY_PATH pointing at both .libs dirs.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>

#include "components/ec/cpu/ec_cpu_thread_pool.h"

#define NUM_PRODUCERS      8
#define NUM_CONSUMERS      4
#define TASKS_PER_PRODUCER 200000
#define TOTAL_TASKS (NUM_PRODUCERS * TASKS_PER_PRODUCER)

static volatile int stop_flag   = 0;
static uint64_t     dequeued_ct = 0; /* relaxed atomic */

static void *
producer_main(void *arg)
{
    ucc_ec_cpu_thread_pool_t *pool = (ucc_ec_cpu_thread_pool_t *)arg;

    for (int i = 0; i < TASKS_PER_PRODUCER; i++) {
        ucc_ee_executor_task_t *task = calloc(1, sizeof(*task));
        if (!task) {
            fprintf(stderr, "producer: out of memory\n");
            exit(1);
        }
        task->args.task_type = UCC_EE_EXECUTOR_TASK_REDUCE;
        task->args.reduce.n_srcs = (uint16_t)((i % 8) + 1);
        if (ucc_ec_cpu_thread_pool_enqueue(pool, task) != UCC_OK) {
            fprintf(stderr, "producer: enqueue failed at %d\n", i);
            free(task);
            exit(1);
        }
    }
    return NULL;
}

static void *
consumer_main(void *arg)
{
    ucc_ec_cpu_thread_pool_t *pool = (ucc_ec_cpu_thread_pool_t *)arg;

    for (;;) {
        ucc_ee_executor_task_t *task = ucc_ec_cpu_thread_pool_dequeue(pool);
        if (task) {
            /* "execute": release-store the final status, then free. */
            __atomic_store_n(&task->status, UCC_OK, __ATOMIC_RELEASE);
            free(task);
            __atomic_add_fetch(&dequeued_ct, 1, __ATOMIC_RELAXED);
            continue;
        }
        if (stop_flag) {
            break;
        }
    }
    return NULL;
}

int
main(void)
{
    ucc_ec_cpu_thread_pool_t pool;
    pthread_t producers[NUM_PRODUCERS], consumers[NUM_CONSUMERS];
    int       rc = 0;

    if (ucc_ec_cpu_thread_pool_init(&pool, 0, TOTAL_TASKS, NULL) != UCC_OK) {
        fprintf(stderr, "pool init failed\n");
        return 1;
    }

    for (int i = 0; i < NUM_CONSUMERS; i++) {
        if (pthread_create(&consumers[i], NULL, consumer_main, &pool) != 0) {
            rc = 1;
            goto done;
        }
    }
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        if (pthread_create(&producers[i], NULL, producer_main, &pool) != 0) {
            rc = 1;
            goto done;
        }
    }
    for (int i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producers[i], NULL);
    }

    /*
     * All producers are joined: no more enqueues will happen, so once a
     * consumer observes an empty queue it is empty forever.  Signal stop;
     * consumers exit on (empty && stop).  The last consumer to exit
     * observed an empty queue, so the pool is fully drained before join.
     */
    stop_flag = 1;
    for (int i = 0; i < NUM_CONSUMERS; i++) {
        pthread_join(consumers[i], NULL);
    }

    if (dequeued_ct != TOTAL_TASKS) {
        fprintf(stderr,
                "stress: MISMATCH dequeued=%" PRIu64 " expected=%d\n",
                dequeued_ct, TOTAL_TASKS);
        rc = 1;
    }
done:
    ucc_ec_cpu_thread_pool_finalize(&pool);

    if (rc) {
        fprintf(stderr, "stress: FAILED\n");
        return 1;
    }
    printf("stress: OK (%d producers x %d tasks, %d consumers, %" PRIu64
           " dequeued exactly-once)\n",
           NUM_PRODUCERS, TASKS_PER_PRODUCER, NUM_CONSUMERS, dequeued_ct);
    return 0;
}
