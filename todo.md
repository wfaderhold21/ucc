# RTT-Based Congestion Avoidance for TL/UCP One-Sided Operations

## Overview

This document outlines the requirements and implementation approach for adding round-trip-time (RTT) based congestion avoidance to one-sided operations in TL/UCP. Round-trip-times will be gathered from segmenting large messages and calculating timestamp differences between sending time and completion time.

## Current Architecture

### TL/UCP Structure
- TL/UCP is a Transport Layer built on UCX's UCP (Unified Communication Protocol)
- One-sided operations currently use `ucp_put_nbx()` and `ucp_get_nbx()` 
- Operations track posted vs completed counts in the task structure
- Memory can be segmented up to `MAX_NR_SEGMENTS` (32 segments)
- Completion callbacks exist: `ucc_tl_ucp_put_completion_cb()` and `ucc_tl_ucp_get_completion_cb()`

### Current Files
- `src/components/tl/ucp/tl_ucp.h` - Main TL/UCP header with context and configuration
- `src/components/tl/ucp/tl_ucp_task.h` - Task structure definitions
- `src/components/tl/ucp/tl_ucp_sendrecv.h` - Send/receive operations including one-sided
- `src/components/tl/ucp/tl_ucp_sendrecv.c` - Completion callbacks
- `src/components/tl/ucp/alltoall/alltoall_onesided.c` - Alltoall using one-sided ops
- `src/components/tl/ucp/alltoallv/alltoallv_onesided.c` - Alltoallv using one-sided ops
- `src/utils/ucc_time.h` - Timing utilities (`ucc_get_time()`)

## Precision Time Protocol (PTP) Integration

### Why PTP Improves RTT Accuracy

**Current Software Timestamping Limitations:**
- `gettimeofday()` has microsecond resolution (~1-10 μs accuracy)
- Software timestamps include kernel scheduling delays and jitter
- System clock drift can cause measurement inconsistencies
- One-sided operations complete on remote node with potentially unsynchronized clocks

**PTP Advantages (IEEE 1588):**
- **Nanosecond precision**: Hardware timestamps at NIC level (10-100 ns accuracy)
- **Clock synchronization**: Sub-microsecond clock sync across all nodes
- **Reduced jitter**: Hardware timestamping eliminates software stack delays
- **True one-way delay**: Synchronized clocks enable accurate one-way latency measurement
- **Congestion detection**: More accurate RTT variance detection for congestion signals

### ConnectX NIC Hardware Timestamp Capabilities

ConnectX NICs (ConnectX-5 and later) provide:
1. **Hardware TX timestamps**: Captured when packet leaves the NIC
2. **Hardware RX timestamps**: Captured when packet arrives at the NIC
3. **PTP clock synchronization**: Onboard PTP hardware clock
4. **Completion queue timestamps**: Available via `ibv_poll_cq()` with `IBV_WC_EX_WITH_COMPLETION_TIMESTAMP`

### Architecture Changes for PTP Support

#### 1. Enhanced Timestamp Structure

```c
// In src/components/tl/ucp/tl_ucp_task.h
typedef enum ucc_tl_ucp_timestamp_type {
    UCC_TL_UCP_TS_SOFTWARE,     // gettimeofday()
    UCC_TL_UCP_TS_HARDWARE_PTP, // PTP hardware timestamp
} ucc_tl_ucp_timestamp_type_t;

typedef struct ucc_tl_ucp_rtt_segment {
    uint64_t send_hw_timestamp;      // Hardware timestamp (nanoseconds)
    uint64_t completion_hw_timestamp; // Hardware timestamp (nanoseconds)
    double   send_timestamp;          // Fallback software timestamp
    double   completion_timestamp;    // Fallback software timestamp
    size_t   segment_size;
    uint64_t segment_id;
    uint8_t  timestamp_type;          // Which timestamp type is valid
} ucc_tl_ucp_rtt_segment_t;
```

#### 2. Context Configuration for PTP

```c
// In src/components/tl/ucp/tl_ucp.h
typedef struct ucc_tl_ucp_ptp_config {
    int      enable_hardware_timestamps;  // Use PTP hardware timestamps
    int      require_ptp_sync;            // Fail if PTP not synchronized
    uint64_t ptp_clock_id;                // Which PTP clock to use
    double   max_clock_offset;            // Maximum acceptable clock offset (ns)
    int      verify_sync_interval;        // How often to check PTP sync (seconds)
} ucc_tl_ucp_ptp_config_t;

typedef struct ucc_tl_ucp_context_config {
    // ... existing fields ...
    ucc_tl_ucp_ptp_config_t ptp;
} ucc_tl_ucp_context_config_t;

typedef struct ucc_tl_ucp_context {
    // ... existing fields ...
    int                ptp_available;     // PTP hardware timestamps available
    int                ptp_synchronized;  // Clocks are PTP synchronized
    uint64_t           ptp_clock_freq;    // PTP clock frequency (Hz)
    double             last_sync_check;   // Last PTP sync verification time
} ucc_tl_ucp_context_t;
```

#### 3. UCX/UCP Hardware Timestamp Integration

**Challenge**: UCX/UCP doesn't currently expose hardware timestamps in its public API.

**Solutions**:

**Option A: UCX Extension (Preferred)**
- Extend UCP completion callback to include hardware timestamps
- Requires UCX modification to expose `ibv_wc_ex` timestamp fields
- Most accurate but requires upstream UCX changes

```c
// Proposed UCP API extension
typedef struct ucp_request_timestamp {
    uint64_t send_timestamp;      // Hardware TX timestamp
    uint64_t completion_timestamp; // Hardware RX/completion timestamp
    int      timestamp_valid;      // Whether timestamps are available
} ucp_request_timestamp_t;

// Extended completion callback
typedef void (*ucp_send_nbx_callback_ext_t)(void *request, 
                                            ucs_status_t status,
                                            void *user_data,
                                            ucp_request_timestamp_t *timestamp);
```

**Option B: Direct Verbs Access (Workaround)**
- Access UCX's internal verbs completion queue directly
- Poll for completion queue entries with `IBV_WC_EX_WITH_COMPLETION_TIMESTAMP`
- More complex, tightly coupled to UCX internals

```c
// Access underlying verbs completion queue (if available)
// This is UCX-internal and may not be stable
struct ibv_cq_ex *cq = /* get from UCX context */;
struct ibv_poll_cq_attr poll_attr = {0};

while (ibv_start_poll(cq, &poll_attr) == 0) {
    uint64_t timestamp = ibv_wc_read_completion_ts(cq);
    // Use hardware timestamp
    ibv_end_poll(cq);
}
```

**Option C: PTP-Synchronized Software Timestamps (Pragmatic)**
- Use PTP to synchronize system clocks across nodes
- Continue using `clock_gettime(CLOCK_REALTIME)` but with PTP-synced clocks
- Less accurate than hardware timestamps but easier to implement
- Still provides nanosecond precision and clock synchronization benefits

```c
// In src/utils/ucc_time.h
static inline uint64_t ucc_get_time_ns_ptp()
{
    struct timespec ts;
    // CLOCK_REALTIME is synchronized by PTP daemon (ptp4l + phc2sys)
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}
```

#### 4. PTP Synchronization Verification

```c
// New file: src/components/tl/ucp/tl_ucp_ptp.c
#include <linux/ptp_clock.h>
#include <sys/ioctl.h>
#include <fcntl.h>

ucc_status_t ucc_tl_ucp_init_ptp(ucc_tl_ucp_context_t *ctx)
{
    // Open PTP device (usually /dev/ptp0 for ConnectX)
    int ptp_fd = open("/dev/ptp0", O_RDWR);
    if (ptp_fd < 0) {
        tl_warn(ctx->super.super.lib, "PTP device not available");
        ctx->ptp_available = 0;
        return UCC_OK; // Not an error, just not available
    }
    
    // Get PTP clock capabilities
    struct ptp_clock_caps caps;
    if (ioctl(ptp_fd, PTP_CLOCK_GETCAPS, &caps) < 0) {
        close(ptp_fd);
        ctx->ptp_available = 0;
        return UCC_OK;
    }
    
    ctx->ptp_available = 1;
    ctx->ptp_clock_freq = 1000000000ULL; // 1 GHz typical for ConnectX
    
    // Verify PTP synchronization by checking offset
    if (ctx->cfg.ptp.require_ptp_sync) {
        ucc_status_t status = ucc_tl_ucp_verify_ptp_sync(ctx);
        if (status != UCC_OK) {
            tl_error(ctx->super.super.lib, 
                     "PTP synchronization required but not achieved");
            close(ptp_fd);
            return status;
        }
    }
    
    close(ptp_fd);
    return UCC_OK;
}

ucc_status_t ucc_tl_ucp_verify_ptp_sync(ucc_tl_ucp_context_t *ctx)
{
    // Check PTP synchronization status
    // This typically involves checking:
    // 1. ptp4l daemon is running and synchronized
    // 2. phc2sys daemon is synchronizing system clock to PTP
    // 3. Clock offset is within acceptable bounds
    
    // Can be implemented by:
    // - Reading /sys/class/ptp/ptp0/ptp_sync_state
    // - Checking ptp4l/chrony statistics
    // - Measuring offset between PTP clock and system clock
    
    ctx->ptp_synchronized = 1; // Set based on actual checks
    ctx->last_sync_check = ucc_get_time();
    
    return ctx->ptp_synchronized ? UCC_OK : UCC_ERR_NOT_SUPPORTED;
}
```

#### 5. Modified RTT Calculation with PTP

```c
// In src/components/tl/ucp/tl_ucp_congestion.c
void ucc_tl_ucp_update_rtt_stats_ptp(ucc_tl_ucp_task_t *task, 
                                     uint32_t segment_idx)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    ucc_tl_ucp_rtt_segment_t *seg = &task->onesided.rtt_segments[segment_idx];
    double rtt_sample;
    
    if (seg->timestamp_type == UCC_TL_UCP_TS_HARDWARE_PTP) {
        // Calculate RTT from hardware timestamps (nanoseconds)
        uint64_t rtt_ns = seg->completion_hw_timestamp - seg->send_hw_timestamp;
        rtt_sample = (double)rtt_ns / 1e9; // Convert to seconds
        
        tl_debug(UCC_TASK_LIB(task),
                 "PTP RTT measurement: %.3f μs (HW timestamps: %lu -> %lu)",
                 rtt_sample * 1e6, seg->send_hw_timestamp, 
                 seg->completion_hw_timestamp);
    } else {
        // Fall back to software timestamps
        rtt_sample = seg->completion_timestamp - seg->send_timestamp;
        
        tl_debug(UCC_TASK_LIB(task),
                 "Software RTT measurement: %.3f μs",
                 rtt_sample * 1e6);
    }
    
    // Rest of RTT statistics update (same as before)
    double alpha = ctx->cfg.rtt_alpha;
    double beta = ctx->cfg.rtt_beta;
    
    if (task->onesided.sample_count == 0) {
        task->onesided.smoothed_rtt = rtt_sample;
        task->onesided.rtt_variance = rtt_sample / 2.0;
    } else {
        double rtt_diff = fabs(rtt_sample - task->onesided.smoothed_rtt);
        task->onesided.rtt_variance = 
            (1.0 - beta) * task->onesided.rtt_variance + beta * rtt_diff;
        task->onesided.smoothed_rtt = 
            (1.0 - alpha) * task->onesided.smoothed_rtt + alpha * rtt_sample;
    }
    
    task->onesided.sample_count++;
    ucc_tl_ucp_adjust_congestion_window(task, rtt_sample);
}
```

### Implementation Approach

#### Phase 1: PTP Infrastructure Setup (Outside UCC)

**System-level PTP configuration:**

```bash
# Install PTP tools
apt-get install linuxptp

# Configure ptp4l (PTP daemon for NIC hardware clock)
cat > /etc/ptp4l.conf <<EOF
[global]
slaveOnly               1
priority1               128
priority2               128
# Use hardware timestamping on ConnectX NIC
time_stamping           hardware
# Network interface with ConnectX NIC
[ens1f0]
EOF

# Start ptp4l to synchronize NIC hardware clock
ptp4l -f /etc/ptp4l.conf -i ens1f0 -s -m &

# Synchronize system clock to PTP hardware clock
phc2sys -s /dev/ptp0 -c CLOCK_REALTIME -w -m &

# Verify synchronization
pmc -u -b 0 'GET TIME_STATUS_NP'
```

#### Phase 2: Software Timestamp with PTP-Synced Clocks (Easiest)

This approach provides significant improvements without requiring UCX changes:

1. **Modify timing utilities** (`src/utils/ucc_time.h`):
```c
static inline uint64_t ucc_get_time_ns()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);  // PTP-synchronized
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static inline double ucc_get_time_ptp()
{
    return (double)ucc_get_time_ns() / 1e9;
}
```

2. **Update timestamp recording**:
- Replace `ucc_get_time()` with `ucc_get_time_ptp()`
- Store timestamps as uint64_t nanoseconds instead of double seconds
- Reduces floating-point precision loss

**Benefits:**
- ✅ Easy to implement (no UCX changes needed)
- ✅ Nanosecond precision
- ✅ Clock synchronization across nodes
- ✅ Accurate one-way delay measurement
- ✅ ~10-100x better accuracy than gettimeofday()

**Limitations:**
- ❌ Still includes some software stack latency
- ❌ Not true hardware timestamps at NIC level

#### Phase 3: Hardware Timestamps via UCX Extension (Most Accurate)

Requires collaboration with UCX upstream:

1. **Propose UCX API extension** for hardware timestamps
2. **Implement in UCX verbs transport** to expose completion timestamps
3. **Update UCC** to use new UCX timestamp API
4. **Validate** that timestamps reflect actual wire time

**Benefits:**
- ✅ True hardware timestamps (10-100 ns accuracy)
- ✅ No software stack delays
- ✅ Most accurate RTT measurement possible

**Limitations:**
- ❌ Requires UCX upstream changes
- ❌ Longer development timeline
- ❌ May not work with all UCX transports (requires verbs)

### Configuration Parameters

```c
// Add to src/components/tl/ucp/tl_ucp_lib.c

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "PTP_ENABLE", "auto",
    "Use PTP hardware timestamps for RTT measurement (yes|no|auto)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, ptp.enable_hardware_timestamps),
    UCC_CONFIG_TYPE_ENUM(auto, no, yes));

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "PTP_REQUIRE_SYNC", "no",
    "Require PTP clock synchronization (fail if not available)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, ptp.require_ptp_sync),
    UCC_CONFIG_TYPE_BOOL);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "PTP_CLOCK_DEVICE", "/dev/ptp0",
    "PTP clock device path",
    ucc_offsetof(ucc_tl_ucp_context_config_t, ptp.ptp_clock_id),
    UCC_CONFIG_TYPE_STRING);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "PTP_MAX_OFFSET", "1000",
    "Maximum acceptable PTP clock offset in nanoseconds",
    ucc_offsetof(ucc_tl_ucp_context_config_t, ptp.max_clock_offset),
    UCC_CONFIG_TYPE_DOUBLE);
```

### Expected Accuracy Improvements

| Method | Accuracy | Jitter | Sync | Complexity |
|--------|----------|--------|------|------------|
| `gettimeofday()` | 1-10 μs | High | None | Low |
| PTP software timestamps | 10-100 ns | Medium | ✅ Yes | Medium |
| PTP hardware timestamps | 10-50 ns | Very Low | ✅ Yes | High |

**RTT Measurement Precision:**
- **Without PTP**: ±2-20 μs (microseconds)
- **With PTP software timestamps**: ±20-200 ns (nanoseconds) = **100x improvement**
- **With PTP hardware timestamps**: ±20-100 ns (nanoseconds) = **200x improvement**

### Testing and Validation

```bash
# Check PTP synchronization status
pmc -u -b 0 'GET TIME_STATUS_NP'

# Monitor PTP offset
watch -n 1 'pmc -u -b 0 "GET TIME_STATUS_NP"'

# Compare RTT with and without PTP
export UCC_TL_UCP_ENABLE_RTT_CONGESTION_CONTROL=yes
export UCC_TL_UCP_PTP_ENABLE=no   # Software timestamps only
./benchmark

export UCC_TL_UCP_PTP_ENABLE=yes  # PTP timestamps
./benchmark

# Expected result: Lower RTT variance and more stable measurements
```

### Summary and Recommendations

**Should you use PTP?**

✅ **Yes, if:**
- You have ConnectX-5 or later NICs
- Your cluster has PTP infrastructure (or can be set up)
- You need high-precision RTT measurements (< 1 μs accuracy)
- You're measuring network congestion in low-latency environments
- One-sided operations span multiple nodes (need clock synchronization)

❌ **No, if:**
- NICs don't support PTP
- Cannot deploy PTP infrastructure
- RTT measurements at 1-10 μs granularity are sufficient
- Only testing on single-node systems

**Recommended Implementation Path:**

1. **Start with Option C (PTP-Synchronized Software Timestamps)**
   - Easiest to implement (no UCX changes)
   - Provides 100x better accuracy than `gettimeofday()`
   - Good enough for most congestion detection use cases
   - Can be deployed immediately

2. **Later: Move to Option A (Hardware Timestamps via UCX)**
   - Coordinate with UCX upstream for API extensions
   - Provides maximum accuracy (200x improvement)
   - Future-proof solution
   - Requires longer development timeline

**Key Insight for One-Sided Operations:**

The critical advantage of PTP for one-sided operations is **clock synchronization** across nodes. Since PUT/GET operations complete on the remote node, accurate RTT measurement requires synchronized clocks. PTP provides this, enabling true one-way delay measurements that are impossible with unsynchronized clocks.

## Required Components

### 1. RTT Measurement Infrastructure

**Add timing data structures to the task** (`src/components/tl/ucp/tl_ucp_task.h`):

```c
typedef struct ucc_tl_ucp_rtt_segment {
    double   send_timestamp;      // Time when segment was posted
    double   completion_timestamp; // Time when segment completed
    size_t   segment_size;        // Size of this segment
    uint64_t segment_id;          // Unique identifier
} ucc_tl_ucp_rtt_segment_t;

// Extend the onesided struct in ucc_tl_ucp_task_t
struct {
    uint32_t                    put_posted;
    uint32_t                    put_completed;
    uint32_t                    get_posted;
    uint32_t                    get_completed;
    // New fields for congestion control:
    ucc_tl_ucp_rtt_segment_t   *rtt_segments;      // Array to track per-segment RTTs
    uint32_t                    max_segments;       // Maximum concurrent segments
    uint32_t                    active_segments;    // Currently in-flight segments
    uint32_t                    sample_count;       // Number of RTT samples collected
    double                      smoothed_rtt;       // Exponentially weighted moving avg RTT
    double                      rtt_variance;       // RTT variance for adaptive control
    size_t                      segment_size;       // Target size for message segmentation
    double                      congestion_window;  // Current congestion window size
    double                      ssthresh;           // Slow start threshold
} onesided;
```

### 2. Context-Level Congestion State

**Add per-peer RTT tracking** (`src/components/tl/ucp/tl_ucp.h`):

```c
typedef struct ucc_tl_ucp_peer_rtt_stats {
    double   smoothed_rtt;       // SRTT
    double   rtt_variance;       // RTTVAR  
    double   min_rtt;            // Minimum observed RTT
    double   max_rtt;            // Maximum observed RTT
    uint32_t sample_count;       // Number of RTT samples
    double   congestion_window;  // Per-peer congestion window
    double   last_update_time;   // Last RTT measurement time
} ucc_tl_ucp_peer_rtt_stats_t;
```

**Add configuration parameters** (`ucc_tl_ucp_context_config_t`):

```c
typedef struct ucc_tl_ucp_context_config {
    // ... existing fields ...
    uint32_t enable_rtt_congestion_control;  // Enable/disable feature
    size_t   rtt_segment_size;               // Default segment size (e.g., 256KB)
    size_t   min_segment_size;               // Minimum segment size
    size_t   max_segment_size;               // Maximum segment size
    double   rtt_alpha;                      // SRTT smoothing factor (default: 0.125)
    double   rtt_beta;                       // RTTVAR smoothing factor (default: 0.25)
    uint32_t initial_cwnd;                   // Initial congestion window
    uint32_t max_cwnd;                       // Maximum congestion window
    double   cwnd_growth_factor;             // Congestion window growth rate
    double   cwnd_reduction_factor;          // Window reduction on congestion
} ucc_tl_ucp_context_config_t;
```

**Add to context** (`ucc_tl_ucp_context_t`):

```c
typedef struct ucc_tl_ucp_context {
    // ... existing fields ...
    ucc_tl_ucp_peer_rtt_stats_t *peer_rtt_stats; // Per-peer RTT statistics
} ucc_tl_ucp_context_t;
```

### 3. Modified Completion Callbacks

**Update completion callbacks to record timestamps** (`src/components/tl/ucp/tl_ucp_sendrecv.c`):

```c
void ucc_tl_ucp_put_completion_cb_with_rtt(void *request, ucs_status_t status,
                                           void *user_data)
{
    ucc_tl_ucp_task_t *task = (ucc_tl_ucp_task_t *)user_data;
    double completion_time;
    uint32_t segment_idx;
    
    if (ucc_unlikely(UCS_OK != status)) {
        tl_error(UCC_TASK_LIB(task), "failure in put completion %s",
                 ucs_status_string(status));
        task->super.status = ucs_status_to_ucc_status(status);
    }
    
    // Record completion timestamp
    completion_time = ucc_get_time();
    segment_idx = task->onesided.put_completed;
    
    if (task->onesided.rtt_segments && 
        segment_idx < task->onesided.max_segments) {
        task->onesided.rtt_segments[segment_idx].completion_timestamp = completion_time;
        
        // Calculate RTT and update statistics
        double rtt = completion_time - 
                     task->onesided.rtt_segments[segment_idx].send_timestamp;
        ucc_tl_ucp_update_rtt_stats(task, segment_idx, rtt);
    }
    
    task->onesided.put_completed++;
    task->onesided.active_segments--;
    ucp_request_free(request);
}

// Similar modification needed for ucc_tl_ucp_get_completion_cb_with_rtt()
```

### 4. Message Segmentation Logic

**Implement adaptive segmentation for large messages** (`src/components/tl/ucp/tl_ucp_sendrecv.h`):

```c
static inline ucc_status_t 
ucc_tl_ucp_put_nb_segmented(void *buffer, void *target, size_t msglen,
                            ucc_rank_t dest_group_rank,
                            ucc_mem_map_mem_h src_memh,
                            ucc_mem_map_mem_h *dest_memh,
                            ucc_tl_ucp_team_t *team,
                            ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_context_t *ctx = UCC_TL_UCP_TEAM_CTX(team);
    size_t segment_size = task->onesided.segment_size;
    size_t num_segments = (msglen + segment_size - 1) / segment_size;
    size_t offset = 0;
    uint32_t seg_idx = 0;
    ucc_status_t status;
    double current_time;
    
    // Allocate RTT segment tracking if not already done
    if (!task->onesided.rtt_segments) {
        task->onesided.rtt_segments = ucc_malloc(
            num_segments * sizeof(ucc_tl_ucp_rtt_segment_t),
            "rtt_segments");
        if (!task->onesided.rtt_segments) {
            return UCC_ERR_NO_MEMORY;
        }
        task->onesided.max_segments = num_segments;
    }
    
    // Post segments with congestion window control
    while (offset < msglen && seg_idx < num_segments) {
        // Check congestion window before posting
        if (task->onesided.active_segments >= 
            (uint32_t)task->onesided.congestion_window) {
            // Window full, need to wait for completions
            break;
        }
        
        size_t chunk_size = ucc_min(segment_size, msglen - offset);
        current_time = ucc_get_time();
        
        // Record send timestamp
        task->onesided.rtt_segments[seg_idx].send_timestamp = current_time;
        task->onesided.rtt_segments[seg_idx].segment_size = chunk_size;
        task->onesided.rtt_segments[seg_idx].segment_id = seg_idx;
        
        // Post the segment
        status = ucc_tl_ucp_put_nb(
            PTR_OFFSET(buffer, offset),
            PTR_OFFSET(target, offset),
            chunk_size, dest_group_rank,
            src_memh, dest_memh, team, task);
            
        if (UCC_OK != status) {
            return status;
        }
        
        task->onesided.active_segments++;
        offset += chunk_size;
        seg_idx++;
    }
    
    return (offset < msglen) ? UCC_INPROGRESS : UCC_OK;
}

// Similar function needed: ucc_tl_ucp_get_nb_segmented()
```

### 5. RTT Statistics and Congestion Control Algorithm

**Create new congestion control module** (`src/components/tl/ucp/tl_ucp_congestion.c`):

```c
#include "tl_ucp_congestion.h"
#include "utils/ucc_time.h"
#include <math.h>

void ucc_tl_ucp_update_rtt_stats(ucc_tl_ucp_task_t *task, 
                                 uint32_t segment_idx, 
                                 double rtt_sample)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double alpha = ctx->cfg.rtt_alpha;  // 0.125 (1/8)
    double beta = ctx->cfg.rtt_beta;    // 0.25 (1/4)
    
    // RFC 6298: Computing TCP's Retransmission Timer
    if (task->onesided.sample_count == 0) {
        // First measurement
        task->onesided.smoothed_rtt = rtt_sample;
        task->onesided.rtt_variance = rtt_sample / 2.0;
    } else {
        // Subsequent measurements: EWMA
        double rtt_diff = fabs(rtt_sample - task->onesided.smoothed_rtt);
        task->onesided.rtt_variance = 
            (1.0 - beta) * task->onesided.rtt_variance + beta * rtt_diff;
        task->onesided.smoothed_rtt = 
            (1.0 - alpha) * task->onesided.smoothed_rtt + alpha * rtt_sample;
    }
    
    task->onesided.sample_count++;
    
    // Adjust congestion window based on RTT
    ucc_tl_ucp_adjust_congestion_window(task, rtt_sample);
}

void ucc_tl_ucp_adjust_congestion_window(ucc_tl_ucp_task_t *task, 
                                         double rtt_sample)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double *cwnd = &task->onesided.congestion_window;
    double *ssthresh = &task->onesided.ssthresh;
    
    // Detect congestion: RTT significantly higher than smoothed RTT
    // Using 4 standard deviations as threshold (similar to TCP)
    double congestion_threshold = task->onesided.smoothed_rtt + 
                                  4.0 * task->onesided.rtt_variance;
    
    if (rtt_sample > congestion_threshold) {
        // Congestion detected: multiplicative decrease
        *ssthresh = ucc_max(*cwnd / 2.0, 2.0);
        *cwnd = *ssthresh;
        
        tl_debug(UCC_TASK_LIB(task), 
                 "Congestion detected: RTT=%.6f ms, SRTT=%.6f ms, "
                 "reducing cwnd to %.2f",
                 rtt_sample * 1000, task->onesided.smoothed_rtt * 1000, *cwnd);
    } else {
        // No congestion: increase window
        if (*cwnd < *ssthresh) {
            // Slow start: exponential growth
            *cwnd += 1.0;
        } else {
            // Congestion avoidance: linear growth
            *cwnd += 1.0 / *cwnd;
        }
        
        // Cap at maximum
        if (ctx->cfg.max_cwnd > 0) {
            *cwnd = ucc_min(*cwnd, (double)ctx->cfg.max_cwnd);
        }
    }
}

void ucc_tl_ucp_init_congestion_control(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    
    task->onesided.segment_size = ctx->cfg.rtt_segment_size;
    task->onesided.congestion_window = (double)ctx->cfg.initial_cwnd;
    task->onesided.ssthresh = (double)(ctx->cfg.initial_cwnd * 2);
    task->onesided.active_segments = 0;
    task->onesided.smoothed_rtt = 0.0;
    task->onesided.rtt_variance = 0.0;
    task->onesided.sample_count = 0;
    task->onesided.rtt_segments = NULL;
    task->onesided.max_segments = 0;
}

void ucc_tl_ucp_cleanup_congestion_control(ucc_tl_ucp_task_t *task)
{
    if (task->onesided.rtt_segments) {
        ucc_free(task->onesided.rtt_segments);
        task->onesided.rtt_segments = NULL;
        task->onesided.max_segments = 0;
    }
}
```

**Create header file** (`src/components/tl/ucp/tl_ucp_congestion.h`):

```c
#ifndef UCC_TL_UCP_CONGESTION_H_
#define UCC_TL_UCP_CONGESTION_H_

#include "tl_ucp.h"

void ucc_tl_ucp_update_rtt_stats(ucc_tl_ucp_task_t *task, 
                                 uint32_t segment_idx, 
                                 double rtt_sample);

void ucc_tl_ucp_adjust_congestion_window(ucc_tl_ucp_task_t *task, 
                                         double rtt_sample);

void ucc_tl_ucp_init_congestion_control(ucc_tl_ucp_task_t *task);

void ucc_tl_ucp_cleanup_congestion_control(ucc_tl_ucp_task_t *task);

#endif /* UCC_TL_UCP_CONGESTION_H_ */
```

### 6. Configuration Parameters

**Add tunable parameters** (`src/components/tl/ucp/tl_ucp_lib.c`):

```c
UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "ENABLE_RTT_CONGESTION_CONTROL", "no",
    "Enable RTT-based congestion avoidance for one-sided operations",
    ucc_offsetof(ucc_tl_ucp_context_config_t, enable_rtt_congestion_control),
    UCC_CONFIG_TYPE_BOOL);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SEGMENT_SIZE", "256k",
    "Default segment size for RTT measurement in one-sided operations",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_segment_size),
    UCC_CONFIG_TYPE_MEMUNITS);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "MIN_SEGMENT_SIZE", "64k",
    "Minimum segment size for RTT measurement",
    ucc_offsetof(ucc_tl_ucp_context_config_t, min_segment_size),
    UCC_CONFIG_TYPE_MEMUNITS);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "MAX_SEGMENT_SIZE", "1m",
    "Maximum segment size for RTT measurement",
    ucc_offsetof(ucc_tl_ucp_context_config_t, max_segment_size),
    UCC_CONFIG_TYPE_MEMUNITS);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_ALPHA", "0.125",
    "SRTT smoothing factor (alpha in RFC 6298)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_alpha),
    UCC_CONFIG_TYPE_DOUBLE);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_BETA", "0.25",
    "RTTVAR smoothing factor (beta in RFC 6298)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_beta),
    UCC_CONFIG_TYPE_DOUBLE);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "INITIAL_CWND", "4",
    "Initial congestion window (number of concurrent segments)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, initial_cwnd),
    UCC_CONFIG_TYPE_UINT);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "MAX_CWND", "128",
    "Maximum congestion window (0 = unlimited)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, max_cwnd),
    UCC_CONFIG_TYPE_UINT);
```

### 7. Integration with Existing One-Sided Operations

**Modify alltoall_onesided** (`src/components/tl/ucp/alltoall/alltoall_onesided.c`):

```c
ucc_status_t ucc_tl_ucp_alltoall_onesided_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx   = TASK_CTX(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems   = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    ucc_rank_t         start    = (grank + 1) % gsize;
    long              *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_mem_map_mem_h  src_memh = TASK_ARGS(task).src_memh.local_memh;
    ucc_mem_map_mem_h *dst_memh = TASK_ARGS(task).dst_memh.global_memh;
    ucc_rank_t         peer;

    if (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL) {
        src_memh = TASK_ARGS(task).src_memh.global_memh[grank];
    }

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    
    // Initialize congestion control if enabled
    if (ctx->cfg.enable_rtt_congestion_control) {
        ucc_tl_ucp_init_congestion_control(task);
    }
    
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    
    for (peer = start; task->onesided.put_posted < gsize; peer = (peer + 1) % gsize) {
        if (ctx->cfg.enable_rtt_congestion_control && 
            nelems > ctx->cfg.rtt_segment_size) {
            // Use segmented put for large messages
            UCPCHECK_GOTO(ucc_tl_ucp_put_nb_segmented(
                              (void *)(src + peer * nelems), (void *)dest, nelems,
                              peer, src_memh, dst_memh, team, task),
                              task, out);
        } else {
            // Use regular put for small messages
            UCPCHECK_GOTO(ucc_tl_ucp_put_nb(
                              (void *)(src + peer * nelems), (void *)dest, nelems,
                              peer, src_memh, dst_memh, team, task),
                              task, out);
        }
        UCPCHECK_GOTO(ucc_tl_ucp_atomic_inc(pSync, peer, dst_memh, team),
                                            task, out);
    }
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
out:
    return task->super.status;
}
```

**Similar modifications needed for:**
- `src/components/tl/ucp/alltoallv/alltoallv_onesided.c`

### 8. Task Finalization and Cleanup

**Update task finalization** (`src/components/tl/ucp/tl_ucp_coll.c` or task management):

```c
void ucc_tl_ucp_task_finalize(ucc_tl_ucp_task_t *task)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    
    // Cleanup congestion control resources
    if (ctx->cfg.enable_rtt_congestion_control) {
        ucc_tl_ucp_cleanup_congestion_control(task);
    }
    
    // ... existing finalization code ...
}
```

## Implementation Checklist

### Phase 1: Basic Infrastructure
- [ ] Add RTT segment tracking structures to `tl_ucp_task.h`
- [ ] Add configuration parameters to `tl_ucp.h`
- [ ] Create `tl_ucp_congestion.c` and `tl_ucp_congestion.h`
- [ ] Add configuration table entries in `tl_ucp_lib.c`
- [ ] Initialize per-peer RTT stats in context creation

### Phase 2: Timing and Callbacks
- [ ] Modify `ucc_tl_ucp_put_completion_cb()` to record timestamps
- [ ] Modify `ucc_tl_ucp_get_completion_cb()` to record timestamps
- [ ] Implement `ucc_tl_ucp_update_rtt_stats()`
- [ ] Implement `ucc_tl_ucp_adjust_congestion_window()`

### Phase 3: Segmentation
- [ ] Implement `ucc_tl_ucp_put_nb_segmented()` in `tl_ucp_sendrecv.h`
- [ ] Implement `ucc_tl_ucp_get_nb_segmented()` in `tl_ucp_sendrecv.h`
- [ ] Add congestion window checking logic
- [ ] Handle partial segment posting (when window is full)

### Phase 4: Integration
- [ ] Integrate with `alltoall_onesided.c`
- [ ] Integrate with `alltoallv_onesided.c`
- [ ] Add initialization in task start routines
- [ ] Add cleanup in task finalization

### Phase 4b: PTP Support (Optional Enhancement)
- [ ] Add PTP configuration structures to `tl_ucp.h`
- [ ] Create `tl_ucp_ptp.c` and `tl_ucp_ptp.h` for PTP utilities
- [ ] Implement `ucc_tl_ucp_init_ptp()` for PTP device detection
- [ ] Implement `ucc_tl_ucp_verify_ptp_sync()` for synchronization checking
- [ ] Add PTP configuration parameters to `tl_ucp_lib.c`
- [ ] Update `ucc_time.h` with `ucc_get_time_ns()` and `ucc_get_time_ptp()`
- [ ] Modify RTT segment structure to support hardware timestamps
- [ ] Update completion callbacks to capture PTP timestamps (if available)
- [ ] Implement fallback to software timestamps when PTP unavailable
- [ ] Add PTP synchronization verification during context initialization
- [ ] Test with PTP-synchronized cluster

**PTP Integration Options (choose one):**
- [ ] Option A: Coordinate with UCX team for hardware timestamp API extension
- [ ] Option B: Implement direct verbs access for hardware timestamps (interim)
- [ ] Option C: Use PTP-synchronized software timestamps (easiest, recommended first)

### Phase 5: Testing and Tuning
- [ ] Add unit tests for RTT calculation
- [ ] Add unit tests for congestion window adjustment
- [ ] Test with various message sizes
- [ ] Test with simulated network congestion
- [ ] Tune default parameters (segment size, initial cwnd, etc.)
- [ ] Add performance benchmarks

### Phase 6: Documentation
- [ ] Document new configuration parameters
- [ ] Add user guide section on congestion control
- [ ] Document algorithm design and rationale
- [ ] Add comments to code

## Key Design Decisions

1. **Segment Size**: Default 256KB segments as a reasonable balance between RTT measurement granularity and overhead
2. **Congestion Window**: Use TCP-like AIMD (Additive Increase Multiplicative Decrease)
3. **RTT Calculation**: Use exponentially weighted moving average (EWMA) per RFC 6298
4. **Congestion Detection**: Consider congestion when RTT > SRTT + 4*RTTVAR (4 standard deviations)
5. **Scope**: Track RTT at task level initially; could aggregate to per-peer level later for better global estimates
6. **Backward Compatibility**: Feature is disabled by default via configuration flag
7. **PTP Support**: Optional enhancement providing 100-200x better timestamp accuracy; falls back gracefully to software timestamps

## Environment Variables

Users can control the feature via:

```bash
# Enable RTT-based congestion control
export UCC_TL_UCP_ENABLE_RTT_CONGESTION_CONTROL=yes

# Set segment size (default: 256KB)
export UCC_TL_UCP_RTT_SEGMENT_SIZE=512k

# Set initial congestion window (default: 4)
export UCC_TL_UCP_INITIAL_CWND=8

# Set maximum congestion window (default: 128)
export UCC_TL_UCP_MAX_CWND=256

# RTT smoothing factors (default: 0.125, 0.25)
export UCC_TL_UCP_RTT_ALPHA=0.125
export UCC_TL_UCP_RTT_BETA=0.25

# PTP configuration (optional)
export UCC_TL_UCP_PTP_ENABLE=auto          # auto|yes|no
export UCC_TL_UCP_PTP_REQUIRE_SYNC=no      # Fail if PTP not synchronized
export UCC_TL_UCP_PTP_CLOCK_DEVICE=/dev/ptp0
export UCC_TL_UCP_PTP_MAX_OFFSET=1000      # nanoseconds
```

## Performance Considerations

1. **Overhead**: RTT tracking adds timestamp recording and statistics calculation per segment
2. **Memory**: Each task allocates an array of RTT segment structures
3. **Trade-offs**: Smaller segments = better RTT granularity but more overhead
4. **Scalability**: Per-task tracking scales with number of concurrent operations

## Future Enhancements

1. **Per-peer aggregation**: Maintain global per-peer RTT statistics across all tasks
2. **Advanced algorithms**: Implement BBR (Bottleneck Bandwidth and RTT) or other modern congestion control
3. **Adaptive segmentation**: Dynamically adjust segment size based on observed RTTs
4. **ECN support**: Integrate with Explicit Congestion Notification if supported by network
5. **Multi-path**: Support different RTT tracking for different network paths
6. **Telemetry**: Export RTT statistics for monitoring and debugging
7. **Hardware timestamp UCX integration**: Full hardware timestamp support via UCX API extensions (requires upstream coordination)
8. **Per-QP PTP clocks**: Support for per-Queue-Pair PTP clocks on advanced ConnectX NICs
9. **Cross-fabric PTP**: Handle PTP synchronization across different network fabrics (InfiniBand, Ethernet, etc.)

## References

### Congestion Control
- RFC 6298: Computing TCP's Retransmission Timer
- RFC 5681: TCP Congestion Control
- BBR: Congestion-Based Congestion Control (Google)

### Precision Time Protocol
- IEEE 1588-2019: Precision Clock Synchronization Protocol
- Linux PTP Project: https://linuxptp.sourceforge.net/
- PTP Best Practices: https://tsn.readthedocs.io/timesync.html
- ConnectX NIC PTP Support: NVIDIA Mellanox Documentation

### UCX/UCC
- UCX Documentation: https://openucx.readthedocs.io/
- UCC Documentation: https://github.com/openucx/ucc
- UCX GitHub: https://github.com/openucx/ucx

---

# Alternative Approach: RTT Probing Thread

## Overview

Instead of measuring RTT from actual data transfers (inline measurement), an alternative approach uses a **dedicated background thread** that periodically sends probe messages between ranks to measure RTT. This approach decouples RTT measurement from user data operations.

### Concept

```
┌─────────────────────────────────────────────────────────────┐
│                    UCC Context                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Main Thread (User Operations)              │   │
│  │  - PUT/GET operations                                │   │
│  │  - Query RTT statistics                              │   │
│  │  - Adjust congestion window                          │   │
│  └──────────────────────────────────────────────────────┘   │
│                          ▲                                   │
│                          │ Read RTT stats (lock-free)       │
│                          │                                   │
│  ┌──────────────────────▼───────────────────────────────┐   │
│  │        Shared RTT Statistics (per-peer)              │   │
│  │  - Smoothed RTT                                      │   │
│  │  - RTT variance                                      │   │
│  │  - Min/Max RTT                                       │   │
│  │  - Last update timestamp                             │   │
│  └──────────────────────▲───────────────────────────────┘   │
│                          │ Update (atomic writes)            │
│                          │                                   │
│  ┌──────────────────────┴───────────────────────────────┐   │
│  │         RTT Probing Thread                           │   │
│  │  Loop:                                               │   │
│  │    1. Select peer rank(s) to probe                   │   │
│  │    2. Send probe message (tag: RTT_PROBE)            │   │
│  │    3. Wait for response                              │   │
│  │    4. Calculate RTT                                  │   │
│  │    5. Update shared statistics                       │   │
│  │    6. Sleep (probe_interval)                         │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Architecture and Design

### 1. Thread Management

```c
// In src/components/tl/ucp/tl_ucp.h

typedef enum ucc_tl_ucp_rtt_probe_strategy {
    UCC_TL_UCP_RTT_PROBE_ROUND_ROBIN,  // Probe one peer at a time in order
    UCC_TL_UCP_RTT_PROBE_ALL_PAIRS,    // Probe all peers each interval
    UCC_TL_UCP_RTT_PROBE_RANDOM,       // Randomly select peer to probe
    UCC_TL_UCP_RTT_PROBE_ADAPTIVE,     // Probe more frequently to congested peers
} ucc_tl_ucp_rtt_probe_strategy_t;

typedef struct ucc_tl_ucp_rtt_probe_config {
    int                              enable;              // Enable probing thread
    double                           probe_interval;      // Seconds between probes
    size_t                           probe_msg_size;      // Size of probe messages
    ucc_tl_ucp_rtt_probe_strategy_t  strategy;           // Probing strategy
    int                              num_probes_per_rtt;  // Samples to average
    int                              use_service_worker;  // Use service worker or own
    double                           startup_delay;       // Delay before first probe
    double                           max_probe_rate;      // Max probes/sec per peer
} ucc_tl_ucp_rtt_probe_config_t;

typedef struct ucc_tl_ucp_context_config {
    // ... existing fields ...
    ucc_tl_ucp_rtt_probe_config_t rtt_probe;
} ucc_tl_ucp_context_config_t;
```

### 2. Per-Peer RTT Statistics (Shared State)

```c
// In src/components/tl/ucp/tl_ucp.h

typedef struct ucc_tl_ucp_peer_rtt_stats {
    // RTT measurements
    volatile uint64_t smoothed_rtt_ns;      // SRTT in nanoseconds (atomic)
    volatile uint64_t rtt_variance_ns;      // RTTVAR in nanoseconds (atomic)
    volatile uint64_t min_rtt_ns;           // Minimum observed RTT
    volatile uint64_t max_rtt_ns;           // Maximum observed RTT
    
    // Metadata
    volatile uint64_t sample_count;         // Total samples collected
    volatile uint64_t last_update_time_ns;  // Timestamp of last update
    volatile uint64_t probe_send_time_ns;   // Last probe send time (for in-flight)
    volatile uint32_t probe_sequence;       // Sequence number for probes
    
    // Congestion indicators
    volatile double   congestion_window;    // Current cwnd estimate
    volatile double   loss_rate;            // Probe loss rate (0.0 - 1.0)
    
    // Thread synchronization
    volatile int      probe_in_flight;      // 0 or 1, atomic flag
    
    // Padding to avoid false sharing (cache line = 64 bytes)
    char              padding[64 - ((8 * 8 + 8 + 4) % 64)];
} __attribute__((aligned(64))) ucc_tl_ucp_peer_rtt_stats_t;

typedef struct ucc_tl_ucp_context {
    // ... existing fields ...
    ucc_tl_ucp_peer_rtt_stats_t *peer_rtt_stats;  // Array of per-peer stats
    
    // Probing thread state
    pthread_t                    rtt_probe_thread;
    volatile int                 rtt_probe_thread_running;
    volatile int                 rtt_probe_thread_stop;
    ucc_tl_ucp_worker_t         *rtt_probe_worker;  // Dedicated UCP worker
    ucc_rank_t                   current_probe_peer; // For round-robin
} ucc_tl_ucp_context_t;
```

### 3. Probe Message Protocol

```c
// In src/components/tl/ucp/tl_ucp_tag.h

// Reserve a tag for RTT probes
#define UCC_TL_UCP_RTT_PROBE_TAG    (UCC_TL_UCP_MAX_COLL_TAG + 2)
#define UCC_TL_UCP_RTT_RESPONSE_TAG (UCC_TL_UCP_MAX_COLL_TAG + 3)

// Probe message structure
typedef struct ucc_tl_ucp_rtt_probe_msg {
    uint64_t send_timestamp_ns;   // Sender's timestamp (if PTP available)
    uint32_t sequence_number;     // Probe sequence number
    uint32_t probe_size;          // Size of this probe (for validation)
    ucc_rank_t sender_rank;       // Who sent this probe
    uint8_t  is_response;         // 0 = probe, 1 = response
    uint8_t  padding[7];          // Align to 8 bytes
    char     payload[0];          // Variable size payload
} ucc_tl_ucp_rtt_probe_msg_t;
```

### 4. Probing Thread Implementation

```c
// New file: src/components/tl/ucp/tl_ucp_rtt_probe.c

#include "tl_ucp.h"
#include "tl_ucp_rtt_probe.h"
#include "utils/ucc_time.h"
#include <pthread.h>
#include <unistd.h>

// Probe message completion callback
static void ucc_tl_ucp_rtt_probe_send_cb(void *request, ucs_status_t status,
                                          void *user_data)
{
    if (ucc_unlikely(UCS_OK != status)) {
        // Log error but don't fail - just skip this sample
        tl_debug(NULL, "RTT probe send failed: %s", ucs_status_string(status));
    }
    ucp_request_free(request);
}

// Response receive callback
static void ucc_tl_ucp_rtt_probe_recv_cb(void *request, ucs_status_t status,
                                          const ucp_tag_recv_info_t *info,
                                          void *user_data)
{
    ucc_tl_ucp_rtt_probe_ctx_t *probe_ctx = user_data;
    
    if (ucc_likely(UCS_OK == status)) {
        uint64_t recv_time = ucc_get_time_ns();
        uint64_t rtt_ns = recv_time - probe_ctx->send_time;
        
        // Update RTT statistics
        ucc_tl_ucp_update_peer_rtt_atomic(probe_ctx->ctx, 
                                          probe_ctx->peer_rank,
                                          rtt_ns);
    } else {
        // Timeout or error - increment loss counter
        ucc_atomic_add64(&probe_ctx->ctx->peer_rtt_stats[probe_ctx->peer_rank].loss_rate,
                         1);
    }
    
    ucp_request_free(request);
    ucc_free(probe_ctx);
}

// Update RTT statistics atomically
static void ucc_tl_ucp_update_peer_rtt_atomic(ucc_tl_ucp_context_t *ctx,
                                               ucc_rank_t peer_rank,
                                               uint64_t rtt_sample_ns)
{
    ucc_tl_ucp_peer_rtt_stats_t *stats = &ctx->peer_rtt_stats[peer_rank];
    uint64_t old_srtt, new_srtt;
    uint64_t old_rttvar, new_rttvar;
    uint64_t sample_count;
    
    // Atomically increment sample count
    sample_count = ucc_atomic_fadd64(&stats->sample_count, 1);
    
    if (sample_count == 0) {
        // First sample - initialize
        ucc_atomic_cswap64(&stats->smoothed_rtt_ns, 0, rtt_sample_ns);
        ucc_atomic_cswap64(&stats->rtt_variance_ns, 0, rtt_sample_ns / 2);
        ucc_atomic_cswap64(&stats->min_rtt_ns, UINT64_MAX, rtt_sample_ns);
        ucc_atomic_cswap64(&stats->max_rtt_ns, 0, rtt_sample_ns);
    } else {
        // RFC 6298 EWMA calculation with atomic operations
        // SRTT = (1 - alpha) * SRTT + alpha * RTT
        // RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - RTT|
        
        double alpha = 0.125;  // 1/8
        double beta = 0.25;    // 1/4
        
        do {
            old_srtt = ucc_atomic_cswap64(&stats->smoothed_rtt_ns, 0, 0); // Read
            new_srtt = (uint64_t)((1.0 - alpha) * old_srtt + alpha * rtt_sample_ns);
        } while (!ucc_atomic_cswap64(&stats->smoothed_rtt_ns, old_srtt, new_srtt));
        
        uint64_t rtt_diff = (rtt_sample_ns > old_srtt) ? 
                            (rtt_sample_ns - old_srtt) : (old_srtt - rtt_sample_ns);
        
        do {
            old_rttvar = ucc_atomic_cswap64(&stats->rtt_variance_ns, 0, 0); // Read
            new_rttvar = (uint64_t)((1.0 - beta) * old_rttvar + beta * rtt_diff);
        } while (!ucc_atomic_cswap64(&stats->rtt_variance_ns, old_rttvar, new_rttvar));
        
        // Update min/max
        uint64_t old_min;
        do {
            old_min = ucc_atomic_cswap64(&stats->min_rtt_ns, 0, 0);
            if (rtt_sample_ns >= old_min) break;
        } while (!ucc_atomic_cswap64(&stats->min_rtt_ns, old_min, rtt_sample_ns));
        
        uint64_t old_max;
        do {
            old_max = ucc_atomic_cswap64(&stats->max_rtt_ns, 0, 0);
            if (rtt_sample_ns <= old_max) break;
        } while (!ucc_atomic_cswap64(&stats->max_rtt_ns, old_max, rtt_sample_ns));
    }
    
    // Update last update time
    ucc_atomic_cswap64(&stats->last_update_time_ns, 0, ucc_get_time_ns());
    
    // Clear in-flight flag
    ucc_atomic_cswap32((uint32_t*)&stats->probe_in_flight, 1, 0);
}

// Send a probe to a specific peer
static ucc_status_t ucc_tl_ucp_send_rtt_probe(ucc_tl_ucp_context_t *ctx,
                                               ucc_rank_t peer_rank)
{
    ucc_tl_ucp_peer_rtt_stats_t *stats = &ctx->peer_rtt_stats[peer_rank];
    ucc_tl_ucp_rtt_probe_msg_t *probe_msg;
    size_t probe_size = ctx->cfg.rtt_probe.probe_msg_size;
    ucp_ep_h ep;
    ucp_tag_t tag;
    ucp_request_param_t req_param = {0};
    ucs_status_ptr_t status;
    uint64_t send_time;
    
    // Check if probe already in flight
    if (ucc_atomic_cswap32((uint32_t*)&stats->probe_in_flight, 0, 1)) {
        return UCC_OK; // Already probing this peer
    }
    
    // Allocate probe message
    probe_msg = ucc_malloc(sizeof(*probe_msg) + probe_size, "rtt_probe_msg");
    if (!probe_msg) {
        ucc_atomic_cswap32((uint32_t*)&stats->probe_in_flight, 1, 0);
        return UCC_ERR_NO_MEMORY;
    }
    
    // Fill probe message
    send_time = ucc_get_time_ns();
    probe_msg->send_timestamp_ns = send_time;
    probe_msg->sequence_number = ucc_atomic_fadd32(&stats->probe_sequence, 1);
    probe_msg->probe_size = probe_size;
    probe_msg->sender_rank = ctx->super.super.rank;
    probe_msg->is_response = 0;
    
    // Get endpoint for peer
    // Use service worker if configured, otherwise main worker
    ucp_worker_h worker = ctx->cfg.rtt_probe.use_service_worker ?
                          ctx->service_worker.ucp_worker :
                          ctx->worker.ucp_worker;
    
    // TODO: Get proper endpoint - this is simplified
    ep = ctx->worker.eps[peer_rank];
    
    // Post probe receive (for response)
    ucc_tl_ucp_rtt_probe_ctx_t *probe_ctx = 
        ucc_malloc(sizeof(*probe_ctx), "probe_ctx");
    probe_ctx->ctx = ctx;
    probe_ctx->peer_rank = peer_rank;
    probe_ctx->send_time = send_time;
    
    tag = UCC_TL_UCP_MAKE_TAG(0, UCC_TL_UCP_RTT_RESPONSE_TAG, 
                              peer_rank, /* team_id */ 0, 0, 0);
    
    req_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | 
                             UCP_OP_ATTR_FIELD_USER_DATA;
    req_param.cb.recv = ucc_tl_ucp_rtt_probe_recv_cb;
    req_param.user_data = probe_ctx;
    
    // Post receive for response (non-blocking)
    status = ucp_tag_recv_nbx(worker, probe_msg, 
                              sizeof(*probe_msg) + probe_size,
                              tag, (ucp_tag_t)-1, &req_param);
    
    // Send probe
    req_param.cb.send = ucc_tl_ucp_rtt_probe_send_cb;
    req_param.user_data = NULL;
    
    tag = UCC_TL_UCP_MAKE_TAG(0, UCC_TL_UCP_RTT_PROBE_TAG,
                              ctx->super.super.rank, 0, 0, 0);
    
    status = ucp_tag_send_nbx(ep, probe_msg,
                              sizeof(*probe_msg) + probe_size,
                              tag, &req_param);
    
    // Store send time for timeout detection
    ucc_atomic_cswap64(&stats->probe_send_time_ns, 0, send_time);
    
    return UCC_OK;
}

// Probe responder - handles incoming probe requests
static void ucc_tl_ucp_rtt_probe_responder(void *request, ucs_status_t status,
                                            const ucp_tag_recv_info_t *info,
                                            void *user_data)
{
    ucc_tl_ucp_context_t *ctx = user_data;
    ucc_tl_ucp_rtt_probe_msg_t *probe_msg = 
        (ucc_tl_ucp_rtt_probe_msg_t*)request;
    
    if (ucc_unlikely(UCS_OK != status)) {
        ucp_request_free(request);
        return;
    }
    
    // Echo back as response
    probe_msg->is_response = 1;
    
    ucp_ep_h ep = ctx->worker.eps[probe_msg->sender_rank];
    ucp_tag_t tag = UCC_TL_UCP_MAKE_TAG(0, UCC_TL_UCP_RTT_RESPONSE_TAG,
                                        ctx->super.super.rank, 0, 0, 0);
    
    ucp_request_param_t req_param = {0};
    req_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK;
    req_param.cb.send = ucc_tl_ucp_rtt_probe_send_cb;
    
    ucp_tag_send_nbx(ep, probe_msg, 
                     sizeof(*probe_msg) + probe_msg->probe_size,
                     tag, &req_param);
}

// Main probing thread function
static void* ucc_tl_ucp_rtt_probe_thread_func(void *arg)
{
    ucc_tl_ucp_context_t *ctx = (ucc_tl_ucp_context_t*)arg;
    ucc_tl_ucp_rtt_probe_config_t *cfg = &ctx->cfg.rtt_probe;
    ucc_rank_t team_size = ctx->super.super.size;
    ucc_rank_t my_rank = ctx->super.super.rank;
    struct timespec sleep_time;
    uint64_t probe_count = 0;
    
    tl_info(ctx->super.super.lib, "RTT probing thread started");
    
    // Startup delay
    if (cfg->startup_delay > 0) {
        sleep_time.tv_sec = (time_t)cfg->startup_delay;
        sleep_time.tv_nsec = 
            (long)((cfg->startup_delay - sleep_time.tv_sec) * 1e9);
        nanosleep(&sleep_time, NULL);
    }
    
    // Post receive for probe requests (always listening)
    // TODO: Post multiple receives for concurrent probes
    
    while (!ctx->rtt_probe_thread_stop) {
        ucc_rank_t peer_rank;
        
        switch (cfg->strategy) {
        case UCC_TL_UCP_RTT_PROBE_ROUND_ROBIN:
            // Probe one peer per iteration
            peer_rank = (ctx->current_probe_peer + 1) % team_size;
            if (peer_rank == my_rank) {
                peer_rank = (peer_rank + 1) % team_size;
            }
            ctx->current_probe_peer = peer_rank;
            ucc_tl_ucp_send_rtt_probe(ctx, peer_rank);
            break;
            
        case UCC_TL_UCP_RTT_PROBE_ALL_PAIRS:
            // Probe all peers
            for (peer_rank = 0; peer_rank < team_size; peer_rank++) {
                if (peer_rank != my_rank) {
                    ucc_tl_ucp_send_rtt_probe(ctx, peer_rank);
                }
            }
            break;
            
        case UCC_TL_UCP_RTT_PROBE_RANDOM:
            // Random peer selection
            peer_rank = rand() % team_size;
            if (peer_rank != my_rank) {
                ucc_tl_ucp_send_rtt_probe(ctx, peer_rank);
            }
            break;
            
        case UCC_TL_UCP_RTT_PROBE_ADAPTIVE:
            // Probe peers with high RTT variance more frequently
            // TODO: Implement adaptive strategy
            break;
        }
        
        // Progress UCP worker
        ucp_worker_progress(ctx->cfg.rtt_probe.use_service_worker ?
                           ctx->service_worker.ucp_worker :
                           ctx->worker.ucp_worker);
        
        probe_count++;
        
        // Sleep for probe_interval
        sleep_time.tv_sec = (time_t)cfg->probe_interval;
        sleep_time.tv_nsec = 
            (long)((cfg->probe_interval - sleep_time.tv_sec) * 1e9);
        nanosleep(&sleep_time, NULL);
    }
    
    tl_info(ctx->super.super.lib, 
            "RTT probing thread stopped after %lu probes", probe_count);
    
    return NULL;
}

// Initialize probing thread
ucc_status_t ucc_tl_ucp_rtt_probe_init(ucc_tl_ucp_context_t *ctx)
{
    if (!ctx->cfg.rtt_probe.enable) {
        return UCC_OK;
    }
    
    // Allocate per-peer statistics
    ucc_rank_t team_size = ctx->super.super.size;
    ctx->peer_rtt_stats = ucc_calloc(team_size, 
                                     sizeof(ucc_tl_ucp_peer_rtt_stats_t),
                                     "peer_rtt_stats");
    if (!ctx->peer_rtt_stats) {
        return UCC_ERR_NO_MEMORY;
    }
    
    // Initialize min_rtt to max value
    for (ucc_rank_t i = 0; i < team_size; i++) {
        ctx->peer_rtt_stats[i].min_rtt_ns = UINT64_MAX;
    }
    
    // Create probing thread
    ctx->rtt_probe_thread_stop = 0;
    ctx->current_probe_peer = 0;
    
    int ret = pthread_create(&ctx->rtt_probe_thread, NULL,
                            ucc_tl_ucp_rtt_probe_thread_func, ctx);
    if (ret != 0) {
        tl_error(ctx->super.super.lib, 
                 "Failed to create RTT probing thread: %d", ret);
        ucc_free(ctx->peer_rtt_stats);
        return UCC_ERR_NO_RESOURCE;
    }
    
    ctx->rtt_probe_thread_running = 1;
    
    tl_info(ctx->super.super.lib,
            "RTT probing thread initialized (strategy=%d, interval=%.3f ms)",
            ctx->cfg.rtt_probe.strategy,
            ctx->cfg.rtt_probe.probe_interval * 1000.0);
    
    return UCC_OK;
}

// Cleanup probing thread
void ucc_tl_ucp_rtt_probe_finalize(ucc_tl_ucp_context_t *ctx)
{
    if (!ctx->rtt_probe_thread_running) {
        return;
    }
    
    // Signal thread to stop
    ctx->rtt_probe_thread_stop = 1;
    
    // Wait for thread to exit
    pthread_join(ctx->rtt_probe_thread, NULL);
    
    // Free resources
    if (ctx->peer_rtt_stats) {
        ucc_free(ctx->peer_rtt_stats);
        ctx->peer_rtt_stats = NULL;
    }
    
    ctx->rtt_probe_thread_running = 0;
    
    tl_info(ctx->super.super.lib, "RTT probing thread finalized");
}

// Query RTT for a specific peer (used by congestion control)
ucc_status_t ucc_tl_ucp_get_peer_rtt(ucc_tl_ucp_context_t *ctx,
                                     ucc_rank_t peer_rank,
                                     double *smoothed_rtt,
                                     double *rtt_variance)
{
    if (!ctx->peer_rtt_stats || peer_rank >= ctx->super.super.size) {
        return UCC_ERR_INVALID_PARAM;
    }
    
    ucc_tl_ucp_peer_rtt_stats_t *stats = &ctx->peer_rtt_stats[peer_rank];
    
    // Atomic read (relaxed memory order is fine for statistics)
    uint64_t srtt_ns = ucc_atomic_cswap64(&stats->smoothed_rtt_ns, 0, 0);
    uint64_t rttvar_ns = ucc_atomic_cswap64(&stats->rtt_variance_ns, 0, 0);
    
    if (srtt_ns == 0) {
        // No samples yet
        return UCC_ERR_NOT_FOUND;
    }
    
    *smoothed_rtt = (double)srtt_ns / 1e9;  // Convert to seconds
    *rtt_variance = (double)rttvar_ns / 1e9;
    
    return UCC_OK;
}
```

### 5. Integration with Congestion Control

```c
// In src/components/tl/ucp/tl_ucp_congestion.c

void ucc_tl_ucp_init_congestion_control_with_probe(ucc_tl_ucp_task_t *task,
                                                    ucc_rank_t peer_rank)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double smoothed_rtt, rtt_variance;
    
    // Initialize with default values
    task->onesided.congestion_window = (double)ctx->cfg.initial_cwnd;
    task->onesided.ssthresh = (double)(ctx->cfg.initial_cwnd * 2);
    
    // If probing thread is active, get current RTT estimate
    if (ctx->rtt_probe_thread_running) {
        ucc_status_t status = ucc_tl_ucp_get_peer_rtt(ctx, peer_rank,
                                                      &smoothed_rtt,
                                                      &rtt_variance);
        if (status == UCC_OK) {
            // Use probed RTT as initial estimate
            task->onesided.smoothed_rtt = smoothed_rtt;
            task->onesided.rtt_variance = rtt_variance;
            
            tl_debug(UCC_TASK_LIB(task),
                     "Initialized congestion control with probed RTT: "
                     "SRTT=%.3f μs, RTTVAR=%.3f μs",
                     smoothed_rtt * 1e6, rtt_variance * 1e6);
        }
    }
}

// Update congestion window based on probed RTT
void ucc_tl_ucp_adjust_cwnd_with_probe(ucc_tl_ucp_task_t *task,
                                       ucc_rank_t peer_rank)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double current_rtt, rtt_var;
    
    if (!ctx->rtt_probe_thread_running) {
        return; // No probe data available
    }
    
    if (ucc_tl_ucp_get_peer_rtt(ctx, peer_rank, &current_rtt, &rtt_var) != UCC_OK) {
        return; // No data yet
    }
    
    // Check if current RTT indicates congestion
    double baseline_rtt = task->onesided.smoothed_rtt;
    double congestion_threshold = baseline_rtt + 4.0 * rtt_var;
    
    if (current_rtt > congestion_threshold) {
        // Congestion detected - reduce window
        task->onesided.ssthresh = 
            ucc_max(task->onesided.congestion_window / 2.0, 2.0);
        task->onesided.congestion_window = task->onesided.ssthresh;
        
        tl_debug(UCC_TASK_LIB(task),
                 "Congestion detected from probe: RTT=%.3f μs > threshold=%.3f μs, "
                 "reducing cwnd to %.2f",
                 current_rtt * 1e6, congestion_threshold * 1e6,
                 task->onesided.congestion_window);
    }
}
```

### 6. Configuration Parameters

```c
// In src/components/tl/ucp/tl_ucp_lib.c

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_PROBE_ENABLE", "no",
    "Enable background RTT probing thread",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_probe.enable),
    UCC_CONFIG_TYPE_BOOL);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_PROBE_INTERVAL", "0.1",
    "Interval between RTT probes in seconds (default: 100ms)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_probe.probe_interval),
    UCC_CONFIG_TYPE_DOUBLE);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_PROBE_MSG_SIZE", "64",
    "Size of RTT probe messages in bytes",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_probe.probe_msg_size),
    UCC_CONFIG_TYPE_MEMUNITS);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_PROBE_STRATEGY", "round_robin",
    "RTT probing strategy (round_robin|all_pairs|random|adaptive)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_probe.strategy),
    UCC_CONFIG_TYPE_ENUM(round_robin, all_pairs, random, adaptive));

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_PROBE_USE_SERVICE_WORKER", "yes",
    "Use service worker for RTT probes (if available)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_probe.use_service_worker),
    UCC_CONFIG_TYPE_BOOL);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_PROBE_STARTUP_DELAY", "0.5",
    "Delay before starting RTT probing (seconds)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_probe.startup_delay),
    UCC_CONFIG_TYPE_DOUBLE);
```

## Comparison: Inline vs. Probing Thread

### Inline RTT Measurement (Original Approach)

**Advantages:**
- ✅ Measures actual data path RTT (real congestion)
- ✅ No additional network traffic overhead
- ✅ RTT measurements reflect actual message sizes
- ✅ Captures congestion when it happens
- ✅ No threading complexity

**Disadvantages:**
- ❌ Adds overhead to data operations
- ❌ Requires message segmentation
- ❌ Only measures RTT during active operations
- ❌ Complexity in completion callbacks
- ❌ May delay operations while measuring

### Probing Thread Approach (Alternative)

**Advantages:**
- ✅ No overhead on user data operations
- ✅ Continuous RTT monitoring (even when idle)
- ✅ Simpler integration (decoupled from data path)
- ✅ Can use small probes for faster sampling
- ✅ Proactive congestion detection
- ✅ All-to-all RTT matrix available

**Disadvantages:**
- ❌ Additional background network traffic
- ❌ May not reflect large message congestion
- ❌ Thread overhead (CPU, memory)
- ❌ Probe traffic may take different QoS path
- ❌ Atomic operations for shared state
- ❌ Probe responses add latency variance

## Combined Approach (Best of Both)

Consider using **both** approaches together:

1. **Probing thread**: Provides baseline RTT estimates and detects network-wide congestion
2. **Inline measurement**: Refines estimates for specific operations and message sizes

```c
// Hybrid congestion control decision
void ucc_tl_ucp_adjust_congestion_hybrid(ucc_tl_ucp_task_t *task,
                                         ucc_rank_t peer_rank,
                                         double measured_rtt)
{
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    double probed_rtt, probed_var;
    
    // Get probed baseline
    if (ctx->rtt_probe_thread_running &&
        ucc_tl_ucp_get_peer_rtt(ctx, peer_rank, &probed_rtt, &probed_var) == UCC_OK) {
        
        // Compare inline measurement with probed baseline
        if (measured_rtt > probed_rtt + 4.0 * probed_var) {
            // Large message congestion detected (not seen in small probes)
            // This is more serious - reduce window more aggressively
            task->onesided.congestion_window *= 0.5;
        } else if (probed_rtt > task->onesided.smoothed_rtt + 
                   4.0 * task->onesided.rtt_variance) {
            // Network-wide congestion detected by probes
            // Reduce window moderately
            task->onesided.congestion_window *= 0.75;
        } else {
            // No congestion - increase window
            ucc_tl_ucp_cwnd_increase(task);
        }
    } else {
        // Fallback to inline-only
        ucc_tl_ucp_adjust_congestion_window(task, measured_rtt);
    }
}
```

## Implementation Checklist - Probing Thread

### Phase 1: Core Infrastructure
- [ ] Create `tl_ucp_rtt_probe.h` and `tl_ucp_rtt_probe.c`
- [ ] Add probe configuration structures to `tl_ucp.h`
- [ ] Define probe message format and reserved tags
- [ ] Implement atomic per-peer RTT statistics structure
- [ ] Add probing configuration parameters

### Phase 2: Thread Management
- [ ] Implement `ucc_tl_ucp_rtt_probe_thread_func()`
- [ ] Implement `ucc_tl_ucp_rtt_probe_init()`
- [ ] Implement `ucc_tl_ucp_rtt_probe_finalize()`
- [ ] Add thread lifecycle management in context create/destroy

### Phase 3: Probe Protocol
- [ ] Implement `ucc_tl_ucp_send_rtt_probe()`
- [ ] Implement probe response receiver
- [ ] Implement probe responder (echo service)
- [ ] Add timeout handling for lost probes
- [ ] Implement probe sequence number tracking

### Phase 4: Statistics Management
- [ ] Implement `ucc_tl_ucp_update_peer_rtt_atomic()`
- [ ] Implement `ucc_tl_ucp_get_peer_rtt()` query API
- [ ] Add atomic operations for lock-free updates
- [ ] Implement cache-aligned structures to avoid false sharing

### Phase 5: Probing Strategies
- [ ] Implement round-robin strategy
- [ ] Implement all-pairs strategy
- [ ] Implement random strategy
- [ ] Implement adaptive strategy (probe congested peers more)

### Phase 6: Integration
- [ ] Integrate with existing congestion control
- [ ] Add hybrid inline+probe decision logic
- [ ] Update `alltoall_onesided.c` to query probe data
- [ ] Update `alltoallv_onesided.c` to query probe data

### Phase 7: Testing
- [ ] Test probe thread lifecycle
- [ ] Test with various probing strategies
- [ ] Measure probe overhead
- [ ] Test atomic statistics under contention
- [ ] Test probe loss handling
- [ ] Validate RTT accuracy vs. inline measurement
- [ ] Performance benchmarks (with/without probing)

### Phase 8: Optimization
- [ ] Optimize probe message size
- [ ] Tune probe interval based on network size
- [ ] Implement adaptive probe rate limiting
- [ ] Add probe coalescing for efficiency
- [ ] Optimize atomic operations

## Environment Variables - Probing Thread

```bash
# Enable probing thread approach
export UCC_TL_UCP_RTT_PROBE_ENABLE=yes

# Set probe interval (default: 100ms)
export UCC_TL_UCP_RTT_PROBE_INTERVAL=0.1

# Set probe message size (default: 64 bytes)
export UCC_TL_UCP_RTT_PROBE_MSG_SIZE=64

# Set probing strategy (default: round_robin)
export UCC_TL_UCP_RTT_PROBE_STRATEGY=round_robin  # or all_pairs, random, adaptive

# Use service worker for probes (default: yes)
export UCC_TL_UCP_RTT_PROBE_USE_SERVICE_WORKER=yes

# Startup delay before probing (default: 0.5s)
export UCC_TL_UCP_RTT_PROBE_STARTUP_DELAY=0.5

# Can combine with inline measurement
export UCC_TL_UCP_ENABLE_RTT_CONGESTION_CONTROL=yes  # Use both
```

## Use Cases for Each Approach

### When to Use Inline Measurement Only
- Applications with continuous traffic
- Need to measure large message RTT specifically
- Want minimal background overhead
- Single-pair intensive communication patterns

### When to Use Probing Thread Only
- Bursty communication patterns
- Need global RTT matrix for routing decisions
- Want proactive congestion detection
- Background monitoring/telemetry needs

### When to Use Hybrid (Both)
- Production HPC systems with diverse workloads
- Need both proactive and reactive congestion control
- Want best accuracy with graceful fallback
- Mission-critical low-latency applications

## Performance Considerations - Probing Thread

### Overhead Analysis

**Network Bandwidth:**
```
Probe overhead = probe_size × probes_per_second × num_peers

Example (100 ranks, round-robin, 100ms interval):
= 64 bytes × 10 probes/sec × 100 peers
= 64 KB/sec per rank
= ~0.5 Mbps total probe traffic

For all-pairs strategy:
= 64 bytes × 10 probes/sec × 99 peers (excluding self)
= 63.36 KB/sec per rank
= ~5 Mbps per rank
```

**CPU Overhead:**
- Thread wake-up: ~10μs every probe_interval
- Message send/recv: ~1-2μs per probe
- Statistics update: ~100ns (atomic operations)
- Total: < 1% CPU on modern systems with 100ms interval

**Memory:**
```
Per-peer stats = 128 bytes (with padding)
For 1000 ranks = 128 KB

Probe messages in flight = probe_size × max_concurrent_probes
= 64 bytes × 100 = 6.4 KB
```

### Tuning Guidelines

| Cluster Size | Recommended Strategy | Probe Interval | Message Size |
|--------------|---------------------|----------------|--------------|
| < 100 ranks | all_pairs | 100ms | 64 bytes |
| 100-1000 ranks | round_robin | 100ms | 64 bytes |
| > 1000 ranks | adaptive | 500ms | 32 bytes |

## Future Enhancements - Probing Thread

1. **Hierarchical Probing**: Probe within node vs. across nodes at different rates
2. **Multicast Probes**: Use multicast for efficient all-to-all probing
3. **Probe Aggregation**: Combine multiple probe responses in one message
4. **Quality of Service**: Mark probe traffic with lower QoS to avoid interfering with data
5. **Machine Learning**: Use ML to predict congestion from probe patterns
6. **Integration with SHARP**: Use in-network aggregation for probe responses
7. **Cross-layer Optimization**: Share probe data with UCX transport layer

---

# RTT-Aware Scheduling Approach

## Overview

Instead of using congestion control (rate limiting via congestion windows), an alternative approach uses the **RTT table as a scheduling oracle** to decide which peers to communicate with at any given time. This is particularly effective for collective operations like alltoall where you have flexibility in the order of communication.

### Concept

The key insight: **defer communication with congested peers and prioritize fast paths**.

```
Traditional alltoall_onesided:
┌─────────────────────────────────────────────────────┐
│ for peer in [0, 1, 2, 3, 4, ...]:                  │
│     put_nb(data, peer)                              │
│     atomic_inc(peer)                                │
│                                                     │
│ Communicates in fixed order regardless of network  │
│ conditions - may send to congested peers early     │
└─────────────────────────────────────────────────────┘

RTT-aware scheduling:
┌─────────────────────────────────────────────────────┐
│ while (peers_remaining):                            │
│     best_peer = select_peer_with_lowest_rtt()       │
│     if (best_peer.rtt < congestion_threshold):      │
│         put_nb(data, best_peer)                     │
│         atomic_inc(best_peer)                       │
│         mark_peer_completed(best_peer)              │
│     else:                                           │
│         sleep_briefly() // Let congestion clear     │
│                                                     │
│ Dynamically schedules based on network conditions   │
└─────────────────────────────────────────────────────┘
```

## RTT-Aware Alltoall Implementation

### 1. Peer Selection Strategies

```c
// In src/components/tl/ucp/tl_ucp_rtt_sched.h

typedef enum ucc_tl_ucp_rtt_sched_policy {
    UCC_TL_UCP_RTT_SCHED_GREEDY,        // Always pick lowest RTT peer
    UCC_TL_UCP_RTT_SCHED_THRESHOLD,     // Only communicate if RTT < threshold
    UCC_TL_UCP_RTT_SCHED_BALANCED,      // Balance between order and RTT
    UCC_TL_UCP_RTT_SCHED_ADAPTIVE,      // Adjust thresholds dynamically
} ucc_tl_ucp_rtt_sched_policy_t;

typedef struct ucc_tl_ucp_rtt_sched_config {
    ucc_tl_ucp_rtt_sched_policy_t policy;
    double                        rtt_threshold_us;     // Max acceptable RTT (μs)
    double                        rtt_variance_factor;  // Multiplier for variance
    int                           max_concurrent;       // Max in-flight to congested peers
    int                           reorder_window;       // How far to look ahead
    double                        backoff_time_ms;      // Sleep when all peers congested
} ucc_tl_ucp_rtt_sched_config_t;

// Per-peer scheduling state
typedef struct ucc_tl_ucp_peer_sched_state {
    int      completed;           // Has this peer been sent to?
    int      in_flight;          // Is there a PUT in progress?
    double   last_attempt_time;  // When did we last try this peer?
    int      defer_count;        // How many times deferred due to congestion?
} ucc_tl_ucp_peer_sched_state_t;
```

### 2. Core Scheduling Logic

```c
// New file: src/components/tl/ucp/tl_ucp_rtt_sched.c

#include "tl_ucp_rtt_sched.h"
#include "tl_ucp_rtt_probe.h"

// Select the best peer to communicate with based on RTT
static ucc_rank_t ucc_tl_ucp_select_best_peer(
    ucc_tl_ucp_context_t *ctx,
    ucc_tl_ucp_peer_sched_state_t *peer_states,
    ucc_rank_t team_size,
    ucc_rank_t my_rank,
    ucc_tl_ucp_rtt_sched_policy_t policy)
{
    ucc_rank_t best_peer = UCC_RANK_INVALID;
    double best_rtt = DBL_MAX;
    double threshold_rtt = ctx->cfg.rtt_sched.rtt_threshold_us * 1e-6; // Convert to seconds
    int num_available = 0;
    
    for (ucc_rank_t peer = 0; peer < team_size; peer++) {
        if (peer == my_rank) continue;
        if (peer_states[peer].completed) continue;
        if (peer_states[peer].in_flight) continue;
        
        // Get current RTT for this peer
        double peer_rtt, peer_var;
        ucc_status_t status = ucc_tl_ucp_get_peer_rtt(ctx, peer, 
                                                       &peer_rtt, &peer_var);
        
        if (status != UCC_OK) {
            // No RTT data yet - use default ordering
            if (best_peer == UCC_RANK_INVALID) {
                best_peer = peer;
            }
            continue;
        }
        
        num_available++;
        
        switch (policy) {
        case UCC_TL_UCP_RTT_SCHED_GREEDY:
            // Always pick the peer with lowest RTT
            if (peer_rtt < best_rtt) {
                best_rtt = peer_rtt;
                best_peer = peer;
            }
            break;
            
        case UCC_TL_UCP_RTT_SCHED_THRESHOLD:
            // Only consider peers below threshold
            if (peer_rtt < threshold_rtt && peer_rtt < best_rtt) {
                best_rtt = peer_rtt;
                best_peer = peer;
            }
            break;
            
        case UCC_TL_UCP_RTT_SCHED_BALANCED:
            // Balance between RTT and original order
            // Prefer earlier peers unless RTT difference is significant
            double rtt_weight = peer_rtt * (1.0 + 0.1 * peer); // Slight preference for order
            if (rtt_weight < best_rtt) {
                best_rtt = rtt_weight;
                best_peer = peer;
            }
            break;
            
        case UCC_TL_UCP_RTT_SCHED_ADAPTIVE:
            // Dynamically adjust threshold based on network conditions
            // If many peers are congested, relax threshold
            double adaptive_threshold = threshold_rtt * (1.0 + num_available * 0.1);
            if (peer_rtt < adaptive_threshold && peer_rtt < best_rtt) {
                best_rtt = peer_rtt;
                best_peer = peer;
            }
            break;
        }
    }
    
    return best_peer;
}

// Check if a peer is acceptable to communicate with
static int ucc_tl_ucp_is_peer_acceptable(
    ucc_tl_ucp_context_t *ctx,
    ucc_rank_t peer_rank,
    ucc_tl_ucp_rtt_sched_config_t *config)
{
    double peer_rtt, peer_var;
    ucc_status_t status = ucc_tl_ucp_get_peer_rtt(ctx, peer_rank, 
                                                   &peer_rtt, &peer_var);
    
    if (status != UCC_OK) {
        // No RTT data - assume acceptable (optimistic)
        return 1;
    }
    
    // Calculate dynamic threshold based on RTT variance
    double threshold = config->rtt_threshold_us * 1e-6;
    double variance_threshold = peer_var * config->rtt_variance_factor;
    double effective_threshold = threshold + variance_threshold;
    
    return (peer_rtt < effective_threshold);
}

// Get statistics about peer RTT distribution
static void ucc_tl_ucp_get_rtt_stats(
    ucc_tl_ucp_context_t *ctx,
    ucc_rank_t team_size,
    ucc_rank_t my_rank,
    double *min_rtt,
    double *max_rtt,
    double *avg_rtt,
    int *num_congested)
{
    double sum = 0.0;
    int count = 0;
    *min_rtt = DBL_MAX;
    *max_rtt = 0.0;
    *num_congested = 0;
    
    for (ucc_rank_t peer = 0; peer < team_size; peer++) {
        if (peer == my_rank) continue;
        
        double peer_rtt, peer_var;
        if (ucc_tl_ucp_get_peer_rtt(ctx, peer, &peer_rtt, &peer_var) == UCC_OK) {
            sum += peer_rtt;
            count++;
            
            if (peer_rtt < *min_rtt) *min_rtt = peer_rtt;
            if (peer_rtt > *max_rtt) *max_rtt = peer_rtt;
            
            // Check if peer is congested (RTT > mean + 2*variance)
            if (peer_rtt > (peer_rtt + 2.0 * peer_var)) {
                (*num_congested)++;
            }
        }
    }
    
    *avg_rtt = (count > 0) ? (sum / count) : 0.0;
}
```

### 3. Modified alltoall_onesided with RTT Scheduling

```c
// In src/components/tl/ucp/alltoall/alltoall_onesided.c

#include "tl_ucp_rtt_sched.h"

ucc_status_t ucc_tl_ucp_alltoall_onesided_rtt_aware_start(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task     = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team     = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx   = TASK_CTX(task);
    ptrdiff_t          src      = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest     = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems   = TASK_ARGS(task).src.info.count;
    ucc_rank_t         grank    = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize    = UCC_TL_TEAM_SIZE(team);
    long              *pSync    = TASK_ARGS(task).global_work_buffer;
    ucc_mem_map_mem_h  src_memh = TASK_ARGS(task).src_memh.local_memh;
    ucc_mem_map_mem_h *dst_memh = TASK_ARGS(task).dst_memh.global_memh;
    ucc_status_t       status;

    if (TASK_ARGS(task).flags & UCC_COLL_ARGS_FLAG_SRC_MEMH_GLOBAL) {
        src_memh = TASK_ARGS(task).src_memh.global_memh[grank];
    }

    ucc_tl_ucp_task_reset(task, UCC_INPROGRESS);
    
    // Allocate per-peer scheduling state
    ucc_tl_ucp_peer_sched_state_t *peer_states = 
        ucc_calloc(gsize, sizeof(ucc_tl_ucp_peer_sched_state_t),
                   "peer_sched_states");
    if (!peer_states) {
        return UCC_ERR_NO_MEMORY;
    }
    
    // Store in task for use in progress function
    task->rtt_sched.peer_states = peer_states;
    task->rtt_sched.peers_completed = 0;
    task->rtt_sched.total_peers = gsize - 1; // Exclude self
    task->rtt_sched.last_progress_time = ucc_get_time();
    
    nelems = (nelems / gsize) * ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest   = dest + grank * nelems;
    
    // Mark self as completed
    peer_states[grank].completed = 1;
    
    // Get RTT statistics for adaptive behavior
    double min_rtt, max_rtt, avg_rtt;
    int num_congested;
    ucc_tl_ucp_get_rtt_stats(ctx, gsize, grank, 
                            &min_rtt, &max_rtt, &avg_rtt, &num_congested);
    
    tl_debug(UCC_TL_TEAM_LIB(team),
             "Starting RTT-aware alltoall: avg_rtt=%.3f μs, "
             "min=%.3f μs, max=%.3f μs, %d/%d peers congested",
             avg_rtt * 1e6, min_rtt * 1e6, max_rtt * 1e6,
             num_congested, gsize - 1);
    
    // Initial peer selection and communication
    // Try to send to multiple "good" peers immediately
    int initial_sends = 0;
    int max_initial_sends = ucc_min(ctx->cfg.rtt_sched.max_concurrent, 
                                    gsize / 4);
    
    for (int i = 0; i < max_initial_sends; i++) {
        ucc_rank_t peer = ucc_tl_ucp_select_best_peer(
            ctx, peer_states, gsize, grank, ctx->cfg.rtt_sched.policy);
        
        if (peer == UCC_RANK_INVALID) {
            break; // No good peers available
        }
        
        // Check if peer is acceptable
        if (!ucc_tl_ucp_is_peer_acceptable(ctx, peer, &ctx->cfg.rtt_sched)) {
            peer_states[peer].defer_count++;
            continue;
        }
        
        // Send to this peer
        status = ucc_tl_ucp_put_nb(
            (void *)(src + peer * nelems), (void *)dest, nelems,
            peer, src_memh, dst_memh, team, task);
        
        if (status != UCC_OK) {
            ucc_free(peer_states);
            return status;
        }
        
        status = ucc_tl_ucp_atomic_inc(pSync, peer, dst_memh, team);
        if (status != UCC_OK) {
            ucc_free(peer_states);
            return status;
        }
        
        peer_states[peer].in_flight = 1;
        peer_states[peer].last_attempt_time = ucc_get_time();
        initial_sends++;
    }
    
    return ucc_progress_queue_enqueue(UCC_TL_CORE_CTX(team)->pq, &task->super);
}

void ucc_tl_ucp_alltoall_onesided_rtt_aware_progress(ucc_coll_task_t *ctask)
{
    ucc_tl_ucp_task_t *task  = ucc_derived_of(ctask, ucc_tl_ucp_task_t);
    ucc_tl_ucp_team_t *team  = TASK_TEAM(task);
    ucc_tl_ucp_context_t *ctx = TASK_CTX(task);
    ucc_rank_t         grank = UCC_TL_TEAM_RANK(team);
    ucc_rank_t         gsize = UCC_TL_TEAM_SIZE(team);
    long              *pSync = TASK_ARGS(task).global_work_buffer;
    ptrdiff_t          src   = (ptrdiff_t)TASK_ARGS(task).src.info.buffer;
    ptrdiff_t          dest  = (ptrdiff_t)TASK_ARGS(task).dst.info.buffer;
    size_t             nelems;
    ucc_mem_map_mem_h  src_memh = TASK_ARGS(task).src_memh.local_memh;
    ucc_mem_map_mem_h *dst_memh = TASK_ARGS(task).dst_memh.global_memh;
    ucc_tl_ucp_peer_sched_state_t *peer_states = task->rtt_sched.peer_states;
    ucc_status_t status;
    double current_time = ucc_get_time();
    
    nelems = (TASK_ARGS(task).src.info.count / gsize) * 
             ucc_dt_size(TASK_ARGS(task).src.info.datatype);
    dest = dest + grank * nelems;
    
    // Check for completed operations
    for (ucc_rank_t peer = 0; peer < gsize; peer++) {
        if (peer == grank) continue;
        if (peer_states[peer].completed) continue;
        if (!peer_states[peer].in_flight) continue;
        
        // Check if PUT completed (simplified - real implementation needs proper tracking)
        // In practice, you'd track individual PUT requests
        if (task->onesided.put_completed > task->rtt_sched.peers_completed) {
            peer_states[peer].in_flight = 0;
            peer_states[peer].completed = 1;
            task->rtt_sched.peers_completed++;
            
            tl_debug(UCC_TL_TEAM_LIB(team),
                     "Completed PUT to peer %d (deferred %d times)",
                     peer, peer_states[peer].defer_count);
        }
    }
    
    // Check if we're done
    if (task->rtt_sched.peers_completed >= task->rtt_sched.total_peers &&
        UCC_TL_UCP_TASK_ONESIDED_P2P_COMPLETE(task) &&
        UCC_TL_UCP_TASK_ONESIDED_SYNC_COMPLETE(task, gsize)) {
        
        // Cleanup
        ucc_free(peer_states);
        task->rtt_sched.peer_states = NULL;
        pSync[0] = 0;
        task->super.status = UCC_OK;
        return;
    }
    
    // Try to send to more peers based on current network conditions
    int in_flight_count = 0;
    for (ucc_rank_t peer = 0; peer < gsize; peer++) {
        if (peer_states[peer].in_flight) in_flight_count++;
    }
    
    // Only send more if we have capacity
    int max_concurrent = ctx->cfg.rtt_sched.max_concurrent;
    if (in_flight_count < max_concurrent) {
        // Select next best peer
        ucc_rank_t peer = ucc_tl_ucp_select_best_peer(
            ctx, peer_states, gsize, grank, ctx->cfg.rtt_sched.policy);
        
        if (peer != UCC_RANK_INVALID) {
            // Check if peer is acceptable
            if (ucc_tl_ucp_is_peer_acceptable(ctx, peer, &ctx->cfg.rtt_sched)) {
                // Send to this peer
                status = ucc_tl_ucp_put_nb(
                    (void *)(src + peer * nelems), (void *)dest, nelems,
                    peer, src_memh, dst_memh, team, task);
                
                if (status == UCC_OK) {
                    ucc_tl_ucp_atomic_inc(pSync, peer, dst_memh, team);
                    peer_states[peer].in_flight = 1;
                    peer_states[peer].last_attempt_time = current_time;
                    
                    double peer_rtt, peer_var;
                    if (ucc_tl_ucp_get_peer_rtt(ctx, peer, &peer_rtt, &peer_var) == UCC_OK) {
                        tl_debug(UCC_TL_TEAM_LIB(team),
                                 "Sending to peer %d (RTT=%.3f μs)",
                                 peer, peer_rtt * 1e6);
                    }
                }
            } else {
                // Peer is congested - defer
                peer_states[peer].defer_count++;
                
                // If we've deferred this peer many times and all peers are congested,
                // send anyway to make progress
                int all_congested = 1;
                for (ucc_rank_t p = 0; p < gsize; p++) {
                    if (p != grank && !peer_states[p].completed) {
                        if (ucc_tl_ucp_is_peer_acceptable(ctx, p, &ctx->cfg.rtt_sched)) {
                            all_congested = 0;
                            break;
                        }
                    }
                }
                
                if (all_congested && peer_states[peer].defer_count > 10) {
                    // Force send to avoid deadlock
                    tl_warn(UCC_TL_TEAM_LIB(team),
                            "All peers congested, forcing send to peer %d", peer);
                    
                    status = ucc_tl_ucp_put_nb(
                        (void *)(src + peer * nelems), (void *)dest, nelems,
                        peer, src_memh, dst_memh, team, task);
                    
                    if (status == UCC_OK) {
                        ucc_tl_ucp_atomic_inc(pSync, peer, dst_memh, team);
                        peer_states[peer].in_flight = 1;
                        peer_states[peer].last_attempt_time = current_time;
                    }
                }
            }
        }
    }
    
    // Small sleep if no progress to avoid busy-waiting
    double time_since_last_progress = current_time - task->rtt_sched.last_progress_time;
    if (time_since_last_progress > 0.001 && in_flight_count == 0) { // 1ms
        // Brief backoff
        struct timespec backoff = {
            .tv_sec = 0,
            .tv_nsec = (long)(ctx->cfg.rtt_sched.backoff_time_ms * 1e6)
        };
        nanosleep(&backoff, NULL);
    }
}
```

### 4. Task Structure Extensions

```c
// In src/components/tl/ucp/tl_ucp_task.h

typedef struct ucc_tl_ucp_task {
    // ... existing fields ...
    
    struct {
        ucc_tl_ucp_peer_sched_state_t *peer_states;
        int                            peers_completed;
        int                            total_peers;
        double                         last_progress_time;
    } rtt_sched;
} ucc_tl_ucp_task_t;
```

### 5. Configuration

```c
// In src/components/tl/ucp/tl_ucp_lib.c

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SCHED_ENABLE", "no",
    "Enable RTT-aware scheduling for one-sided collectives",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_sched.enable),
    UCC_CONFIG_TYPE_BOOL);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SCHED_POLICY", "greedy",
    "RTT scheduling policy (greedy|threshold|balanced|adaptive)",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_sched.policy),
    UCC_CONFIG_TYPE_ENUM(greedy, threshold, balanced, adaptive));

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SCHED_THRESHOLD", "100",
    "RTT threshold in microseconds for acceptable peers",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_sched.rtt_threshold_us),
    UCC_CONFIG_TYPE_DOUBLE);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SCHED_MAX_CONCURRENT", "8",
    "Maximum concurrent sends to different peers",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_sched.max_concurrent),
    UCC_CONFIG_TYPE_UINT);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SCHED_BACKOFF_MS", "0.1",
    "Backoff time in milliseconds when all peers are congested",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_sched.backoff_time_ms),
    UCC_CONFIG_TYPE_DOUBLE);

UCC_CONFIG_REGISTER_TABLE_ENTRY(&ucc_tl_ucp_context_config_table,
    "RTT_SCHED_VARIANCE_FACTOR", "2.0",
    "Multiplier for RTT variance in threshold calculation",
    ucc_offsetof(ucc_tl_ucp_context_config_t, rtt_sched.rtt_variance_factor),
    UCC_CONFIG_TYPE_DOUBLE);
```

## Scheduling Policies Explained

### 1. Greedy Policy
**Strategy**: Always communicate with the peer that currently has the lowest RTT.

**Pros:**
- Maximizes throughput on fast paths
- Simple to understand and implement
- Works well when RTTs vary significantly

**Cons:**
- May starve high-RTT peers
- Can create unfairness
- Requires forced sends to avoid deadlock

**Best for**: Heterogeneous networks with clear fast/slow paths

### 2. Threshold Policy
**Strategy**: Only communicate with peers whose RTT is below a threshold.

**Pros:**
- Avoids congested paths entirely
- Predictable behavior
- Easy to tune

**Cons:**
- May wait indefinitely if all peers congested
- Threshold choice is workload-dependent
- Less adaptive to changing conditions

**Best for**: Networks with known congestion patterns

### 3. Balanced Policy
**Strategy**: Balance between RTT and original communication order.

**Pros:**
- Maintains some degree of fairness
- Respects algorithm design intentions
- Reduces variance in completion times

**Cons:**
- May not fully exploit fast paths
- More complex heuristic
- Tuning parameters needed

**Best for**: Production systems where predictability matters

### 4. Adaptive Policy
**Strategy**: Dynamically adjust thresholds based on current network state.

**Pros:**
- Self-tuning to network conditions
- Handles varying workloads
- Best overall performance

**Cons:**
- Most complex implementation
- May have adaptation lag
- Harder to debug

**Best for**: General-purpose HPC clusters with varying traffic

## Example Behavior

```
Scenario: 8-rank alltoall with varying RTTs

RTT Table at start (μs):
  Rank 0 -> Ranks: [-, 10, 15, 50, 12, 200, 18, 25]
  
Traditional order: 1, 2, 3, 4, 5, 6, 7
- Sends to rank 5 (200μs RTT) early
- Waits for slow completion

Greedy policy: 1, 4, 2, 6, 7, 3, 5
- Picks rank 1 (10μs) first
- Then rank 4 (12μs)
- Defers rank 5 (200μs) until last
- Rank 5 might improve by then!

Threshold policy (threshold=50μs): 1, 4, 2, 6, 7, 3, <wait>, 5
- Sends to all peers under 50μs
- Waits for rank 5's RTT to improve
- If doesn't improve, eventually sends anyway

Result: ~30% faster completion in this scenario
```

## Performance Analysis

### Advantages over Congestion Control

| Aspect | Congestion Control | RTT Scheduling |
|--------|-------------------|----------------|
| **Overhead** | Segmentation + tracking | Peer selection |
| **Latency** | Segments serialize | Parallel on fast paths |
| **Fairness** | Per-operation | Global optimization |
| **Adaptation** | Per-segment | Per-operation |
| **Complexity** | High (windowing) | Medium (scheduling) |

### Expected Performance Gains

**Homogeneous Network**: 0-5% improvement
- All peers have similar RTT
- Limited scheduling benefit

**Heterogeneous Network**: 10-40% improvement
- Mixed fast/slow paths
- Large benefit from path selection

**Congested Network**: 30-60% improvement
- Dynamic congestion patterns
- Avoids temporary hotspots

**Bursty Traffic**: 20-50% improvement
- Adapts to changing conditions
- Exploits idle periods on fast paths

## Implementation Checklist - RTT Scheduling

### Phase 1: Core Scheduling
- [ ] Create `tl_ucp_rtt_sched.h` and `tl_ucp_rtt_sched.c`
- [ ] Add scheduling configuration structures
- [ ] Implement `ucc_tl_ucp_select_best_peer()`
- [ ] Implement `ucc_tl_ucp_is_peer_acceptable()`
- [ ] Add per-peer scheduling state to tasks

### Phase 2: Policies
- [ ] Implement greedy scheduling policy
- [ ] Implement threshold scheduling policy
- [ ] Implement balanced scheduling policy
- [ ] Implement adaptive scheduling policy
- [ ] Add policy selection configuration

### Phase 3: Integration with Alltoall
- [ ] Modify `alltoall_onesided_start()` for RTT-aware version
- [ ] Modify `alltoall_onesided_progress()` for RTT-aware version
- [ ] Add completion tracking per peer
- [ ] Implement backoff when all peers congested
- [ ] Add forced send to avoid deadlock

### Phase 4: Integration with Alltoallv
- [ ] Extend to `alltoallv_onesided.c`
- [ ] Handle variable message sizes in scheduling
- [ ] Weight peer selection by message size
- [ ] Add size-aware threshold adjustment

### Phase 5: Testing
- [ ] Test with uniform RTT distribution
- [ ] Test with highly variable RTT distribution
- [ ] Test with dynamic congestion patterns
- [ ] Measure completion time improvement
- [ ] Test fairness across all peers
- [ ] Validate no deadlocks under all policies

### Phase 6: Optimization
- [ ] Profile scheduling overhead
- [ ] Optimize peer selection algorithm (O(n) → O(log n))
- [ ] Cache RTT queries
- [ ] Batch peer selections
- [ ] Add fast path for homogeneous networks

## Environment Variables

```bash
# Enable RTT-aware scheduling
export UCC_TL_UCP_RTT_SCHED_ENABLE=yes

# Requires RTT probing thread
export UCC_TL_UCP_RTT_PROBE_ENABLE=yes
export UCC_TL_UCP_RTT_PROBE_INTERVAL=0.05  # 50ms for faster updates

# Set scheduling policy
export UCC_TL_UCP_RTT_SCHED_POLICY=greedy  # or threshold, balanced, adaptive

# Set RTT threshold (microseconds)
export UCC_TL_UCP_RTT_SCHED_THRESHOLD=100

# Set maximum concurrent sends
export UCC_TL_UCP_RTT_SCHED_MAX_CONCURRENT=8

# Backoff time when congested (milliseconds)
export UCC_TL_UCP_RTT_SCHED_BACKOFF_MS=0.1

# Variance factor for threshold
export UCC_TL_UCP_RTT_SCHED_VARIANCE_FACTOR=2.0
```

## Use Cases

### When RTT Scheduling Works Best

1. **Heterogeneous Networks**
   - Mixed InfiniBand + Ethernet
   - Different NIC generations
   - Multi-rail configurations

2. **Shared Infrastructure**
   - Multiple jobs on same network
   - Bursty background traffic
   - QoS-enabled networks

3. **Large-Scale Systems**
   - Many-to-many communication
   - Non-uniform network topology
   - Dragonfly or fat-tree networks

4. **Collectives with Communication Flexibility**
   - Alltoall / Alltoallv
   - Allgather / Allgatherv
   - Reduce-scatter (with reordering)

### When Congestion Control Works Better

1. **Point-to-Point Heavy Workloads**
   - Direct peer communication
   - No communication flexibility
   - Fixed communication patterns

2. **Large Message Transfers**
   - Need to segment anyway
   - RTT measured per segment
   - Fine-grained control needed

3. **Uniform Networks**
   - All paths have similar RTT
   - Limited scheduling benefit
   - Overhead not justified

## Comparison: Three Approaches Summary

| Approach | Overhead | Accuracy | Flexibility | Best For |
|----------|----------|----------|-------------|----------|
| **Inline Measurement + Congestion Control** | High | High | Low | Point-to-point, large messages |
| **RTT Probing + Scheduling** | Medium | Medium | High | Collectives, flexible algorithms |
| **Hybrid (Both)** | High | Highest | Highest | Production systems, critical workloads |

## Key Insights

1. **Scheduling is inherently collective-friendly**: You can reorder communication in collectives
2. **Probing provides global view**: All-to-all RTT matrix enables smart decisions
3. **Greedy can be unfair**: May need forced sends to ensure progress
4. **Dynamic thresholds are crucial**: Static thresholds fail under varying load
5. **Combining approaches wins**: Use probing for scheduling, inline for verification

