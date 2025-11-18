# Metrics System Documentation

## Overview

The sageFlow metrics system provides a reusable infrastructure for collecting performance metrics across all operators. The system is designed to be:

- **Zero-overhead when disabled**: All metrics code compiles to no-ops when `SAGEFLOW_ENABLE_METRICS` is not defined
- **Thread-safe**: Uses atomic operations for concurrent access
- **Extensible**: Easy to add new operator-specific metrics

## Architecture

### Core Infrastructure (`include/utils/metrics.h`)

The general-purpose metrics utilities that can be used by any operator:

#### Classes

**`ScopedTimerAtomic`**
- RAII timer that measures elapsed time and adds it to an atomic counter
- Usage:
  ```cpp
  std::atomic<uint64_t> my_timing_metric{0};
  {
    ScopedTimerAtomic timer(my_timing_metric);
    // ... code to measure ...
  } // Time automatically recorded
  ```

**`ScopedAccumulateAtomic`**
- RAII accumulator for measuring time from a pre-captured timestamp
- Useful when you need to measure time including lock wait
- Usage:
  ```cpp
  uint64_t start = ScopedAccumulateAtomic::now_ns();
  // ... some work ...
  {
    ScopedAccumulateAtomic acc(my_metric, start);
    // ... more work ...
  } // Total time from start is recorded
  ```

**`MetricsTimer`**
- Conditionally compiled RAII timer
- When metrics disabled, compiles to empty class with no overhead
- Usage:
  ```cpp
  {
    MetricsTimer timer(my_metric);
    // ... code to measure ...
  }
  ```

#### Helper Functions

- **`metrics_timestamp()`**: Get current timestamp (0 when metrics disabled)
- **`metrics_record_elapsed(metric, start_time)`**: Record elapsed time to one metric
- **`metrics_record_elapsed_dual(metric1, metric2, start_time)`**: Record to two metrics
- **`metrics_increment(counter, value)`**: Increment a counter metric

### Join Operator Metrics (`include/operator/join_metrics.h`)

Join-specific metrics implementation built on top of the core infrastructure:

#### JoinMetrics Structure

Singleton container for all join operator metrics:

**Timing Metrics** (in nanoseconds):
- `window_insert_ns`: Time for window insert/expire operations
- `index_insert_ns`: Time for index operations
- `candidate_fetch_ns`: Time fetching join candidates
- `similarity_ns`: Time computing similarity
- `join_function_ns`: Time executing join function
- `emit_ns`: Time emitting results
- `lock_wait_ns`: Time waiting for locks
- `apply_processing_ns`: Total time in apply() method

**Counter Metrics**:
- `total_records_left/right`: Records processed per side
- `total_emits`: Results emitted
- `window_records_left/right_completed`: Records expired
- `apply_processing_count`: Number of apply() calls
- `e2e_latency_ns/count`: End-to-end latency tracking

#### Join-Specific Helpers

- **`metrics_record_lock_wait(start_time)`**: Record to lock_wait_ns
- **`metrics_record_lock_wait_dual(start_time, additional_metric)`**: Record to lock_wait_ns and another metric
- **`metrics_record_e2e_latency(start_time)`**: Record end-to-end latency

## Adding Metrics to New Operators

### Step 1: Create Operator-Specific Metrics Header

Create `include/operator/your_operator_metrics.h`:

```cpp
#pragma once
#include <atomic>
#include <cstdint>
#include "utils/metrics.h"

namespace sageFlow {

struct YourOperatorMetrics {
  // Define your metrics
  std::atomic<uint64_t> processing_ns{0};
  std::atomic<uint64_t> records_processed{0};
  
  static YourOperatorMetrics& instance() {
    static YourOperatorMetrics inst;
    return inst;
  }
  
  void reset() {
    processing_ns = 0;
    records_processed = 0;
  }
};

// Optional: Add operator-specific helper functions
inline void your_operator_specific_helper(uint64_t start_time) {
#ifdef SAGEFLOW_ENABLE_METRICS
  metrics_record_elapsed(YourOperatorMetrics::instance().processing_ns, start_time);
#else
  (void)start_time;
#endif
}

} // namespace sageFlow
```

### Step 2: Use Metrics in Your Operator

```cpp
#include "operator/your_operator_metrics.h"

void YourOperator::process() {
  // Use MetricsTimer for scoped timing
  MetricsTimer timer(YourOperatorMetrics::instance().processing_ns);
  
  // Use metrics_increment for counters (always call, not conditional)
  YourOperatorMetrics::instance().records_processed.fetch_add(1, std::memory_order_relaxed);
  
  // Or use helper if you created one
  uint64_t start = metrics_timestamp();
  // ... work ...
  your_operator_specific_helper(start);
}
```

## Important Notes

### Conditional vs Unconditional Metrics

- **Conditional** (wrapped in `#ifdef`): Use `MetricsTimer`, `metrics_increment()`, helper functions
  - These compile to no-ops when metrics disabled
  - Use for timing and optional counters

- **Unconditional** (always executed): Direct atomic operations
  - Use when tests or other code depends on the counter
  - Example: `metric.fetch_add(1, std::memory_order_relaxed)`

### Thread Safety

All metrics use `std::atomic` with `memory_order_relaxed` for:
- Lock-free operation
- Minimal performance impact
- Correct concurrent access

Note: Relaxed ordering is sufficient because metrics don't synchronize program logic.

## Examples

See `src/operator/join_operator.cpp` for comprehensive examples of metrics usage in a production operator.
