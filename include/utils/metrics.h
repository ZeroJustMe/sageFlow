#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>

namespace sageFlow {

// ============================================================================
// General Metrics Infrastructure - Reusable across all operators
// ============================================================================

/**
 * @brief RAII timer that accumulates elapsed time to an atomic counter
 * 
 * Usage:
 *   std::atomic<uint64_t> my_metric{0};
 *   {
 *     ScopedTimerAtomic timer(my_metric);
 *     // ... code to measure ...
 *   } // Elapsed time is added to my_metric on destruction
 */
class ScopedTimerAtomic {
 public:
  using Clock = std::chrono::high_resolution_clock;
  
  explicit ScopedTimerAtomic(std::atomic<uint64_t>& slot) 
    : slot_(slot), start_(Clock::now()) {}
  
  ~ScopedTimerAtomic() {
    auto d = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start_).count();
    slot_.fetch_add(static_cast<uint64_t>(d), std::memory_order_relaxed);
  }
  
 private:
  std::atomic<uint64_t>& slot_;
  Clock::time_point start_;
};

/**
 * @brief RAII accumulator that adds elapsed time since a given start timestamp
 * 
 * Usage:
 *   uint64_t start_ns = ScopedAccumulateAtomic::now_ns();
 *   // ... some code ...
 *   {
 *     ScopedAccumulateAtomic acc(my_metric, start_ns);
 *     // ... more code ...
 *   } // Time from start_ns to now is added to my_metric
 */
class ScopedAccumulateAtomic {
 public:
  ScopedAccumulateAtomic(std::atomic<uint64_t>& slot, uint64_t start_ns) 
    : slot_(slot), start_ns_(start_ns) {}
  
  ~ScopedAccumulateAtomic() {
    uint64_t end_ns = now_ns();
    slot_.fetch_add(end_ns - start_ns_, std::memory_order_relaxed);
  }
  
  /**
   * @brief Get current timestamp in nanoseconds
   * @return Current time since epoch in nanoseconds
   */
  static uint64_t now_ns() {
    return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
      ).count()
    );
  }
  
 private:
  std::atomic<uint64_t>& slot_;
  uint64_t start_ns_;
};

// ============================================================================
// Metrics Helper Functions - Conditionally compiled based on SAGEFLOW_ENABLE_METRICS
// ============================================================================

/**
 * @brief Get current timestamp for metrics tracking
 * @return Timestamp in nanoseconds when metrics enabled, 0 otherwise
 */
inline uint64_t metrics_timestamp() {
#ifdef SAGEFLOW_ENABLE_METRICS
  return ScopedAccumulateAtomic::now_ns();
#else
  return 0;
#endif
}

/**
 * @brief Record elapsed time to a single metric
 * @param metric Target metric to update
 * @param start_time Start timestamp from metrics_timestamp()
 */
inline void metrics_record_elapsed(std::atomic<uint64_t>& metric, uint64_t start_time) {
#ifdef SAGEFLOW_ENABLE_METRICS
  if (start_time > 0) {
    uint64_t elapsed = ScopedAccumulateAtomic::now_ns() - start_time;
    metric.fetch_add(elapsed, std::memory_order_relaxed);
  }
#else
  (void)metric;
  (void)start_time;
#endif
}

/**
 * @brief Record elapsed time to two metrics simultaneously
 * @param metric1 First metric to update
 * @param metric2 Second metric to update
 * @param start_time Start timestamp from metrics_timestamp()
 */
inline void metrics_record_elapsed_dual(std::atomic<uint64_t>& metric1, 
                                        std::atomic<uint64_t>& metric2,
                                        uint64_t start_time) {
#ifdef SAGEFLOW_ENABLE_METRICS
  if (start_time > 0) {
    uint64_t elapsed = ScopedAccumulateAtomic::now_ns() - start_time;
    metric1.fetch_add(elapsed, std::memory_order_relaxed);
    metric2.fetch_add(elapsed, std::memory_order_relaxed);
  }
#else
  (void)metric1;
  (void)metric2;
  (void)start_time;
#endif
}

/**
 * @brief Increment a counter metric
 * @param counter Counter to increment
 * @param value Value to add (default 1)
 */
inline void metrics_increment(std::atomic<uint64_t>& counter, uint64_t value = 1) {
#ifdef SAGEFLOW_ENABLE_METRICS
  counter.fetch_add(value, std::memory_order_relaxed);
#else
  (void)counter;
  (void)value;
#endif
}

/**
 * @brief RAII wrapper for scoped timing - no-op when metrics disabled
 * 
 * Usage:
 *   std::atomic<uint64_t> my_metric{0};
 *   {
 *     MetricsTimer timer(my_metric);
 *     // ... code to measure ...
 *   } // Time is recorded when metrics enabled, no overhead when disabled
 */
class MetricsTimer {
 public:
#ifdef SAGEFLOW_ENABLE_METRICS
  explicit MetricsTimer(std::atomic<uint64_t>& slot) : timer_(slot) {}
 private:
  ScopedTimerAtomic timer_;
#else
  explicit MetricsTimer(std::atomic<uint64_t>&) {}
#endif
};

} // namespace sageFlow
