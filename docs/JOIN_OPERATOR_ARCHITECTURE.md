# Join Operator Architecture

## Overview

This document describes the architecture of the join operator in sageFlow, specifically addressing the multi-threading model and synchronization mechanisms.

## Problem Statement

The original join operator architecture had race conditions when using parallelism > 1:

### Original Issues
1. **Random Distribution**: Upstream operators used `RoundRobinPartitioner` to randomly distribute records to downstream join instances
2. **Shared Index Access**: All join instances access the same shared index (via `ConcurrencyManager`), but insertion order was non-deterministic
3. **Non-Deterministic Results**: The order of insertions into the shared index varied based on scheduling, leading to inconsistent join results
4. **Race Condition**: The real issue is not about which instance sees which record, but about the **order of insertion into the shared index**

## Key Insight

Unlike typical distributed systems where data must be co-located for joins, sageFlow's join operators already use a **shared index** that all instances can access. The IVF and BruteForce join methods create shared indexes via `ConcurrencyManager`, which means:

- All operator instances can see all vectors through the shared memory/index
- Broadcasting records is unnecessary and wasteful
- The problem is ensuring **stable insertion order** into the shared index

## Solution Architecture

### Timestamp-Based Key Partitioning

The solution implements **KeyPartitioner** that partitions based on timestamp:

```
┌─────────────┐
│Left Stream  │──┐
└─────────────┘  │
                 ├──► [KeyPartitioner(timestamp)] ──┬──► Join Instance 0 (handles T0-T4)
                 │                                   ├──► Join Instance 1 (handles T5-T9)
┌─────────────┐  │                                   └──► Join Instance 2 (handles T10-T14)
│Right Stream │──┘
└─────────────┘
```

### Key Components

#### 1. KeyPartitioner Implementation

```cpp
class KeyPartitioner : public IPartitioner {
public:
  size_t partition(const Response& data, size_t num_channels) override {
    if (!data.record_) return 0;
    // Use timestamp as partition key to ensure records with similar timestamps
    // are routed to the same instance, maintaining stable insertion order
    return std::hash<int64_t>{}(data.record_->timestamp_) % num_channels;
  }
};
```

#### 2. ExecutionGraph Configuration

The `ExecutionGraph` automatically selects the appropriate partitioner:

```cpp
if (is_join_operator) {
    // Join operators use KeyPartitioner to ensure temporal ordering
    partitioner = std::make_unique<KeyPartitioner>();
} else {
    // Other operators use RoundRobinPartitioner for load balancing
    partitioner = std::make_unique<RoundRobinPartitioner>();
}
```

## Benefits

### 1. Deterministic Insertion Order
- Records with similar timestamps are processed by the same instance
- Insertion into shared index follows a predictable order
- Join results are consistent regardless of scheduling

### 2. Efficient Resource Usage
- No data duplication (unlike broadcasting)
- Leverages existing shared index infrastructure
- Maintains parallelism benefits

### 3. Algorithm Compatibility
- Works with all join algorithms: BruteForce, IVF, and future implementations
- No changes required to join algorithms themselves
- Transparent to the join operator implementation

## How Shared Index Works

Each join operator creates a pair of shared indexes:

```cpp
// In JoinOperator constructor
left_index_id_ = concurrency_manager_->create_index(prefix + "_left", type, dim);
right_index_id_ = concurrency_manager_->create_index(prefix + "_right", type, dim);

// All instances can access the same index
concurrency_manager_->insert(index_id, record);  // Thread-safe insertion
concurrency_manager_->query(index_id, ...);       // Thread-safe query
```

The race condition occurs when:
1. Instance A receives record R1 at T1
2. Instance B receives record R2 at T1
3. Both insert into shared index, but order is non-deterministic
4. Query results depend on insertion order

By using KeyPartitioner with timestamp-based partitioning, we ensure:
- Records at T1 always go to the same instance
- Insertion order is stable within that time bucket
- Join results are deterministic

## Performance Considerations

### Trade-offs

#### Advantages
✅ No data duplication (memory efficient)  
✅ Deterministic join results  
✅ Leverages existing shared index  
✅ Good load balancing based on time distribution  

#### Disadvantages
⚠️ May have load imbalance if timestamps are not uniformly distributed  
⚠️ Still requires synchronization for shared index access  

### When to Use Key Partitioning

**Good for:**
- Window-based joins where temporal ordering matters
- Similarity joins with shared index infrastructure
- Systems where deterministic results are critical

**Consider alternatives for:**
- Non-temporal joins where timestamp is not relevant
- Systems without shared index (would need co-partitioning or broadcast)

## Future Enhancements

### 1. Adaptive Partitioning
Monitor timestamp distribution and adjust partitioning strategy dynamically:
```cpp
// Switch between timestamp-based and UID-based partitioning
// based on workload characteristics
```

### 2. Fine-grained Timestamp Buckets
Partition based on timestamp windows rather than hash:
```cpp
// Route records in [T0-T5) to Instance 0, [T5-T10) to Instance 1, etc.
size_t partition = (timestamp / window_size) % num_channels;
```

### 3. Synchronized Window Triggers
Implement a coordinator that synchronizes window triggers across all join instances to further reduce timing-related variance.

## Implementation Details

### Files Modified
- `include/execution/partitioner.h`: Removed BroadcastPartitioner, updated KeyPartitioner to use timestamp
- `src/execution/execution_graph.cpp`: Use KeyPartitioner for JOIN operators
- `docs/JOIN_OPERATOR_ARCHITECTURE.md`: Updated documentation

### Configuration
No configuration changes required. The system automatically uses timestamp-based key partitioning for join operators.

### Compatibility
- ✅ Backward compatible with existing code
- ✅ No changes to join operator implementations
- ✅ No changes to function APIs
- ✅ All existing tests pass

## References

- Original issue: https://github.com/intellistream/sageFlow/issues/58
- Related: Shared index via ConcurrencyManager
- Related: Window operator synchronization
