#include <gtest/gtest.h>
#include "execution/partitioner.h"
#include "execution/result_partition.h"
#include "execution/blocking_queue.h"
#include "common/data_types.h"
#include "test_utils/test_data_adapter.h"
#include <memory>
#include <vector>

namespace sageFlow {
namespace test {

// Test that BroadcastPartitioner properly identifies as broadcast
TEST(PartitionerTest, BroadcastPartitionerIdentification) {
    auto broadcast = std::make_unique<BroadcastPartitioner>();
    EXPECT_TRUE(broadcast->isBroadcast());
    
    auto roundrobin = std::make_unique<RoundRobinPartitioner>();
    EXPECT_FALSE(roundrobin->isBroadcast());
    
    auto key = std::make_unique<KeyPartitioner>();
    EXPECT_FALSE(key->isBroadcast());
}

// Test that KeyPartitioner produces consistent hashing
TEST(PartitionerTest, KeyPartitionerConsistency) {
    KeyPartitioner partitioner;
    
    // Create test record
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    auto record = createVectorRecord(12345, 1000, data);
    Response response{ResponseType::Record, std::move(record)};
    
    // Same UID should always map to same partition
    size_t first_partition = partitioner.partition(response, 4);
    
    // Recreate with same UID
    auto record2 = createVectorRecord(12345, 2000, data);
    Response response2{ResponseType::Record, std::move(record2)};
    
    size_t second_partition = partitioner.partition(response2, 4);
    
    EXPECT_EQ(first_partition, second_partition);
}

// Test that RoundRobinPartitioner distributes evenly
TEST(PartitionerTest, RoundRobinDistribution) {
    RoundRobinPartitioner partitioner;
    
    std::vector<size_t> counts(4, 0);
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    
    for (int i = 0; i < 100; ++i) {
        auto record = createVectorRecord(i, 1000 + i, data);
        Response response{ResponseType::Record, std::move(record)};
        
        size_t partition = partitioner.partition(response, 4);
        EXPECT_LT(partition, 4);
        counts[partition]++;
    }
    
    // Each partition should get exactly 25 records
    for (size_t count : counts) {
        EXPECT_EQ(count, 25);
    }
}

// Test broadcast functionality in ResultPartition
TEST(ResultPartitionTest, BroadcastToAllChannels) {
    ResultPartition partition;
    
    // Create multiple queues (store raw pointers for later access)
    std::vector<std::shared_ptr<BlockingQueue>> raw_queues;
    std::vector<QueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        auto queue = std::make_shared<BlockingQueue>(10);
        raw_queues.push_back(queue);
        queues.push_back(queue);
    }
    
    // Setup with broadcast partitioner
    auto broadcast = std::make_unique<BroadcastPartitioner>();
    partition.setup(std::move(broadcast), std::move(queues), 0);
    
    // Emit a record
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    auto record = createVectorRecord(999, 1000, data);
    Response response{ResponseType::Record, std::move(record)};
    partition.emit(std::move(response), 0);
    
    // Verify all queues received the data
    int received_count = 0;
    for (size_t i = 0; i < 3; ++i) {
        auto tagged = raw_queues[i]->pop();
        if (tagged.has_value()) {
            received_count++;
            EXPECT_TRUE(tagged->response.record_ != nullptr);
            EXPECT_EQ(tagged->response.record_->uid_, 999);
            EXPECT_EQ(tagged->slot, 0);
        }
    }
    
    // All 3 queues should have received the broadcast
    EXPECT_EQ(received_count, 3);
}

// Test standard partitioning in ResultPartition
TEST(ResultPartitionTest, StandardPartitioning) {
    ResultPartition partition;
    
    // Create multiple queues (store raw pointers for later access)
    std::vector<std::shared_ptr<BlockingQueue>> raw_queues;
    std::vector<QueuePtr> queues;
    for (int i = 0; i < 3; ++i) {
        auto queue = std::make_shared<BlockingQueue>(10);
        raw_queues.push_back(queue);
        queues.push_back(queue);
    }
    
    // Setup with round-robin partitioner
    auto roundrobin = std::make_unique<RoundRobinPartitioner>();
    partition.setup(std::move(roundrobin), std::move(queues), 0);
    
    // Emit multiple records
    std::vector<float> data = {1.0f, 2.0f, 3.0f};
    
    for (int i = 0; i < 9; ++i) {
        auto record = createVectorRecord(i, 1000 + i, data);
        Response response{ResponseType::Record, std::move(record)};
        partition.emit(std::move(response), 0);
    }
    
    // Stop queues to prevent blocking pop
    for (auto& q : raw_queues) {
        q->stop();
    }
    
    // Count records in each queue
    std::vector<int> queue_counts(3, 0);
    for (size_t i = 0; i < 3; ++i) {
        while (true) {
            auto tagged = raw_queues[i]->pop();
            if (!tagged.has_value()) break;
            queue_counts[i]++;
        }
    }
    
    // Each queue should have exactly 3 records (9 records / 3 queues)
    for (int count : queue_counts) {
        EXPECT_EQ(count, 3);
    }
}

} // namespace test
} // namespace sageFlow
