//
// Created by ZeroJustMe on 25-7-22.
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include "common/data_types.h"

namespace sageFlow {
class IPartitioner {
public:
  virtual ~IPartitioner() = default;
  virtual size_t partition(const Response& data, size_t num_channels) = 0;
  // 是否为广播模式（默认false）
  virtual bool isBroadcast() const { return false; }
};

// 轮询/随机分发
class RoundRobinPartitioner : public IPartitioner {
private:
  std::atomic<size_t> counter_ = 0;
public:
  size_t partition(const Response&, size_t num_channels) override {
    return counter_++ % num_channels;
  }
};

// 按Key分区分发 - 确保相同key的记录路由到同一个实例
// 这对于Join算子至关重要，避免竞态条件
class KeyPartitioner : public IPartitioner {
public:
  size_t partition(const Response& data, size_t num_channels) override {
    if (!data.record_) {
      return 0;  // 默认分区
    }
    // 使用record的uid作为分区key，确保相同uid的记录到达同一实例
    return std::hash<uint64_t>{}(data.record_->uid_) % num_channels;
  }
};

// 基于向量内容的哈希分区 - 用于确保相似向量分配到同一实例
class VectorHashPartitioner : public IPartitioner {
public:
  size_t partition(const Response& data, size_t num_channels) override {
    if (!data.record_ || data.record_->data_.dim_ == 0) {
      return 0;
    }
    // 使用向量的前几个维度计算哈希，平衡计算开销和分区质量
    size_t hash = 0;
    const int dims_to_hash = std::min(8, data.record_->data_.dim_);
    for (int i = 0; i < dims_to_hash; ++i) {
      // 组合哈希值
      hash ^= std::hash<float>{}(data.record_->data_.data_[i]) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    return hash % num_channels;
  }
};

// 广播分区器 - 将每条记录发送到所有下游实例
// 用于JOIN算子确保每个实例都能看到所有记录，从而实现完整的join结果
// 注意：这会增加网络/队列开销，仅适用于需要全局视图的算子（如window-based join）
class BroadcastPartitioner : public IPartitioner {
public:
  size_t partition(const Response&, size_t) override {
    // 广播模式下，partition方法返回值无意义
    // 实际的广播逻辑需要在ResultPartition中特殊处理
    return 0;
  }
  
  // 标记此分区器需要广播
  virtual bool isBroadcast() const { return true; }
};

};