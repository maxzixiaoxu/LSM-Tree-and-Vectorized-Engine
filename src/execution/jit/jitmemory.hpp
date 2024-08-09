#pragma once

#include <memory>
#include <vector>

#include "storage/storage.hpp"
#include "type/tuple.hpp"

namespace wing {

// Global memory managing global variables,
// and unique_ptrs such as Iterator, TupleStore.
class JitMemory {
 public:
  JitMemory() = default;

  /* Allocate memory and return the offset. */
  size_t Allocate(size_t t) {
    auto ret = memory_size_;
    memory_size_ += t;
    return ret;
  }

  /* Init the value at t with data. */
  void Init(size_t t, uint8_t data) { init_values_.push_back({t, data}); }

  /* Add an iterator used in JITExecutor.*/
  void AddIterator(std::unique_ptr<Iterator<const uint8_t*>>&& iter) {
    iters_.push_back(std::move(iter));
  }

  /* ADd a tuple store used in JITExecutor. */
  void AddTupleStore(std::unique_ptr<TupleStore>&& store) {
    tuple_store_ = std::move(store);
  }

  /* Allocate an memory region. */
  std::unique_ptr<uint8_t[]> GetMemory() {
    auto ret = std::unique_ptr<uint8_t[]>(new uint8_t[memory_size_]);
    for (auto& [x, y] : init_values_)
      ret.get()[x] = y;
    return ret;
  }

  auto& GetIterators() { return iters_; }

  auto& GetTupleStore() { return tuple_store_; }

 private:
  size_t memory_size_{0};
  std::vector<std::pair<size_t, uint8_t>> init_values_;
  std::vector<std::unique_ptr<Iterator<const uint8_t*>>> iters_;

  std::unique_ptr<TupleStore> tuple_store_;
};

}  // namespace wing
