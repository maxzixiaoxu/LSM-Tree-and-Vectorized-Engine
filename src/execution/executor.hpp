#pragma once

#include <numeric>

#include "catalog/db.hpp"
#include "execution/execoptions.hpp"
#include "execution/volcano/expr_executor.hpp"
#include "parser/expr.hpp"
#include "plan/plan.hpp"
#include "storage/storage.hpp"
#include "transaction/txn.hpp"
#include "type/tuple_batch.hpp"

namespace wing {

/**
 * Init(): Only allocate memory and set some flags, don't evaluate expressions
 * or read/write tuples. Next(): Do operations for each tuple. Return invalid
 * result if it has completed.
 *
 * The first Next() returns the first tuple. The i-th Next() returns the i-th
 * tuple. It is illegal to invoke Next() after Next() returns invalid result.
 * Ensure that Init is invoked only once before executing.
 *
 * You should ensure that the SingleTuple is valid until Next() is invoked
 * again.
 *
 * GetTotalOutputSize: return the total size of output of executors.
 */
class Executor {
 public:
  virtual ~Executor() = default;
  virtual void Init() = 0;
  virtual SingleTuple Next() = 0;
  virtual size_t GetTotalOutputSize() const { return 0; }
};

class VecExecutor {
 public:
  VecExecutor(const ExecOptions& options);
  virtual ~VecExecutor() = default;
  virtual void Init() = 0;
  TupleBatch Next();
  virtual size_t GetTotalOutputSize() const { return stat_output_size_; }

 protected:
  virtual TupleBatch InternalNext() = 0;
  size_t max_batch_size_;
  size_t stat_output_size_{0};
};

#ifdef BUILD_JIT
class JitExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(
      const PlanNode* plan, DB& db, size_t txn_id);

 private:
};
#else
class JitExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(const PlanNode*, DB&, size_t) {
    return nullptr;
  }

 private:
};
#endif

class ExecutorGenerator {
 public:
  static std::unique_ptr<Executor> Generate(
      const PlanNode* plan, DB& db, txn_id_t txn_id);
  static std::unique_ptr<VecExecutor> GenerateVec(
      const PlanNode* plan, DB& db, txn_id_t txn_id);

 private:
};

}  // namespace wing