// #pragma once

// #include <functional>
// #include <iostream>

// #include "common/murmurhash.hpp"
// #include "execution/executor.hpp"
// #include "execution/vec/expr_vexecutor.hpp"
// #include "type/tuple_batch.hpp"

// namespace wing {

// class HashJoinVecExecutor : public VecExecutor {
//  public:
//   HashJoinVecExecutor(const ExecOptions& options, const OutputSchema& schema,
//       const OutputSchema& left_schema, const OutputSchema& right_schema,
//       const std::unique_ptr<Expr>& expr,
//       const std::vector<std::unique_ptr<Expr>>& left_hash_exprs,
//       const std::vector<std::unique_ptr<Expr>>& right_hash_exprs,
//       std::unique_ptr<VecExecutor> ch, std::unique_ptr<VecExecutor> ch2)
//     : VecExecutor(options),
//       output_schema_(schema),
//       pred_(ExprVecExecutor::Create(expr.get(), schema)),
//       ch_(std::move(ch)),
//       ch2_(std::move(ch2)) {
//     for (size_t i = 0; i < left_hash_exprs.size(); ++i) {
//       left_hash_exprs_.push_back(
//           ExprVecExecutor::Create(left_hash_exprs[i].get(), left_schema));
//     }
//     for (size_t i = 0; i < right_hash_exprs.size(); i++) {
//       right_hash_exprs_.push_back(
//           ExprVecExecutor::Create(right_hash_exprs[i].get(), right_schema));
//     }
//   }

//   void Init() override {
//     ch_->Init();
//     ch2_->Init();
//     build_batches_.clear();
//     left_hash_exprs_res_.resize(left_hash_exprs_.size());
//     right_hash_exprs_res_.resize(right_hash_exprs_.size());
//     probe_batch_ = TupleBatch();
//     int batch_num = 0;
//     TupleBatch batch = ch_->Next();
//     while (batch.size() != 0) {
//       TupleBatch build_batch;
//       build_batch.Init(batch);
//       build_batch.SetSelVector(batch.GetSelVector());
//       build_batches_.push_back(build_batch);
//       BuildHashTable(batch, batch_num);
//       batch_num++;
//       batch = ch_->Next();
//     }
//   }

//   TupleBatch InternalNext() override {
//     TupleBatch output_batch;
//     if (!buffer_.empty()) {
//       output_batch = buffer_.back();
//       buffer_.pop_back();
//       return output_batch;
//     }
//     output_batch.Init(output_schema_.GetTypes(), max_batch_size_);
//     probe_batch_ = ch2_->Next();
//     if (probe_batch_.size() == 0) {
//       return output_batch;
//     }
//     for (size_t i = 0; i < right_hash_exprs_.size(); ++i) {
//       right_hash_exprs_[i].Evaluate(probe_batch_.GetCols(), probe_batch_.size(),
//           right_hash_exprs_res_[i]);
//     }
//     for (size_t tuple_id = 0; tuple_id < probe_batch_.size(); ++tuple_id) {
//       if (!probe_batch_.IsValid(tuple_id)) {
//         continue;
//       }
//       size_t hashkey = 0;
//       hashkey = ComputeHashKey(right_hash_exprs_res_, tuple_id);
//       if (hash_table_.find(hashkey) != hash_table_.end()) {
//         for (auto build_idx : hash_table_[hashkey]) {
//           auto [batch_id, build_tuple_id] = build_idx;
//           TupleBatch::SingleTuple build_tuple =
//               build_batches_[batch_id].GetSingleTuple(build_tuple_id);
//           TupleBatch::SingleTuple probe_tuple =
//               probe_batch_.GetSingleTuple(tuple_id);
//           std::vector<StaticFieldRef> joined_tuple;
//           for (size_t i = 0; i < build_tuple.size(); i++) {
//             joined_tuple.push_back(build_tuple[i]);
//           }
//           for (size_t i = 0; i < probe_tuple.size(); i++) {
//             joined_tuple.push_back(probe_tuple[i]);
//           }
//           if (output_batch.size() == max_batch_size_) {
//             if (pred_) {
//               EvaluatePredicate(output_batch, output_batch.size());
//             }
//             buffer_.push_back(output_batch);
//             output_batch.Init(output_schema_.GetTypes(), max_batch_size_);
//           }
//           output_batch.Append(joined_tuple);
//         }
//       }
//     }
//     if (pred_) {
//       EvaluatePredicate(output_batch, output_batch.size());
//     }
//     return output_batch;
//   }

//   virtual size_t GetTotalOutputSize() const override {
//     return ch_->GetTotalOutputSize() + ch2_->GetTotalOutputSize() +
//            stat_output_size_;
//   }

//  private:
//   void BuildHashTable(TupleBatch& batch, size_t batch_id) {
//     for (size_t i = 0; i < left_hash_exprs_.size(); ++i) {
//       left_hash_exprs_[i].Evaluate(
//           batch.GetCols(), batch.size(), left_hash_exprs_res_[i]);
//     }
//     for (size_t j = 0; j < batch.size(); ++j) {
//       if (batch.IsValid(j)) {
//         size_t hashkey = 0;
//         hashkey = ComputeHashKey(left_hash_exprs_res_, j);
//         hash_table_[hashkey].push_back(std::make_tuple(batch_id, j));
//       }
//     }
//   }

//   size_t ComputeHashKey(
//       const std::vector<Vector>& hash_exprs_res, size_t tuple_index) {
//     size_t seed = 0x5678;
//     size_t hashkey = 0;

//     for (const auto& vec : hash_exprs_res) {
//       if (vec.GetElemType() == LogicalType::STRING) {
//         hashkey = utils::Hash(vec.Get(tuple_index).ReadStringView(), seed);
//       } else {
//         hashkey = utils::Hash8(vec.Get(tuple_index).ReadInt(), seed);
//       }
//       seed = hashkey;
//     }

//     return hashkey;
//   }

//   void EvaluatePredicate(TupleBatch& batch, size_t batch_size) {
//     pred_.Evaluate(batch.GetCols(), batch_size, predicate_res_);
//     for (size_t i = 0; i < batch_size; ++i) {
//       if (predicate_res_.Get(i).ReadInt() == 0) {
//         batch.SetValid(i, false);
//       }
//     }
//   }

//   OutputSchema output_schema_;
//   ExprVecExecutor pred_;
//   Vector predicate_res_;
//   std::vector<ExprVecExecutor> left_hash_exprs_;
//   std::vector<ExprVecExecutor> right_hash_exprs_;
//   std::vector<Vector> left_hash_exprs_res_;
//   std::vector<Vector> right_hash_exprs_res_;
//   std::unique_ptr<VecExecutor> ch_;
//   std::unique_ptr<VecExecutor> ch2_;
//   std::vector<TupleBatch> build_batches_;
//   TupleBatch probe_batch_;
//   std::unordered_map<size_t, std::vector<std::tuple<int, int>>> hash_table_;
//   std::vector<TupleBatch> buffer_;
// };

// }  // namespace wing


#pragma once

#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"
#include "type/tuple_batch.hpp"
#include "common/murmurhash.hpp"

#include <iostream>
using namespace std;

namespace wing {

class HashJoinVecExecutor : public VecExecutor {
 public:
  HashJoinVecExecutor(const ExecOptions& options, const std::unique_ptr<Expr>& expr,
      const OutputSchema& output_schema, const OutputSchema& left_schema, 
      const OutputSchema& right_schema, const std::vector<std::unique_ptr<Expr>>& left_hash_exprs,
      const std::vector<std::unique_ptr<Expr>>& right_hash_exprs, std::unique_ptr<VecExecutor> left,
      std::unique_ptr<VecExecutor> right)
    : VecExecutor(options),
      left_(std::move(left)),
      right_(std::move(right)),
      output_schema_(output_schema){
        predicate_ = ExprVecExecutor::Create(expr.get(), output_schema);
        for (auto& expr : left_hash_exprs) {
          left_hash_exprs_.push_back(ExprVecExecutor::Create(expr.get(), left_schema));
        }
        for (auto& expr : right_hash_exprs) {
          right_hash_exprs_.push_back(ExprVecExecutor::Create(expr.get(), right_schema));
        }
    }

  void Init() override {
    left_->Init();
    right_->Init();
    build_batches_.clear();
    left_hash_exprs_res_.resize(left_hash_exprs_.size());
    right_hash_exprs_res_.resize(right_hash_exprs_.size());
    probe_batch_ = TupleBatch();
    int batch_num = 0;
    TupleBatch batch = left_->Next();
    while (batch.size() != 0) {
      TupleBatch build_batch;
      build_batch.Init(batch);
      build_batch.SetSelVector(batch.GetSelVector());
      build_batches_.push_back(build_batch);
      BuildHashTable(batch, batch_num);
      batch_num++;
      batch = left_->Next();
    }
  }

   TupleBatch InternalNext() override {
    TupleBatch output_batch;
    if (!buffer_.empty()) {
      output_batch = buffer_.back();
      buffer_.pop_back();
      return output_batch;
    }
    output_batch.Init(output_schema_.GetTypes(), max_batch_size_);
    probe_batch_ = right_->Next();
    if (probe_batch_.size() == 0) {
      return output_batch;
    }
    for (size_t i = 0; i < right_hash_exprs_.size(); ++i) {
      right_hash_exprs_[i].Evaluate(probe_batch_.GetCols(), probe_batch_.size(),
          right_hash_exprs_res_[i]);
    }
    for (size_t tuple_id = 0; tuple_id < probe_batch_.size(); ++tuple_id) {
      if (!probe_batch_.IsValid(tuple_id)) {
        continue;
      }
      size_t hashkey = 0;
      hashkey = ComputeHashKey(right_hash_exprs_res_, tuple_id);
      if (hash_table_.find(hashkey) != hash_table_.end()) {
        for (auto build_idx : hash_table_[hashkey]) {
          auto [batch_id, build_tuple_id] = build_idx;
          TupleBatch::SingleTuple build_tuple =
              build_batches_[batch_id].GetSingleTuple(build_tuple_id);
          TupleBatch::SingleTuple probe_tuple =
              probe_batch_.GetSingleTuple(tuple_id);
          std::vector<StaticFieldRef> joined_tuple;
          for (size_t i = 0; i < build_tuple.size(); i++) {
            joined_tuple.push_back(build_tuple[i]);
          }
          for (size_t i = 0; i < probe_tuple.size(); i++) {
            joined_tuple.push_back(probe_tuple[i]);
          }
          if (output_batch.size() == max_batch_size_) {
            if (predicate_) {
              predicate_.Evaluate(output_batch.GetCols(), output_batch.size(), predicate_result_);
              for (size_t i = 0; i < output_batch.size(); ++i) {
                if (predicate_result_.Get(i).ReadInt() == 0) {
                  output_batch.SetValid(i, false);
                }
              }
            }
            buffer_.push_back(output_batch);
            output_batch.Init(output_schema_.GetTypes(), max_batch_size_);
          }
          output_batch.Append(joined_tuple);
        }
      }
    }
    if (predicate_) {
      predicate_.Evaluate(output_batch.GetCols(), output_batch.size(), predicate_result_);
      for (size_t i = 0; i < output_batch.size(); ++i) {
        if (predicate_result_.Get(i).ReadInt() == 0) {
          output_batch.SetValid(i, false);
        }
      }
    }
    return output_batch;
  }

 private:
  std::unique_ptr<VecExecutor> left_;
  std::unique_ptr<VecExecutor> right_;
  OutputSchema output_schema_;
  ExecOptions exec_options_;
  std::vector<ExprVecExecutor> left_hash_exprs_;
  std::vector<ExprVecExecutor> right_hash_exprs_;
  std::vector<Vector> left_hash_exprs_res_;
  std::vector<Vector> right_hash_exprs_res_;
  std::unordered_map<size_t, std::vector<std::tuple<int, int>>> hash_table_;
  std::vector<TupleBatch> build_batches_;
  // ssize_t current_probe_index_ = 0;
  // size_t current_tuple_index_ = 0;
  ExprVecExecutor predicate_;
  TupleBatch probe_batch_;
  std::vector<TupleBatch> buffer_;

  // TupleBatch hashed_tuples_;
  // size_t hashed_tuples_index_ = 0;
  Vector predicate_result_;


  void BuildHashTable(TupleBatch& batch, size_t batch_id) {

    for (int i = 0; i < left_hash_exprs_.size(); i++) {
      left_hash_exprs_[i].Evaluate(batch.GetCols(), batch.size(), left_hash_exprs_res_[i]);
    }

    for (int i = 0; i < batch.size(); i++) {
      if (batch.IsValid(i)){
        size_t seed = 0x1234;
        for (int j = 0; j < left_hash_exprs_res_.size(); j++) {
          auto value = left_hash_exprs_res_[j].Get(i);
          if (left_hash_exprs_res_[j].GetElemType() == LogicalType::INT) {
            seed = utils::Hash8(value.ReadInt(), seed);
          } else if (left_hash_exprs_res_[j].GetElemType() == LogicalType::FLOAT) {
            seed = utils::Hash8(value.ReadInt(), seed); 
          } else if (left_hash_exprs_res_[j].GetElemType() == LogicalType::STRING) {
            seed = utils::Hash(value.ReadStringView(), seed);
          }
        }
        hash_table_[seed].push_back(std::make_tuple(batch_id, i));
      }
    }
  }

  size_t ComputeHashKey(vector<Vector> result, size_t i){

    size_t seed = 0x1234;
    for (auto j = 0; j < result.size(); j++){
      auto value = result[j].Get(i);  
      if (result[j].GetElemType() == LogicalType::INT) {
        seed = utils::Hash8(value.ReadInt(), seed); 
      } else if (result[j].GetElemType() == LogicalType::FLOAT) {
        seed = utils::Hash8(value.ReadInt(), seed);  
      } else if (result[j].GetElemType() == LogicalType::STRING) {
        seed = utils::Hash(value.ReadStringView(), seed);  
      }
    }
    return seed;

  }
  virtual size_t GetTotalOutputSize() const override {
    return left_->GetTotalOutputSize() + right_->GetTotalOutputSize() +
           stat_output_size_;
  }

};

}  // namespace wing

