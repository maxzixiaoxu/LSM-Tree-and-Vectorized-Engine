#pragma once

#include "type/tuple_batch.hpp"
#include <unordered_map>
#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"
#include "plan/output_schema.hpp"
#include "type/tuple_batch.hpp"
#include "common/murmurhash.hpp"

#include <iostream>
using namespace std;

namespace wing {

class JoinVecExecutor : public VecExecutor {
 public:
  JoinVecExecutor(const ExecOptions& options,
      const std::unique_ptr<Expr>& expr, const OutputSchema& input_schema,
      std::unique_ptr<VecExecutor> ch, std::unique_ptr<VecExecutor> ch2)
    : VecExecutor(options),
      pred_(ExprVecExecutor::Create(expr.get(), input_schema)),
      schema_(input_schema),
      ch_(std::move(ch)),
      ch2_(std::move(ch2)){}

  void Init() override { 
    ch_->Init(); 
    ch2_->Init(); 
    build_.clear();
    TupleBatch batch;
    while ((batch = ch_->Next()).size() != 0) {
      build_.emplace_back();
      build_.back().Init(batch.GetColElemTypes(), max_batch_size_);
      for (auto tuple : batch)
        build_.back().Append(tuple);
    }

    probe_ = TupleBatch();

  }
  
  TupleBatch InternalNext() override {
    TupleBatch output;
    output.Init(schema_.GetTypes(), max_batch_size_); 
    while (true) {
      if (probe_index_ >= probe_.size()) {
        probe_ = ch2_->Next();
        if (probe_.size() == 0) {
          return output;
        }
        b_index_ = 0;
        build_index_ = 0;
        probe_index_ = 0;
      }
      for (uint32_t i = probe_index_; i < probe_.size(); i++){
        for (uint32_t j = build_index_; j < build_.size(); j++){
          auto pr = probe_.GetSingleTuple(i);
          auto& bb = build_[j];

          if (b_index_ == 0) {
            std::vector<Vector> input;
            for(size_t a = 0; a < pr.size(); a++){

              auto cv = Vector(VectorType::Constant, pr.GetElemType(a), bb.size());
              cv.Set(0, pr[a]);
              input.push_back(cv);

            }
            for (auto col : bb.GetCols()){
              
              input.push_back(Vector(col));

            }
            if (pred_){
              pred_.Evaluate(input, bb.size(), pred_result_);
            }
          }

          for (size_t b = b_index_; b < bb.size(); b++) {
            if (!pred_ || pred_result_.Get(b).ReadInt() == 1) {
              
              std::vector<StaticFieldRef> t;

              for (size_t i = 0; i < bb.GetCols().size(); i++){

                t.push_back(bb.Get(b, i));

              }

              for (size_t j = 0; j < pr.size(); j++){

                t.push_back(pr[j]);

              }
              output.Append(t);
              if (output.size() == max_batch_size_) {
                build_index_ = j;
                probe_index_ = i;
                b_index_ = b + 1;
                return output;
              }
            }
          }
          b_index_ = 0;
        }
        build_index_ = 0;
      }
      probe_index_ = probe_.size();
    }

    return output;
  }

 private:
  ExprVecExecutor pred_;
  Vector pred_result_;
  OutputSchema schema_;
  std::vector<TupleBatch> build_;
  TupleBatch probe_;
  size_t build_index_{0};
  size_t probe_index_{0};
  size_t b_index_{0};
  std::unique_ptr<VecExecutor> ch_;
  std::unique_ptr<VecExecutor> ch2_;

  virtual size_t GetTotalOutputSize() const override {
    return ch_->GetTotalOutputSize() + ch2_->GetTotalOutputSize() +
            stat_output_size_;
  }
};

}  // namespace wing
