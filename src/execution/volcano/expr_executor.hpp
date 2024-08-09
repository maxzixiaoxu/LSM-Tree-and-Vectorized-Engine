#pragma once

#include <span>

#include "catalog/schema.hpp"
#include "common/allocator.hpp"
#include "parser/expr.hpp"
#include "plan/output_schema.hpp"
#include "type/static_field.hpp"

namespace wing {

/**
 * This data structure stores the pointers to the results from storage or
 * expression evaluation results.
 *
 * If the result is from storage, then it points to the raw data.
 * If the result is from subqueries or join, then it points to an array of
 * StaticFieldRef.
 *
 * StaticFieldRef:
 *
 * For each result of executors, we use 8 bytes for each field, storing pointers
 * or numeric values.
 *
 * Different from Field, FieldRef,
 * Since the type is determined, we do not need to store type information in
 * fields. And by put the string size to the beginning of a string, we can avoid
 * storing string size in fields.
 *
 * For CHAR strings, we can avoid storing string size, but now for simplicity,
 * we consider them as VARCHAR.
 */
class SingleTuple {
 public:
  SingleTuple() : data_(nullptr) {}
  SingleTuple(const void* data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {}
  SingleTuple(const std::vector<StaticFieldRef>& vec)
    : data_(reinterpret_cast<const uint8_t*>(vec.data())) {}
  template <typename T>
  T Read(uint32_t offset) const {
    return *reinterpret_cast<const T*>(data_ + offset);
  }
  /**
   * It gets a reference to the raw VARCHAR string.
   * Used when it points to raw data.
   * In tuple/tuple.hpp, there exists an offset table in the tuple. Input
   * parameter is the offset of the offset of the string. This can be
   * pre-calculated because we make sure that the fixed length fields are in
   * front of the offset table.
   */
  StaticFieldRef CreateStringRef(uint32_t offset) const {
    return reinterpret_cast<const StaticStringField*>(
        Read<uint32_t>(offset) + data_);
  }
  const uint8_t* Data() const { return data_; }
  operator bool() const { return data_ != nullptr; }

 private:
  const uint8_t* data_;
};

/**
 * This data structure stores intermediate results of aggregate functions.
 *
 * Since StaticFieldRef is too small to evaluate avg() which needs a float
 * number and a integer number. We use this to evaluate complex aggregate
 * functions. (Although I think we don't need to do that...)
 */
class AggIntermediateData {
 public:
  union {
    int64_t int_data;
    double double_data;
  } data_{0};
  size_t size_{0};

  void Init() {
    size_ = 0;
    data_.int_data = 0;
  }
};

/**
 * This data structure can evaluate an expression with no aggregate functions.
 * It create a std::function object in its constructor,
 * and find the positions of the parameters of the function in input_schema.
 * It assumes that the input provided in Evaluate has the same schema as
 * input_schema.
 */
class ExprExecutor {
 public:
  ExprExecutor(const Expr* expr, const OutputSchema& input_schema);
  StaticFieldRef Evaluate(SingleTuple input) const;
  operator bool() const;

 private:
  std::function<StaticFieldRef(SingleTuple)> func_;
};

/**
 * This data structure can evaluate an expression with no aggregate functions.
 * It is used in join executors.
 * In join executor, the predicate receives two SingleTuple from two childern
 * and evaluate.
 */
class JoinExprExecutor {
 public:
  JoinExprExecutor(const Expr* expr, const OutputSchema& left_input_schema,
      const OutputSchema& right_input_schema);
  StaticFieldRef Evaluate(
      SingleTuple left_input, SingleTuple right_input) const;
  operator bool() const;

 private:
  std::function<StaticFieldRef(SingleTuple, SingleTuple)> func_;
};

/**
 * This data structure can evaluate an expression containing aggregate
 * functions. It create a std::function object in its constructor, and find the
 * correct reading method of the parameters of the function in input_schema. To
 * use it, you should invoke FirstEvaluate at the first tuple, then Aggregate at
 * each tuple, then LastEvaluate when there are no new tuples. In Aggregate, it
 * evaluates aggregate expressions. In LastEvaluate, it uses the stored
 * parameters and aggregate expressions to evaluate the result.
 */
class AggregateExprExecutor {
 public:
  AggregateExprExecutor(const Expr* expr, const OutputSchema& input_schema);
  size_t GetImmediateDataSize() { return aggregate_exprfunction_.size(); }
  void FirstEvaluate(AggIntermediateData* aggregate_data, SingleTuple input);
  void Aggregate(AggIntermediateData* aggregate_data, SingleTuple input);
  StaticFieldRef LastEvaluate(
      AggIntermediateData* aggregate_data, SingleTuple stored_parameter);
  operator bool() const;

 private:
  /* Function for last evaluate. */
  std::function<StaticFieldRef(SingleTuple, const AggIntermediateData*)> func_;
  /* Function for evaluate the expression inside aggreagte functions. i.e. sum(x
   * + 2), the x + 2.*/
  std::vector<ExprExecutor> aggregate_exprfunction_;
  /* Aggregate functions. i.e. sum(x), max(x), min(x) and so on. */
  std::vector<std::function<void(AggIntermediateData&, StaticFieldRef)>>
      aggregate_func_;
};

}  // namespace wing
