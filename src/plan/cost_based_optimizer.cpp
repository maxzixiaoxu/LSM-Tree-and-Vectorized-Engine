#include <queue>

#include "plan/optimizer.hpp"
#include "plan/predicate_transfer/pt_graph.hpp"
#include "rules/convert_to_hash_join.hpp"

namespace wing {

std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules, const DB& db) {
  for (auto& a : rules) {
    if (a->Match(plan.get())) {
      plan = a->Transform(std::move(plan));
      break;
    }
  }
  if (plan->ch2_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
    plan->ch2_ = Apply(std::move(plan->ch2_), rules, db);
  } else if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  /* We don't want to consider values clause in cost based optimizer. */
  if (plan->type_ == PlanType::Print) {
    return 10000;
  }

  if (plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

bool CheckIsAllJoin(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan ||
      plan->type_ == PlanType::RangeScan) {
    return true;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckIsAllJoin(plan->ch_.get()) && CheckIsAllJoin(plan->ch2_.get());
}

bool CheckHasStat(const PlanNode* plan, const DB& db) {
  if (plan->type_ == PlanType::Print) {
    return false;
  }
  if (plan->type_ == PlanType::SeqScan) {
    auto stat =
        db.GetTableStat(static_cast<const SeqScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ == PlanType::RangeScan) {
    auto stat = db.GetTableStat(
        static_cast<const RangeScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckHasStat(plan->ch_.get(), db) &&
         CheckHasStat(plan->ch2_.get(), db);
}

/**
 * Check whether we can use cost based optimizer.
 * For simplicity, we only use cost based optimizer when:
 * (1) The root plan node is Project, and there is only one Project.
 * (2) The other plan nodes can only be Join or SeqScan or RangeScan.
 * (3) The number of tables is <= 20.
 * (4) All tables have statistics or true cardinality is provided.
 */
bool CheckCondition(const PlanNode* plan, const DB& db) {
  if (GetTableNum(plan) > 20)
    return false;
  if (plan->type_ != PlanType::Project && plan->type_ != PlanType::Aggregate)
    return false;
  if (!CheckIsAllJoin(plan->ch_.get()))
    return false;
  return db.GetOptions().optimizer_options.true_cardinality_hints ||
         CheckHasStat(plan->ch_.get(), db);
}

std::unique_ptr<PlanNode> GeneratePlan(size_t S, std::vector<std::unique_ptr<PlanNode>> &table_plan, 
  std::vector<int>& choose, std::vector<bool>& is_hj, std::vector<BitVector>& bitsets_, 
  std::vector<PredicateElement>& vec){

  if(__builtin_popcountll(S) == 1){
    return table_plan[__builtin_ctz(S)]->clone();
  }

  auto join_plan=std::make_unique<JoinPlanNode>();
  join_plan->table_bitset_= bitsets_[S];
  join_plan->ch_= GeneratePlan(choose[S], table_plan, choose, is_hj, bitsets_, vec);
  join_plan->ch2_= GeneratePlan(S-choose[S],table_plan, choose, is_hj, bitsets_, vec);
  join_plan->output_schema_ = OutputSchema::Concat(join_plan->ch_->output_schema_, join_plan->ch2_->output_schema_);

  PredicateVec pred_vec;
  DB_INFO("S:{}, {}", S, bitsets_[S].ToString());

  for(size_t i = 0; i < vec.size(); i++){
    if (vec[i].CheckLeft(bitsets_[S]) && vec[i].CheckRight(bitsets_[S]) && 
        !(vec[i].CheckLeft(join_plan->ch_->table_bitset_) 
          && vec[i].CheckRight(join_plan->ch_->table_bitset_)) && 
        !(vec[i].CheckLeft(join_plan->ch2_->table_bitset_) 
          && vec[i].CheckRight(join_plan->ch2_->table_bitset_))) {
      pred_vec.Append(vec[i].clone());
    }
  }

  join_plan->predicate_ = std::move(pred_vec); 
  return join_plan;

}

void DFS(const PlanNode* plan, std::vector<PredicateElement>& vec, 
std::vector<std::string>& table_names , std::vector<BitVector>& bitset_,
std::vector<std::unique_ptr<PlanNode>>& table_plan) {

  if(plan->type_ == PlanType::SeqScan) {
    auto sp = static_cast<const SeqScanPlanNode*>(plan);
    table_names.push_back(sp->table_name_in_sql_);
    bitset_.push_back(sp->table_bitset_);
    table_plan.push_back(sp->clone());
    return;
  }

  if(plan->ch_ != nullptr)
    DFS(plan->ch_.get(), vec, table_names, bitset_, table_plan);
  if(plan->ch2_ != nullptr)
    DFS(plan->ch2_.get(),vec, table_names, bitset_, table_plan);

  if (plan->type_ == PlanType::Join) {
    auto jp = static_cast<const JoinPlanNode*>(plan);
    for (auto& element : jp->predicate_.GetVec()) {
      vec.push_back(element.clone());
    }
  }

}

std::unique_ptr<PlanNode> DP(std::unique_ptr<PlanNode> plan, DB& db) {

  if (!db.GetOptions().optimizer_options.true_cardinality_hints) {
    return plan;
  }

  std::vector<PredicateElement> vec;
  std::vector<BitVector> bitsets_;
  std::vector<std::string> table_names;
  std::vector<std::unique_ptr<PlanNode>> table_plan;
  DFS(plan.get(), vec, table_names, bitsets_, table_plan);
  auto true_card = db.GetOptions().optimizer_options.true_cardinality_hints.value();
  std::vector<double> sz(1 << 20);
  std::vector<BitVector> table_set(1 << 20);
  std::vector<double> dp(1 << 20);
  std::vector<int> choose(1 << 20);
  std::vector<bool> is_hj(1<<20);
  size_t n = table_names.size();

  for(size_t i = 0; i < true_card.size(); i++){
    size_t bit_num = 0;
    bool flag = true;
    for(size_t j=0; j<true_card[i].first.size(); j++){
      auto x = -1;
      
      for(size_t k = 0; k < table_names.size(); k++){
        if(table_names[k] == true_card[i].first[j]){
          x = k;
        }
      }
      
      if (x == -1) {
        flag = false;
        break;
      }
      bit_num += pow(2,x) ;
    }
    if (!flag) {
      continue;
    }
    sz[bit_num] = true_card[i].second;
  }

  double scan_cost = db.GetOptions().optimizer_options.scan_cost;
  double hash_join_cost = db.GetOptions().optimizer_options.hash_join_cost;
  
  for (size_t i = 0; i < n; i++) {
    table_set[1 << i] = bitsets_[i];
    dp[1 << i] = scan_cost * sz[1 << i];
    choose[1 << i] = 0;
    is_hj[1<<i] = 0;
  }

  for (size_t S = 0; S < (1 << n); S++) if (__builtin_popcountll(S) >= 2) {
    size_t start_ = 0;
    size_t binary_rep = S;
    BitVector current_bitv;
    while(binary_rep > 0){
      size_t last_bit = binary_rep % 2;
      if(last_bit){
        current_bitv = bitsets_[start_]|current_bitv;
      }
      start_++;
      binary_rep /= 2;
    }
    table_set[S] = current_bitv;
  }

  for (size_t S = 0; S < (1 << n); S++) if (__builtin_popcountll(S) >= 2){
    dp[S] = 1e200;
    for (int T = (S - 1) & S; T >=(S/2); T = (T - 1) & S) {
      double current_cost = dp[T]+dp[S-T];
      
      auto L = table_set[T];
      auto R = table_set[S-T];
      bool can_hj = false;
      for(size_t ii = 0; ii < vec.size(); ii++){
       
          if (vec[ii].expr_->op_ == OpType::EQ) {
            
            if (!vec[ii].CheckRight(L) && !vec[ii].CheckLeft(R) && vec[ii].CheckRight(R) &&
                vec[ii].CheckLeft(L)) {
              can_hj = true;
              break;
            }
            if (!vec[ii].CheckLeft(L) && !vec[ii].CheckRight(R) && vec[ii].CheckRight(L) &&
                vec[ii].CheckLeft(R)) {
              can_hj = true;
              break;
            }
          }
      }
      
      if(can_hj){
        double hj_cost = std::min(scan_cost*sz[T]*sz[S-T], hash_join_cost*(sz[T]+sz[S-T])+scan_cost*sz[S]);
        current_cost += hj_cost;
      }else{
        double nl_cost = scan_cost*sz[T]*sz[S-T];
        current_cost += nl_cost;
      }
      
      if(choose[S] == 0 || current_cost<dp[S]){
        dp[S] = current_cost;
        choose[S] = T;
        is_hj[S] = can_hj;
      }

    }
  }

  plan->ch_ = GeneratePlan((1 << n) - 1, table_plan, choose, is_hj, table_set, vec);
  plan->cost_ = dp[(1 << n) - 1];
  return plan;

}

std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(
    std::unique_ptr<PlanNode> plan, DB& db) {
  if (CheckCondition(plan.get(), db) &&
      db.GetOptions().optimizer_options.enable_cost_based) {
    // std::vector<std::unique_ptr<OptRule>> R;
    // R.push_back(std::make_unique<ConvertToHashJoinRule>());
    // plan = Apply(std::move(plan), R, db);
    // TODO...
    plan = DP(std::move(plan), db);
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);

  } else {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);
  }
  if (db.GetOptions().exec_options.enable_predicate_transfer) {
    if (plan->type_ != PlanType::Insert && plan->type_ != PlanType::Delete &&
        plan->type_ != PlanType::Update) {
      auto pt_plan = std::make_unique<PredicateTransferPlanNode>();
      pt_plan->graph_ = std::make_shared<PtGraph>(plan.get());
      pt_plan->output_schema_ = plan->output_schema_;
      pt_plan->table_bitset_ = plan->table_bitset_;
      pt_plan->ch_ = std::move(plan);
      plan = std::move(pt_plan);
    }
  }
  return plan;
}

}  // namespace wing
