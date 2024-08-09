#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

bool overlaps(
    std::shared_ptr<SSTable> table1, std::shared_ptr<SSTable> table2) {
  return ((table1->GetLargestKey() <= table2->GetLargestKey()) &&
             (table1->GetLargestKey() >= table2->GetSmallestKey())) ||
         ((table1->GetSmallestKey() <= table2->GetLargestKey()) &&
             (table1->GetSmallestKey() >= table2->GetSmallestKey()));
}

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  bool is_trivial_move = false;
  int src_level = -1, target_level = -1;
  int levels = -1;
  if (version) {
    levels = version->GetLevels().size();
  }

  if (levels > 0 && version->GetLevels()[0].GetRuns().size() > level0_compaction_trigger_) {
    src_level = 0;
    target_level = 1;
  }

  for (size_t level = 1; level < levels; ++level) {
    size_t level_size = version->GetLevels()[level].size();
    if (level_size >= base_level_size_ * std::pow(ratio_, level)) {
      src_level = level;
      target_level = level + 1;
      break;
    }
  }

  std::shared_ptr<wing::lsm::SSTable> table;

  if (src_level != -1) {
    std::shared_ptr<SortedRun> target_run = nullptr;
    if (target_level < levels) {
      if (src_level != 0) {
        auto curr = version->GetLevels()[src_level].GetRuns()[0];
        ParsedKey target_l = version->GetLevels()[target_level].GetRuns()[0]->GetSmallestKey();
        ParsedKey target_r = version->GetLevels()[target_level].GetRuns()[0]->GetLargestKey();
        int rsf = -1;

        for (auto& curr_table : curr->GetSSTs()) {
          ParsedKey l = curr_table->GetSmallestKey();
          ParsedKey r = curr_table->GetLargestKey();

          if (target_l > r || l > target_r) {
            table = curr_table;
            break;
          } else {
            int curr = 0;
            for (auto& t_table : version->GetLevels()[target_level].GetRuns()[0]->GetSSTs()) {
              if (overlaps(curr_table, t_table)) {
                curr += t_table->GetSSTInfo().size_;
              }
            }
            if (rsf == -1 || curr < rsf) {
              rsf = curr;
              table = curr_table;
            }
          }
        }
      }
      target_run = version->GetLevels()[target_level].GetRuns()[0];
    } else {
      is_trivial_move =
          true;  

        table = version->GetLevels()[src_level].GetRuns()[0]->GetSSTs()[0];
      if (target_level < levels) {
        
        target_run = version->GetLevels()[target_level].GetRuns()[0];
      }
    }

    if (src_level == 0) {
      
      return std::make_unique<Compaction>(
          std::vector<std::shared_ptr<SSTable>>{},
          version->GetLevels()[0].GetRuns(), src_level, target_level,
          target_run, is_trivial_move);
    } 

    return std::make_unique<Compaction>(
          std::vector<std::shared_ptr<SSTable>>{table},
          version->GetLevels()[src_level].GetRuns(), src_level, target_level,
          target_run, is_trivial_move);
  }

  return nullptr;  
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  size_t num_levels = version->GetLevels().size();

  if (num_levels == 0 || !version) {
    return nullptr;
  }

  for (int i = num_levels - 1; i >= 0; i--) {
    Level level = version->GetLevels()[i];
    std::shared_ptr<wing::lsm::SortedRun> target_run = nullptr;

    if (level.GetRuns().size() >= ratio_ ||
        level.size() >= (std::pow(ratio_, i) * base_level_size_)) {
      
      if (i == num_levels - 1) {
 
        std::unique_ptr<Compaction> compaction = std::make_unique<Compaction>(
            std::vector<std::shared_ptr<SSTable>>{}, level.GetRuns(),
            level.GetID(), level.GetID() + 1, target_run, true);
        compaction->SetLazyLeveling(true);
        return compaction;
      }

      if (num_levels > 2 &&
          i == num_levels - 2) {  
   
        std::shared_ptr<SortedRun> target_run =
            version->GetLevels()[num_levels - 1].GetRuns()[0];
        std::unique_ptr<Compaction> compaction = std::make_unique<Compaction>(
            std::vector<std::shared_ptr<SSTable>>{}, level.GetRuns(),
            level.GetID(), level.GetID() + 1, target_run, false);
        compaction->SetLazyLeveling(true);
        return compaction;
      } else {
      
        std::unique_ptr<Compaction> compaction = std::make_unique<Compaction>(
            std::vector<std::shared_ptr<SSTable>>{}, level.GetRuns(),
            level.GetID(), level.GetID() + 1, target_run, false);
        compaction->SetLazyLeveling(true);
        return compaction;
      }
    }
  }
  return nullptr;
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {

  if (alpha_ >= 0.3) {
    int comp_size_ratio = int(36 * alpha_);
    auto picker = std::make_unique<LeveledCompactionPicker>(
        comp_size_ratio, base_level_size_, 1);
    return picker->Get(version);
  }

  int ratio = 8;
 
  int sst_file_size = base_level_size_ / level0_compaction_trigger_;
  int new_base_level_size = sst_file_size * ratio;
  auto picker = std::make_unique<TieredCompactionPicker>(
      ratio, new_base_level_size, ratio);
  return picker->Get(version);

}

std::unique_ptr<Compaction> LazyLevelingCompactionPicker::Get(
    Version* version) {
  if (version->GetLevels().empty() || !version) {
    return nullptr;
  }

  std::vector<Level> levels = version->GetLevels();

  Level last_level = levels[levels.size() - 1];

  std::shared_ptr<wing::lsm::SortedRun> target_run = nullptr;
  Options options;
  auto level_size_limit = level0_compaction_trigger_ * options.sst_file_size;

  if (levels.size() == 1) {
    if (last_level.GetRuns().size() >= level0_compaction_trigger_) {
      std::unique_ptr<Compaction> compaction = std::make_unique<Compaction>(
          std::vector<std::shared_ptr<SSTable>>{}, last_level.GetRuns(),
          last_level.GetID(), last_level.GetID() + 1, target_run, true);
      compaction->SetLazyLeveling(true);
      return compaction;
    }
  }

  if (last_level.size() >=
      std::pow(ratio_, levels.size() - 1) * base_level_size_) {
    std::unique_ptr<Compaction> compaction = std::make_unique<Compaction>(
        std::vector<std::shared_ptr<SSTable>>{}, last_level.GetRuns(),
        last_level.GetID(), last_level.GetID() + 1, target_run, true);
    compaction->SetLazyLeveling(true);
    return compaction;
  }

  TieredCompactionPicker tiered_compact_pick(
      ratio_, base_level_size_, level0_compaction_trigger_);
  std::unique_ptr<Compaction> compaction = tiered_compact_pick.Get(version);
  if (compaction) {
    compaction->SetLazyLeveling(true);
    return compaction;
  }
  return nullptr;
}

}  // namespace lsm

}  // namespace wing