#include "storage/lsm/version.hpp"

namespace wing {

namespace lsm {

bool Version::Get(std::string_view user_key, seq_t seq, std::string* value) {
  
  for (int i = 0; i < levels_.size(); i++){

    if (levels_[i].Get(user_key, seq, value) == GetResult::kFound){

      return true;

    }
  }

  return false;

}

void Version::Append(
    uint32_t level_id, std::vector<std::shared_ptr<SortedRun>> sorted_runs) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_runs));
}
void Version::Append(uint32_t level_id, std::shared_ptr<SortedRun> sorted_run) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_run));
}

bool SuperVersion::Get(
    std::string_view user_key, seq_t seq, std::string* value) {

    if (mt_->Get(user_key, seq, value) == GetResult::kFound){

      return true;

    }

    for (std::shared_ptr<MemTable> mt : *imms_) {
      if (mt->Get(user_key, seq, value) == GetResult::kFound) {
       
        return true;

      }
    }
  
    if (version_->Get(user_key, seq, value)){

      return true;

    }

    return false;

}

std::string SuperVersion::ToString() const {
  std::string ret;
  ret += fmt::format("Memtable: size {}, ", mt_->size());
  ret += fmt::format("Immutable Memtable: size {}, ", imms_->size());
  ret += fmt::format("Tree: [ ");
  for (auto& level : version_->GetLevels()) {
    size_t num_sst = 0;
    for (auto& run : level.GetRuns()) {
      num_sst += run->SSTCount();
    }
    ret += fmt::format("{}, ", num_sst);
  }
  ret += "]";
  return ret;
}

void SuperVersionIterator::SeekToFirst() { 

  it_.Clear();
  for (MemTableIterator& mt_it : mt_its_) {
    mt_it.SeekToFirst();
    it_.Push(&mt_it);
  }

  for (SortedRunIterator& sr_it : sst_its_) {
    sr_it.SeekToFirst();
    it_.Push(&sr_it);
  }

 }

void SuperVersionIterator::Seek(Slice key, seq_t seq) {

  SeekToFirst();
  std::vector<Iterator*> itv;
  while (Valid()) {

    if (typeid(*it_.Top()) == typeid(MemTableIterator)) {

      MemTableIterator* mit = dynamic_cast<MemTableIterator*>(it_.Top());
      mit->Seek(key, seq);

    } else {

      SortedRunIterator* sst = dynamic_cast<SortedRunIterator*>(it_.Top());
      sst->Seek(key, seq);

    }

      itv.push_back(it_.Top());
      it_.Pop();

  }

  for (int i = 0; i < itv.size(); i++) {
    
    it_.Push(itv[i]);
  
  }

}

bool SuperVersionIterator::Valid() { 
  
  return it_.Valid();

}

Slice SuperVersionIterator::key() const { 

  return it_.key();

 }

Slice SuperVersionIterator::value() const { 

  return it_.value();

}

void SuperVersionIterator::Next() { 

  it_.Next();

 }

}  // namespace lsm

}  // namespace wing

