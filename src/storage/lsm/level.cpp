#include "storage/lsm/level.hpp"

namespace wing {

namespace lsm {

GetResult SortedRun::Get(Slice key, uint64_t seq, std::string* value) {
  
  int l = 0;
  int r = ssts_.size() - 1;
  int i = -1;
  
  while (l <= r){
    int m = l + (r-l)/2;

    if (key >= ssts_[m]->GetSmallestKey().user_key_ && key <= ssts_[m]->GetLargestKey().user_key_){

      i = m;
      break;

    }

    else if (key < ssts_[m]->GetSmallestKey().user_key_){

      r = m - 1;

    }
    
    else{

      l = m + 1;

    }
  }

  if (i == -1) {
    
    return GetResult::kNotFound;

  }

  return ssts_[i]->Get(key, seq, value);

}

SortedRunIterator SortedRun::Seek(Slice key, uint64_t seq) {
  
  int l = 0;
  int r = ssts_.size() - 1;
  int i = -1;
  
  while (l <= r){
    int m = l + (r-l)/2;

    if (key >= ssts_[m]->GetSmallestKey().user_key_ && key <= ssts_[m]->GetLargestKey().user_key_){

      i = m;
      break;

    }

    else if (key < ssts_[m]->GetSmallestKey().user_key_){

      r = m - 1;

    }
    
    else{

      l = m + 1;

    }
  }

  if (i == -1) {
  
    i++;

  }

  SSTableIterator sst_iterator = ssts_[i]->Begin();
  sst_iterator.Seek(key, seq);

  return SortedRunIterator(this, std::move(sst_iterator), ssts_[i]->GetSSTInfo().sst_id_);

}

SortedRunIterator SortedRun::Begin() { 

  return SortedRunIterator(this, ssts_[0]->Begin(), ssts_[0]->GetSSTInfo().sst_id_);

}

SortedRun::~SortedRun() {
  if (remove_tag_) {
    for (auto sst : ssts_) {
      sst->SetRemoveTag(true);
    }
  }
}

void SortedRunIterator::SeekToFirst() {

  sst_id_ = 0;
  sst_it_ = run_->ssts_[sst_id_]->Begin();

}

void SortedRunIterator::Seek(Slice key, seq_t seq) {

  *this = run_->Seek(key, seq);

}

bool SortedRunIterator::Valid() { 

  return sst_it_.Valid() && sst_id_ < run_->ssts_.size();

 }

Slice SortedRunIterator::key() const { 
  
  return sst_it_.key(); 
  
}

Slice SortedRunIterator::value() const { 

  return sst_it_.value();

}

void SortedRunIterator::Next() { 
  
  sst_it_.Next();

  if (sst_it_.Valid()){

    return;

  }

  sst_id_++;

  if (sst_id_ < run_->ssts_.size()){

    sst_it_ = run_->ssts_[sst_id_]->Begin();
  
  }
  
}

GetResult Level::Get(Slice key, uint64_t seq, std::string* value) {
  for (int i = runs_.size() - 1; i >= 0; --i) {
    auto res = runs_[i]->Get(key, seq, value);
    if (res != GetResult::kNotFound) {
      return res;
    }
  }
  return GetResult::kNotFound;
}

void Level::Append(std::vector<std::shared_ptr<SortedRun>> runs) {
  for (auto& run : runs) {
    size_ += run->size();
  }
  runs_.insert(runs_.end(), std::make_move_iterator(runs.begin()),
      std::make_move_iterator(runs.end()));
}

void Level::Append(std::shared_ptr<SortedRun> run) {
  size_ += run->size();
  runs_.push_back(std::move(run));
}

}  // namespace lsm

}  // namespace wing
