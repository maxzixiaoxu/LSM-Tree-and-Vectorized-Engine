#include "storage/lsm/block.hpp"

#include <iostream>
using namespace std;

namespace wing {

namespace lsm {

bool BlockBuilder::Append(ParsedKey key, Slice value) {
  
  size_t kvp_size = key.size() + value.size() + sizeof(u_int32_t) * 2;
  size_t check_size = key.size() + value.size() + sizeof(u_int32_t) * 3;
  if (check_size + current_size_ > block_size_){
      
    return false;

  }

  else{

    file_->AppendValue<uint32_t>(key.size());
    file_->AppendString(key.user_key_);
    file_->AppendValue(key.seq_);
    file_->AppendValue(key.type_);
    file_->AppendValue<uint32_t>(value.size());
    file_->AppendString(value);
    current_size_ += check_size;
    offsets_.push_back(offset_);
    offset_ += kvp_size;

    return true;
      
  }
  
}

void BlockBuilder::Finish() { 
  
  for(int i = 0; i < (int)count(); i += 1){

    file_->AppendValue<uint32_t>(offsets_[i]);

  }

  //current_size_ += offsets_.size() * sizeof(u_int32_t);

} 

void BlockIterator::Seek(Slice user_key, seq_t seq) {
  
  SeekToFirst();

  ParsedKey sk = ParsedKey (user_key, seq, RecordType::Value);

  while (Valid() && (ParsedKey(key()) < sk)) {
    Next();

  }

}

void BlockIterator::SeekToFirst() { 

  curr_ = data_;

}

Slice BlockIterator::key() const{

  //auto klen = *(const uint32_t*)(curr_);
  return Slice((curr_ + sizeof(uint32_t)), *(const uint32_t*)(curr_));

}

Slice BlockIterator::value() const{ 

  auto klen = *(const uint32_t*)(curr_);
  //auto vlen = *(const uint32_t*)(klen + curr_ + sizeof(uint32_t));
  return Slice(curr_ + 2 * sizeof(uint32_t) + klen, *(const uint32_t*)(curr_ + klen + sizeof(uint32_t)));
  
}

void BlockIterator::Next() { 

  auto klen = *(const uint32_t*)(curr_);
  curr_ += sizeof(uint32_t);
  curr_ += klen;
  auto vlen = *(const uint32_t*)(curr_);
  curr_ += sizeof(uint32_t);
  curr_ += vlen;

}

bool BlockIterator::Valid() { 

  return (curr_ >= data_) && (curr_ < (data_ + handle_.size_ - handle_.count_ * sizeof(u_int32_t))); 
  
}

}  // namespace lsm

}  // namespace wing
