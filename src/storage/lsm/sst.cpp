#include "storage/lsm/sst.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>

#include "common/bloomfilter.hpp"

namespace wing {

namespace lsm {

SSTable::SSTable(SSTInfo sst_info, size_t block_size, bool use_direct_io)
  : sst_info_(std::move(sst_info)), block_size_(block_size) {
  file_ = std::make_unique<ReadFile>(sst_info_.filename_, use_direct_io);
  FileReader reader(file_.get(), sst_info_.size_ - sst_info_.index_offset_, sst_info_.index_offset_);

  uint32_t reader_offset = sst_info_.index_offset_;

  while (reader_offset < sst_info_.bloom_filter_offset_){

      // read size
      uint32_t ksize = reader.ReadValue<uint32_t>();
      std::string user_key = reader.ReadString(ksize);
      seq_t seq_ = reader.ReadValue<seq_t>();
      RecordType type_ = reader.ReadValue<RecordType>();
      InternalKey key(user_key, seq_, type_);

      offset_t bh_offset = reader.ReadValue<offset_t>();
      offset_t bh_size = reader.ReadValue<offset_t>();
      offset_t bh_count = reader.ReadValue<offset_t>();

      
      BlockHandle handle;
      handle.offset_ = bh_offset;
      handle.size_ = bh_size;
      handle.count_ = bh_count;
      IndexValue iv;
      iv.key_ = key;
      iv.block_ = handle;

      index_.push_back(iv);
      reader_offset += 3 * sizeof(offset_t) + sizeof(uint32_t) + key.size();

  }

  offset_t bloom_filter_size = reader.ReadValue<uint32_t>();
  bloom_filter_ = reader.ReadString(bloom_filter_size);

  uint32_t sksize = reader.ReadValue<uint32_t>();
  std::string skuser_key = reader.ReadString(sksize);
  seq_t skseq_ = reader.ReadValue<seq_t>();
  RecordType sktype_ = reader.ReadValue<RecordType>();
  smallest_key_ = InternalKey(skuser_key, skseq_, sktype_);

  uint32_t lksize = reader.ReadValue<uint32_t>();
  std::string lkuser_key = reader.ReadString(lksize);
  seq_t lkseq_ = reader.ReadValue<seq_t>();
  RecordType lktype_ = reader.ReadValue<RecordType>();
  largest_key_ = InternalKey(lkuser_key, lkseq_, lktype_);

}

SSTable::~SSTable() {
  if (remove_tag_) {
    file_.reset();
    std::filesystem::remove(sst_info_.filename_);
  }
}

  /**
   * Try to get the associated value of key with the sequence number <= seq.
   * If the record has type RecordType::Value, then it copies the value,
   * and returns GetResult::kFound
   * If the record has type RecordType::Deletion, then it does nothing to the
   * value, and returns GetResult::kDelete If there is no such record, it
   * returns GetResult::kNotFound.
   * */
GetResult SSTable::Get(Slice key, uint64_t seq, std::string* value) {

  if (!(utils::BloomFilter::Find(key, bloom_filter_))){

    return GetResult::kNotFound;

  }

  int l = 0;
  int r = index_.size() - 1;
  int i = -1;
  
  while (l <= r){
    int m = l + (r-l)/2;

    if (ParsedKey(index_[m].key_) < ParsedKey(key, seq, RecordType::Value)){

      i = m;
      l = m + 1;

    }

    else{

      r = m - 1;

    }
  }

  if (i == -1){

    if (ParsedKey(key, seq, RecordType::Value) > ParsedKey(index_[0].key_)){

      return GetResult::kNotFound;

    }

  }

  i++;

  if (i >= index_.size()) {
    
    return GetResult::kNotFound;

  }

  BlockHandle bh = index_[i].block_;
  FileReader reader(file_.get(), bh.size_, bh.offset_);
  std::string block = reader.ReadString(bh.size_);
  BlockIterator it(block.c_str(), bh);
  
  GetResult getResult = GetResult::kNotFound;

  for (it; it.Valid(); it.Next()){

    ParsedKey pk(it.key());

    if (pk.user_key_ == key && pk.seq_ <= seq){
      if (pk.type_ == RecordType::Deletion){
        if(getResult == GetResult::kNotFound){
          getResult = GetResult::kDelete;
        }
      }

      else{

        *value = it.value();
        return GetResult::kFound;

      }
    }
  }

  i--;

  if (0 <= i && index_[i].key_.user_key() == key){
    BlockHandle bh = index_[i].block_;
    FileReader reader(file_.get(), bh.size_, bh.offset_);
    std::string block = reader.ReadString(bh.size_);
    BlockIterator it(block.c_str(), bh);

    for (it; it.Valid(); it.Next()){

      ParsedKey pk(it.key());

      if (pk.user_key_ == key && pk.seq_ <= seq){
        if (pk.type_ == RecordType::Deletion){
          if(getResult == GetResult::kNotFound){
             getResult = GetResult::kDelete;
          }
        }

        else{

          *value = it.value();
          getResult = GetResult::kFound;

        }
      }
    }
  }

  return getResult;

}

SSTableIterator SSTable::Seek(Slice key, uint64_t seq) {
  
  SSTableIterator ssti = Begin();
  ssti.Seek(key, seq);
  return ssti;

}

SSTableIterator SSTable::Begin() { 

  return SSTableIterator(this);

}

void SSTableIterator::Seek(Slice key, uint64_t seq) {

  auto is = sst_->index_;
  int l = 0;
  int r = is.size() - 1;
  int i = -1;
  
  while (l <= r){
    int m = l + (r-l)/2;

    if (ParsedKey(is[m].key_) < ParsedKey(key, seq, RecordType::Value)){

      i = m;
      l = m + 1;

    }

    else{

      r = m - 1;

    }
  }

  block_id_ = i;
  BlockHandle bh = sst_->index_[block_id_].block_;
  FileReader reader(sst_->file_.get(), bh.size_, bh.offset_);
  block_buf = reader.ReadString(bh.size_);
  block_it_ = BlockIterator(block_buf.c_str(), bh);
  block_it_.Seek(key, seq);

  if (!(block_it_.Valid())){

    block_id_++;
    if (block_id_ < is.size()){
      BlockHandle bh = sst_->index_[block_id_].block_;
      FileReader reader(sst_->file_.get(), bh.size_, bh.offset_);
      block_buf = reader.ReadString(bh.size_);
      block_it_ = BlockIterator(block_buf.c_str(), bh);
      block_it_.Seek(key, seq);
    }
  }
}

void SSTableIterator::SeekToFirst() { 

  block_id_ = 0;
  BlockHandle bh = sst_->index_[block_id_].block_;
  FileReader reader(sst_->file_.get(), bh.size_, bh.offset_);
  block_buf = reader.ReadString(bh.size_);
  block_it_ = BlockIterator(block_buf.c_str(), bh);

}

bool SSTableIterator::Valid() { 
  
  return block_it_.Valid() && 0 <= block_id_ && block_id_ < sst_->sst_info_.count_;

}

Slice SSTableIterator::key() const { 

  return block_it_.key();

}

Slice SSTableIterator::value() const { 

  return block_it_.value();

}

void SSTableIterator::Next() { 
  
  block_it_.Next();
  if (block_it_.Valid()){

    return;

  }
  block_id_++;

  if (block_id_ < sst_->index_.size()){

    BlockHandle bh = sst_->index_[block_id_].block_;
    FileReader reader(sst_->file_.get(), bh.size_, bh.offset_);
    block_buf = reader.ReadString(bh.size_);
    block_it_ = BlockIterator(block_buf.c_str(), bh);
  
  }

}
  
void SSTableBuilder::Append(ParsedKey key, Slice value) {

  if (block_builder_.Append(key, value)){

    if (count_ == 0){

      smallest_key_ = key;
      largest_key_ = key;

    }

    if (key.user_key_ > largest_key_.user_key() || (key.user_key_ == largest_key_.user_key()) && largest_key_.seq() > key.seq_){

      largest_key_ = key;

    }
    
    count_ ++;
    
    key_hashes_.push_back(utils::BloomFilter::BloomHash(key.user_key_));

  }

  else{

    IndexValue iv;
    iv.key_ = largest_key_;
    BlockHandle handle;
    handle.offset_ = current_block_offset_;
    handle.size_ = block_builder_.size();
    handle.count_ = block_builder_.count();
    iv.block_ = handle;
    index_data_.push_back(iv);
    current_block_offset_ += block_builder_.size();
    index_offset_ = current_block_offset_;
    bloom_filter_offset_ += block_builder_.size() + 3 * sizeof(offset_t) + sizeof(u_int32_t) + iv.key_.size();
    block_builder_.Finish();
    block_builder_.Clear();
    block_builder_.Append(key, value);
    key_hashes_.push_back(utils::BloomFilter::BloomHash(key.user_key_));
    
    if (count_ == 0){

      smallest_key_ = key;
      largest_key_ = key;

    }

    if (key.user_key_ > largest_key_.user_key() || (key.user_key_ == largest_key_.user_key()) && largest_key_.seq() > key.seq_){

      largest_key_ = key;

    }

    count_++;
      
  }
}

void SSTableBuilder::Finish() { 
  
  IndexValue iv;
  iv.key_ = largest_key_;
  BlockHandle handle;
  handle.offset_ = current_block_offset_;
  handle.size_ = block_builder_.size();
  handle.count_ = block_builder_.count();
  iv.block_ = handle;
  index_data_.push_back(iv);
  current_block_offset_ += block_builder_.size();
  index_offset_ = current_block_offset_;
  bloom_filter_offset_ += block_builder_.size() + 3 * sizeof(offset_t) + sizeof(u_int32_t) + iv.key_.size();
  block_builder_.Finish();
  block_builder_.Clear();
  
  for(int i = 0; i < (int) index_data_.size(); i++){
    
    ParsedKey pk = index_data_[i].key_;
    writer_->AppendValue<uint32_t>((pk).user_key_.size());
    writer_->AppendString((pk).user_key_);
    writer_->AppendValue<seq_t>((pk).seq_);
    writer_->AppendValue<RecordType>((pk).type_);

    writer_->AppendValue<offset_t>(index_data_[i].block_.offset_);
    writer_->AppendValue<offset_t>(index_data_[i].block_.size_);
    writer_->AppendValue<offset_t>(index_data_[i].block_.count_);

  }

  std::string bloom_filter;
  utils::BloomFilter::Create(count_, bloom_bits_per_key_, bloom_filter);

  for(int j = 0; j < (int) key_hashes_.size(); j++){

    utils::BloomFilter::Add(key_hashes_[j], bloom_filter);

  }

  writer_->AppendValue<uint32_t>(bloom_filter.size());
  writer_->AppendString(bloom_filter);

  writer_->AppendValue<u_int32_t>((smallest_key_).user_key().size());
  writer_->AppendString((smallest_key_).user_key());
  writer_->AppendValue<seq_t>((smallest_key_).seq());
  writer_->AppendValue<RecordType>((smallest_key_).record_type());
  writer_->AppendValue<u_int32_t>((largest_key_).user_key().size());
  writer_->AppendString((largest_key_).user_key());
  writer_->AppendValue<seq_t>((largest_key_).seq());
  writer_->AppendValue<RecordType>((largest_key_).record_type());
  writer_->Flush();

}  // namespace lsm

}  // namespace lsm

}  // namespace wing

