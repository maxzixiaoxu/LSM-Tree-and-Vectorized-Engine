  #pragma once

  #include "storage/lsm/sst.hpp"

  namespace wing {

  namespace lsm {

  class CompactionJob {
   public:
    CompactionJob(FileNameGenerator* gen, size_t block_size, size_t sst_size,
        size_t write_buffer_size, size_t bloom_bits_per_key, bool use_direct_io)
      : file_gen_(gen),
        block_size_(block_size),
        sst_size_(sst_size),
        write_buffer_size_(write_buffer_size),
        bloom_bits_per_key_(bloom_bits_per_key),
        use_direct_io_(use_direct_io) {}

    /**
     * It receives an iterator and returns a list of SSTable
     */
    template <typename IterT>
    std::vector<SSTInfo> Run(IterT&& it) {
      std::vector<SSTInfo> sst_infos;
      std::vector<std::pair<std::string, std::string>> temp;
      size_t curr_size = 0;
      std::string a;
      std::string b;
      bool add = true;

      while (it.Valid()) {
        size_t record_size = ParsedKey(it.key()).size() + it.value().size() + 3*sizeof(uint32_t); 

        if (!temp.empty() && ParsedKey(temp.back().first).user_key_ == ParsedKey(it.key()).user_key_) {
          if (ParsedKey(temp.back().first).seq_ <= ParsedKey(it.key()).seq_) {
            int rec_size = ParsedKey(temp.back().first).size() + temp.back().second.size() + 3*sizeof(uint32_t);
            temp.pop_back();
            curr_size -= rec_size;
          } else {
            add = false;
          }
        }

        if (curr_size + record_size > sst_size_) {
          std::vector<std::pair<std::string, std::string>>* t = &temp;
          std::pair<std::string, size_t> pair = file_gen_->Generate();
          SSTableBuilder builder(std::make_unique<FileWriter>(std::make_unique<SeqWriteFile>(pair.first, use_direct_io_), write_buffer_size_), block_size_, bloom_bits_per_key_);

          for (auto& kv_pair : *t) {
            builder.Append(ParsedKey(kv_pair.first), kv_pair.second);
          }
          builder.Finish();
          SSTInfo ssti = SSTInfo{builder.size(), builder.count(), pair.second, builder.GetIndexOffset(), builder.GetBloomFilterOffset(), pair.first};
          sst_infos.push_back(ssti);
          temp.clear();
          curr_size = 0;
        }
        
        if (add) {
          a = it.key();
          b = it.value();
          auto pair = std::make_pair(a, b);
          temp.push_back(pair);
          curr_size += record_size;
        }
        it.Next();
        add = true;
      }

      if (!temp.empty()) {
        
        std::vector<std::pair<std::string, std::string>>* t = &temp;
        std::pair<std::string, size_t> pair = file_gen_->Generate();
        SSTableBuilder builder(std::make_unique<FileWriter>(std::make_unique<SeqWriteFile>(pair.first, use_direct_io_), write_buffer_size_), block_size_, bloom_bits_per_key_);

        for (auto& kv_pair : *t) {
          builder.Append(ParsedKey(kv_pair.first), kv_pair.second);
        }
        builder.Finish();
        SSTInfo ssti = SSTInfo{builder.size(), builder.count(), pair.second, builder.GetIndexOffset(), builder.GetBloomFilterOffset(), pair.first};
        sst_infos.push_back(ssti);
      }

      return sst_infos;
    }

   private:
    /* Generate new SSTable file name */
    FileNameGenerator* file_gen_;
    /* The target block size */
    size_t block_size_;
    /* The target SSTable size */
    size_t sst_size_;
    /* The size of write buffer in FileWriter */
    size_t write_buffer_size_;
    /* The number of bits per key in bloom filter */
    size_t bloom_bits_per_key_;
    /* Use O_DIRECT or not */
    bool use_direct_io_;
  };

  }  // namespace lsm

  }  // namespace wing

