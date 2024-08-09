#include "common/murmurhash.hpp"

namespace wing {

namespace utils {

// https://github.com/hhrhhr/MurmurHash-for-Lua/blob/master/MurmurHash64A.c
size_t Hash(const char* _data, size_t n, size_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995LLU;
  const int r = 47;

  uint64_t h = seed ^ (n * m);

  const uint64_t* data = (const uint64_t*)_data;
  const uint64_t* end = (n >> 3) + data;

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char* data2 = (const unsigned char*)data;

  switch (n & 7) {
    case 7:
      h ^= (uint64_t)(data2[6]) << 48;
    case 6:
      h ^= (uint64_t)(data2[5]) << 40;
    case 5:
      h ^= (uint64_t)(data2[4]) << 32;
    case 4:
      h ^= (uint64_t)(data2[3]) << 24;
    case 3:
      h ^= (uint64_t)(data2[2]) << 16;
    case 2:
      h ^= (uint64_t)(data2[1]) << 8;
    case 1:
      h ^= (uint64_t)(data2[0]);
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

size_t Hash(std::string_view str, size_t seed) {
  return Hash(str.data(), str.length(), seed);
}

size_t Hash8(const void* _data, size_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995LLU;
  const int r = 47;

  uint64_t h = seed ^ m;

  const uint64_t* data = (const uint64_t*)_data;
  uint64_t k = *data;

  k *= m;
  k ^= k >> r;
  k *= m;

  h ^= k;
  h *= m;

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

size_t Hash8(size_t data, size_t seed) { return Hash8(&data, seed); }

}  // namespace utils

}  // namespace wing
