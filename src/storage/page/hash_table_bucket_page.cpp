//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool find = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      find = true;
    }
  }
  return find;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsOccupied(BUCKET_ARRAY_SIZE - 1)) {
    ReOrganize();
    if (IsOccupied(BUCKET_ARRAY_SIZE - 1)) {
      return false;
    }
  }
  uint32_t occupy_index = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i) && occupy_index != BUCKET_ARRAY_SIZE) {
      break;
    }
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        return false;
      }
    } else if (occupy_index == BUCKET_ARRAY_SIZE) {
      occupy_index = i;
    }
  }
  assert(occupy_index != BUCKET_ARRAY_SIZE);
  SetOccupied(occupy_index);
  array_[occupy_index] = {key, value};
  SetReadable(occupy_index);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        SetUnreadable(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::InsertNoCheck(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsOccupied(BUCKET_ARRAY_SIZE - 1)) {
    ReOrganize();
    if (IsOccupied(BUCKET_ARRAY_SIZE - 1)) {
      return false;
    }
  }
  uint32_t occupy_index = BUCKET_ARRAY_SIZE;
  uint32_t l = 0;
  uint32_t r = BUCKET_ARRAY_SIZE;
  while (l < r) {
    uint32_t mid = (l + r) / 2;
    if (IsOccupied(mid)) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  occupy_index = l;
  SetOccupied(occupy_index);
  array_[occupy_index] = {key, value};
  SetReadable(occupy_index);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::FastInsert(KeyType key, ValueType value, uint32_t index) {
  SetOccupied(index);
  array_[index] = {key, value};
  SetReadable(index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ReOrganize() {
  uint32_t tail = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      array_[tail] = array_[i];
      SetReadable(tail);
      tail++;
    }
  }
  for (uint32_t i = tail; i < BUCKET_ARRAY_SIZE; i++) {
    SetUnoccupied(i);
    SetUnreadable(i);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  SetUnreadable(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return (occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] |= (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return (readable_[bucket_idx / 8] & (1 << (bucket_idx % 8))) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] |= (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnreadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetUnoccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] &= ~(1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  if (!IsOccupied(BUCKET_ARRAY_SIZE - 1)) {
    return false;
  }
  uint32_t n = sizeof(readable_) / sizeof(char);
  uint32_t i;
  for (i = 0; i + 4 < n; i += 4) {
    if (*reinterpret_cast<int32_t *>(readable_ + i) != -1) {
      return false;
    }
  }
  for (i = i * 8; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t ret = 0;
  uint32_t n = sizeof(readable_) / sizeof(char);
  uint32_t i;
  for (i = 0; i + 4 <= n; i += 4) {
    if (!IsOccupied(i * 8)) {
      return ret;
    }
    ret += __builtin_popcount(*reinterpret_cast<uint32_t *>(readable_ + i));
  }
  for (i = i * 8; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      ret++;
    }
  }
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  uint32_t n = sizeof(readable_) / sizeof(char);
  uint32_t i;
  for (i = 0; i + 4 < n; i += 4) {
    if (!IsOccupied(i * 8)) {
      return true;
    }
    if (*reinterpret_cast<int32_t *>(readable_ + i) != 0) {
      return false;
    }
  }
  for (i = i * 8; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
