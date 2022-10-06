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
  if (IsFull()) {
    MappingType tail = tail_.value_;
    if (cmp(key, tail.first) == 0) {
      result->push_back(tail.second);
    }
  }
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE - 1; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    return false;
  }
  uint32_t occupy_index = BUCKET_ARRAY_SIZE;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE - 1; i++) {
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
  InsertAt(key, value, occupy_index == BUCKET_ARRAY_SIZE ? BUCKET_ARRAY_SIZE - 1 : occupy_index);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE - 1; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      if (cmp(key, array_[i].first) == 0 && value == array_[i].second) {
        RemoveAt(i);
        return true;
      }
    }
  }
  if (IsFull() && cmp(key, tail_.value_.first) == 0 && tail_.value_.second == value) {
    RemoveAt(BUCKET_ARRAY_SIZE - 1);
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::InsertAt(KeyType key, ValueType value, uint32_t bucket_idx) {
  if (bucket_idx != BUCKET_ARRAY_SIZE - 1) {
    assert(!IsReadable(bucket_idx));
    SetOccupied(bucket_idx);
    array_[bucket_idx] = {key, value};
    tail_.properties_.readable_count_++;
    SetReadable(bucket_idx);
  } else {
    SetOccupied(BUCKET_ARRAY_SIZE - 1);
    tail_.value_ = {key, value};
    SetReadable(BUCKET_ARRAY_SIZE - 1);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  if (bucket_idx != BUCKET_ARRAY_SIZE - 1) {
    return array_[bucket_idx].first;
  }
  // assert(const_cast<HASH_TABLE_BUCKET_TYPE *>(this)->IsFull());
  return tail_.value_.first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  if (bucket_idx != BUCKET_ARRAY_SIZE - 1) {
    return array_[bucket_idx].second;
  }
  // assert(const_cast<HASH_TABLE_BUCKET_TYPE *>(this)->IsFull());
  return tail_.value_.second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (!IsReadable(bucket_idx)) {
    return;
  }
  if (bucket_idx == BUCKET_ARRAY_SIZE - 1) {
    assert(IsFull());
    SetUnreadable(bucket_idx);
    tail_.properties_.readable_count_ = BUCKET_ARRAY_SIZE - 1;
  } else {
    if (IsFull()) {
      array_[bucket_idx] = tail_.value_;
      tail_.properties_.readable_count_ = BUCKET_ARRAY_SIZE - 1;
      SetUnreadable(BUCKET_ARRAY_SIZE - 1);
    } else {
      tail_.properties_.readable_count_--;
      SetUnreadable(bucket_idx);
    }
  }
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
  return IsReadable(BUCKET_ARRAY_SIZE - 1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  if (IsFull()) {
    return BUCKET_ARRAY_SIZE;
  }
  return tail_.properties_.readable_count_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ReOrganize() {
  if (IsFull() || NumReadable() + 1 == BUCKET_ARRAY_SIZE) {
    return;
  }
  uint32_t tail = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE - 1; i++) {
    if (!IsOccupied(i)) {
      break;
    }
    if (IsReadable(i)) {
      array_[tail] = array_[i];
      SetOccupied(tail);
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
