//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  buffer_pool_manager_->NewPage(&directory_page_id_);
  HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  page_id_t new_page_id;
  buffer_pool_manager_->NewPage(&new_page_id);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  dir_page->SetBucketPageId(0, new_page_id);
  dir_page->SetLocalDepth(0, 0);
  buffer_pool_manager->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  return FetchBucketPage(page);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  ReadLock<ReaderWriterLatch> read_lock(&table_latch_);
  page_id_t bucket_page_id;

  {
    HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
    bucket_page_id = KeyToPageId(key, &*dir_page);
  }

  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->RLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(page);
  bool ret = bucket->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  ReadLock<ReaderWriterLatch> read_lock(&table_latch_);

  page_id_t bucket_page_id;
  {
    HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
    bucket_page_id = KeyToPageId(key, &*dir_page);
  }

  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(page);
  bool ret = bucket->Insert(key, value, comparator_);
  if (ret || !bucket->IsFull()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, ret);
    return ret;
  }
  if (!bucket->IsFull()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    return false;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  read_lock.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  WriteLock<ReaderWriterLatch> write_lock(&table_latch_);
  page_id_t bucket_page_id;
  {
    HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
    bucket_page_id = KeyToPageId(key, &*dir_page);
  }

  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(page);

  // split
  while (!bucket->Insert(key, value, comparator_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    {
      HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
      uint32_t bucket_idx = KeyToDirectoryIndex(key, &*dir_page);
      dir_page->GetGlobalDepth();
      uint32_t global_depth = dir_page->GetGlobalDepth();
      if (dir_page->GetLocalDepth(bucket_idx) == global_depth) {
        if (dir_page->Size() * 2 > DIRECTORY_ARRAY_SIZE) {
          return false;
        }
        IncrGlobalDepth(&*dir_page);
      }
      if (!IncrLocalDepth(&*dir_page, bucket_idx)) {
        return false;
      }
      dir_page.SetDirty();
      bucket_page_id = KeyToPageId(key, &*dir_page);
    }
    page = buffer_pool_manager_->FetchPage(bucket_page_id);
    page->WLatch();
    bucket = FetchBucketPage(page);
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  WriteLock<ReaderWriterLatch> write_lock(&table_latch_);
  uint32_t bucket_page_id;
  {
    HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
    bucket_page_id = KeyToPageId(key, &*dir_page);
  }
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page);
  if (bucket->Remove(key, value, comparator_)) {
    while (bucket->IsEmpty()) {
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      if (!Merge(transaction, key, value)) {
        break;
      }
      HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
      dir_page.SetDirty();
      if (dir_page->CanShrink()) {
        Shrink(&*dir_page);
      }
      bucket_page_id = KeyToPageId(key, &*dir_page);
      bucket = FetchBucketPage(bucket_page_id);
    }
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPageWrapper dir_page(directory_page_id_, buffer_pool_manager_);
  uint32_t bucket_idx = KeyToDirectoryIndex(key, &*dir_page);
  uint32_t this_page_id = dir_page->GetBucketPageId(bucket_idx);
  if (dir_page->GetLocalDepth(bucket_idx) == 0) {
    return false;
  }
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
  uint32_t new_local_depth_mask = dir_page->GetLocalDepthMask(bucket_idx) >> 1;
  uint32_t new_local_depth_value = bucket_idx & new_local_depth_mask;
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if ((i & new_local_depth_mask) == new_local_depth_value) {
      if (dir_page->GetLocalDepth(i) > local_depth) {
        // The bucket's local depth doesn't match its split image's local depth.
        return false;
      }
    }
  }
  uint32_t another_page_id = dir_page->GetBucketPageId(bucket_idx ^ (1 << (local_depth - 1)));
  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if ((i & new_local_depth_mask) == new_local_depth_value) {
      dir_page->SetBucketPageId(i, another_page_id);
      dir_page->DecrLocalDepth(i);
    }
  }
  buffer_pool_manager_->DeletePage(this_page_id);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Shrink(HashTableDirectoryPage *directory) {
  uint32_t max_local_depth = 0;
  for (uint32_t i = 0; i < directory->Size(); i++) {
    max_local_depth = std::max(max_local_depth, directory->GetLocalDepth(i));
  }
  while (directory->GetGlobalDepth() > max_local_depth) {
    directory->DecrGlobalDepth();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::IncrGlobalDepth(HashTableDirectoryPage *directory) {
  uint32_t size = directory->Size();
  for (uint32_t i = size * 2; i > 0; i--) {
    directory->SetBucketPageId(i - 1, directory->GetBucketPageId((i - 1) / 2));
  }
  directory->IncrGlobalDepth();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::IncrLocalDepth(HashTableDirectoryPage *directory, uint32_t bucket_idx) {
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  uint32_t current_local_mask = directory->GetLocalDepthMask(bucket_idx);
  uint32_t current_local_value = bucket_idx & current_local_mask;
  Page *bucket_page = buffer_pool_manager_->FetchPage(bucket_page_id);

  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page);
  bucket_page->RLatch();
  page_id_t new_bucket_page_id[2] = {INVALID_PAGE_ID, INVALID_PAGE_ID};
  // move data
  for (uint32_t i = 0; i < 2; i++) {
    uint32_t new_local_mask = current_local_mask << 1 | 1;
    uint32_t new_local_value = current_local_value | (i << local_depth);
    Page *new_page = buffer_pool_manager_->NewPage(new_bucket_page_id + i);
    HASH_TABLE_BUCKET_TYPE *new_bucket = FetchBucketPage(new_page);
    if (new_page == nullptr) {
      return false;
    }
    for (uint32_t j = 0; j < BUCKET_ARRAY_SIZE; j++) {
      if (bucket->IsReadable(j)) {
        KeyType key = bucket->KeyAt(j);
        ValueType value = bucket->ValueAt(j);
        if ((Hash(key) & new_local_mask) == new_local_value) {
          if (!new_bucket->Insert(key, value, comparator_)) {
            LOG_ERROR("insert failed");
            buffer_pool_manager_->UnpinPage(new_bucket_page_id[i], true);
            return false;
          }
          if (!bucket->IsReadable(j)) {
            new_bucket->RemoveAt(j);
          }
        }
      }
    }
    buffer_pool_manager_->UnpinPage(new_bucket_page_id[i], true);
  }
  bucket_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->DeletePage(bucket_page_id);
  for (uint32_t i = 0; i < directory->Size(); i++) {
    if (directory->GetBucketPageId(i) == bucket_page_id) {
      directory->SetBucketPageId(i, new_bucket_page_id[(i >> local_depth) & 1]);
      directory->IncrLocalDepth(i);
    }
  }
  return false;
}

/*****************************************************************************
 * HashTableDirectoryPageWrapper
 *****************************************************************************/

HashTableDirectoryPageWrapper::HashTableDirectoryPageWrapper(page_id_t directory_page_id,
                                                             BufferPoolManager *buffer_pool_manager)
    : buffer_pool_manager_(buffer_pool_manager), directory_page_id_(directory_page_id), is_dirty_(false) {
  page_ = buffer_pool_manager->FetchPage(directory_page_id);
  directory_ = reinterpret_cast<HashTableDirectoryPage *>(page_->GetData());
}

HashTableDirectoryPageWrapper::~HashTableDirectoryPageWrapper() {
  buffer_pool_manager_->UnpinPage(directory_page_id_, is_dirty_);
}

void HashTableDirectoryPageWrapper::SetDirty() { is_dirty_ = true; }

HashTableDirectoryPage &HashTableDirectoryPageWrapper::operator*() { return *directory_; }
HashTableDirectoryPage *HashTableDirectoryPageWrapper::operator->() { return directory_; }

/*****************************************************************************
 * WriteLock
 *****************************************************************************/

template <typename Mutex>
WriteLock<Mutex>::WriteLock(Mutex *mutex) : locked_(false), mutex_(mutex) {
  WLock();
}

template <typename Mutex>
WriteLock<Mutex>::~WriteLock() {
  if (locked_) {
    WUnlock();
  }
}

template <typename Mutex>
void WriteLock<Mutex>::WLock() {
  if (!locked_) {
    mutex_->WLock();
    locked_ = true;
  }
}

template <typename Mutex>
void WriteLock<Mutex>::WUnlock() {
  if (locked_) {
    mutex_->WUnlock();
    locked_ = false;
  }
}

/*****************************************************************************
 * ReadLock
 *****************************************************************************/

template <typename Mutex>
ReadLock<Mutex>::ReadLock(Mutex *mutex) : locked_(false), mutex_(mutex) {
  RLock();
}

template <typename Mutex>
ReadLock<Mutex>::~ReadLock() {
  if (locked_) {
    RUnlock();
  }
}

template <typename Mutex>
void ReadLock<Mutex>::RLock() {
  if (!locked_) {
    mutex_->RLock();
    locked_ = true;
  }
}

template <typename Mutex>
void ReadLock<Mutex>::RUnlock() {
  if (locked_) {
    mutex_->RUnlock();
    locked_ = false;
  }
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
