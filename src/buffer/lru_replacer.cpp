//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { iterator_.resize(num_pages, list_.end()); }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> scoped_lock(latch_);
  if (!list_.empty()) {
    *frame_id = list_.front();
    list_.pop_front();
    iterator_[*frame_id] = list_.end();
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> scoped_lock(latch_);
  if (iterator_[frame_id] == list_.end()) {
    return;
  }
  list_.erase(iterator_[frame_id]);
  iterator_[frame_id] = list_.end();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> scoped_lock(latch_);
  if (iterator_[frame_id] != list_.end()) {
    return;
  }
  list_.push_back(frame_id);
  iterator_[frame_id] = --list_.end();
}

size_t LRUReplacer::Size() {
  std::scoped_lock<std::mutex> scoped_lock(latch_);
  return list_.size();
}

}  // namespace bustub
