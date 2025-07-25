// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <fmt/format.h>

#include <memory>

#include "olap/lru_cache.h"
#include "runtime/memory/cache_policy.h"
#include "runtime/memory/lru_cache_value_base.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/time.h"

namespace doris {
#include "common/compile_check_begin.h"

// Base of lru cache, allow prune stale entry and prune all entry.
class LRUCachePolicy : public CachePolicy {
public:
    LRUCachePolicy(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                   uint32_t stale_sweep_time_s, uint32_t num_shards = DEFAULT_LRU_CACHE_NUM_SHARDS,
                   uint32_t element_count_capacity = DEFAULT_LRU_CACHE_ELEMENT_COUNT_CAPACITY,
                   bool enable_prune = true, bool is_lru_k = DEFAULT_LRU_CACHE_IS_LRU_K)
            : CachePolicy(type, capacity, stale_sweep_time_s, enable_prune),
              _lru_cache_type(lru_cache_type) {
        if (check_capacity(capacity, num_shards)) {
            _cache = std::shared_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type), capacity, lru_cache_type, num_shards,
                                        element_count_capacity, is_lru_k));
        } else {
            _cache = std::make_shared<doris::DummyLRUCache>();
        }
        _init_mem_tracker(lru_cache_type_string(lru_cache_type));
        CacheManager::instance()->register_cache(this);
    }

    LRUCachePolicy(CacheType type, size_t capacity, LRUCacheType lru_cache_type,
                   uint32_t stale_sweep_time_s, uint32_t num_shards,
                   uint32_t element_count_capacity,
                   CacheValueTimeExtractor cache_value_time_extractor,
                   bool cache_value_check_timestamp, bool enable_prune = true,
                   bool is_lru_k = DEFAULT_LRU_CACHE_IS_LRU_K)
            : CachePolicy(type, capacity, stale_sweep_time_s, enable_prune),
              _lru_cache_type(lru_cache_type) {
        if (check_capacity(capacity, num_shards)) {
            _cache = std::shared_ptr<ShardedLRUCache>(
                    new ShardedLRUCache(type_string(type), capacity, lru_cache_type, num_shards,
                                        cache_value_time_extractor, cache_value_check_timestamp,
                                        element_count_capacity, is_lru_k));
        } else {
            _cache = std::make_shared<doris::DummyLRUCache>();
        }
        _init_mem_tracker(lru_cache_type_string(lru_cache_type));
        CacheManager::instance()->register_cache(this);
    }

    void reset_cache() { _cache.reset(); }

    bool check_capacity(size_t capacity, uint32_t num_shards) {
        if (capacity < num_shards) {
            LOG(INFO) << fmt::format(
                    "{} lru cache capacity({} B) less than num_shards({}), init failed, will be "
                    "disabled.",
                    type_string(type()), capacity, num_shards);
            _enable_prune = false;
            return false;
        }
        return true;
    }

    static std::string lru_cache_type_string(LRUCacheType type) {
        switch (type) {
        case LRUCacheType::SIZE:
            return "size";
        case LRUCacheType::NUMBER:
            return "number";
        default:
            throw Exception(
                    Status::FatalError("not match type of lru cache:{}", static_cast<int>(type)));
        }
    }

    std::shared_ptr<MemTrackerLimiter> mem_tracker() const {
        DCHECK(_mem_tracker != nullptr);
        return _mem_tracker;
    }

    int64_t mem_consumption() {
        DCHECK(_mem_tracker != nullptr);
        return _mem_tracker->consumption();
    }

    int64_t value_mem_consumption() {
        DCHECK(_value_mem_tracker != nullptr);
        return _value_mem_tracker->consumption();
    }

    // Insert will consume tracking_bytes to _mem_tracker and cache value destroy will release tracking_bytes.
    // If LRUCacheType::SIZE, value_tracking_bytes usually equal to charge.
    // If LRUCacheType::NUMBER, value_tracking_bytes usually not equal to charge, at this time charge is an weight.
    // If LRUCacheType::SIZE and value_tracking_bytes equals 0, memory must be tracked in Doris Allocator,
    //    cache value is allocated using Alloctor.
    // If LRUCacheType::NUMBER and value_tracking_bytes equals 0, usually currently cannot accurately tracking memory size,
    //    only tracking handle_size(106).
    Cache::Handle* insert(const CacheKey& key, void* value, size_t charge,
                          size_t value_tracking_bytes,
                          CachePriority priority = CachePriority::NORMAL) {
        size_t tracking_bytes = sizeof(LRUHandle) - 1 + key.size() + value_tracking_bytes;
        if (value != nullptr) {
            ((LRUCacheValueBase*)value)
                    ->set_tracking_bytes(tracking_bytes, _mem_tracker, value_tracking_bytes,
                                         _value_mem_tracker);
        }
        return _cache->insert(key, value, charge, priority);
    }

    Cache::Handle* lookup(const CacheKey& key) { return _cache->lookup(key); }

    void release(Cache::Handle* handle) { _cache->release(handle); }

    void* value(Cache::Handle* handle) { return _cache->value(handle); }

    void erase(const CacheKey& key) { _cache->erase(key); }

    int64_t get_usage() { return _cache->get_usage(); }

    size_t get_element_count() { return _cache->get_element_count(); }

    size_t get_capacity() override { return _cache->get_capacity(); }

    uint64_t new_id() { return _cache->new_id(); };

    // Subclass can override this method to determine whether to do the minor or full gc
    virtual bool exceed_prune_limit() {
        return _lru_cache_type == LRUCacheType::SIZE ? mem_consumption() > CACHE_MIN_PRUNE_SIZE
                                                     : get_usage() > CACHE_MIN_PRUNE_NUMBER;
    }

    // Try to prune the cache if expired.
    void prune_stale() override {
        std::lock_guard<std::mutex> l(_lock);
        COUNTER_SET(_freed_entrys_counter, (int64_t)0);
        COUNTER_SET(_freed_memory_counter, (int64_t)0);
        if (_stale_sweep_time_s <= 0 || std::dynamic_pointer_cast<doris::DummyLRUCache>(_cache)) {
            return;
        }
        if (exceed_prune_limit()) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            const int64_t curtime = UnixMillis();
            auto pred = [this, curtime](const LRUHandle* handle) -> bool {
                return static_cast<bool>((handle->last_visit_time + _stale_sweep_time_s * 1000) <
                                         curtime);
            };

            LOG(INFO) << fmt::format("[MemoryGC] {} prune stale start, consumption {}, usage {}",
                                     type_string(_type), mem_consumption(), get_usage());
            {
                SCOPED_TIMER(_cost_timer);
                // Prune cache in lazy mode to save cpu and minimize the time holding write lock
                PrunedInfo pruned_info = _cache->prune_if(pred, true);
                COUNTER_SET(_freed_entrys_counter, pruned_info.pruned_count);
                COUNTER_SET(_freed_memory_counter, pruned_info.pruned_size);
            }
            COUNTER_UPDATE(_prune_stale_number_counter, 1);
            LOG(INFO) << fmt::format(
                    "[MemoryGC] {} prune stale {} entries, {} bytes, cost {}, {} times prune",
                    type_string(_type), _freed_entrys_counter->value(),
                    _freed_memory_counter->value(), _cost_timer->value(),
                    _prune_stale_number_counter->value());
        } else {
            if (_lru_cache_type == LRUCacheType::SIZE) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune stale, LRUCacheType::SIZE consumption {} "
                        "less "
                        "than CACHE_MIN_PRUNE_SIZE {}",
                        type_string(_type), mem_consumption(), CACHE_MIN_PRUNE_SIZE);
            } else if (_lru_cache_type == LRUCacheType::NUMBER) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune stale, LRUCacheType::NUMBER usage {} less "
                        "than "
                        "CACHE_MIN_PRUNE_NUMBER {}",
                        type_string(_type), get_usage(), CACHE_MIN_PRUNE_NUMBER);
            }
        }
    }

    void prune_all(bool force) override {
        std::lock_guard<std::mutex> l(_lock);
        COUNTER_SET(_freed_entrys_counter, (int64_t)0);
        COUNTER_SET(_freed_memory_counter, (int64_t)0);
        if (std::dynamic_pointer_cast<doris::DummyLRUCache>(_cache)) {
            return;
        }
        if ((force && mem_consumption() != 0) || exceed_prune_limit()) {
            COUNTER_SET(_cost_timer, (int64_t)0);
            LOG(INFO) << fmt::format("[MemoryGC] {} prune all start, consumption {}, usage {}",
                                     type_string(_type), mem_consumption(), get_usage());
            {
                SCOPED_TIMER(_cost_timer);
                PrunedInfo pruned_info = _cache->prune();
                COUNTER_SET(_freed_entrys_counter, pruned_info.pruned_count);
                COUNTER_SET(_freed_memory_counter, pruned_info.pruned_size);
            }
            COUNTER_UPDATE(_prune_all_number_counter, 1);
            LOG(INFO) << fmt::format(
                    "[MemoryGC] {} prune all {} entries, {} bytes, cost {}, {} times prune, is "
                    "force: {}",
                    type_string(_type), _freed_entrys_counter->value(),
                    _freed_memory_counter->value(), _cost_timer->value(),
                    _prune_all_number_counter->value(), force);
        } else {
            if (_lru_cache_type == LRUCacheType::SIZE) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune all, force is {}, LRUCacheType::SIZE "
                        "consumption {}, "
                        "CACHE_MIN_PRUNE_SIZE {}",
                        type_string(_type), force, mem_consumption(), CACHE_MIN_PRUNE_SIZE);
            } else if (_lru_cache_type == LRUCacheType::NUMBER) {
                LOG(INFO) << fmt::format(
                        "[MemoryGC] {} not need prune all, force is {}, LRUCacheType::NUMBER "
                        "usage {}, CACHE_MIN_PRUNE_NUMBER {}",
                        type_string(_type), force, get_usage(), CACHE_MIN_PRUNE_NUMBER);
            }
        }
    }

    int64_t adjust_capacity_weighted_unlocked(double adjust_weighted) {
        auto capacity =
                static_cast<size_t>(static_cast<double>(_initial_capacity) * adjust_weighted);
        COUNTER_SET(_freed_entrys_counter, (int64_t)0);
        COUNTER_SET(_freed_memory_counter, (int64_t)0);
        COUNTER_SET(_cost_timer, (int64_t)0);
        if (std::dynamic_pointer_cast<doris::DummyLRUCache>(_cache)) {
            return 0;
        }

        size_t old_capacity = get_capacity();
        int64_t old_mem_consumption = mem_consumption();
        int64_t old_usage = get_usage();
        {
            SCOPED_TIMER(_cost_timer);
            PrunedInfo pruned_info = _cache->set_capacity(capacity);
            COUNTER_SET(_freed_entrys_counter, pruned_info.pruned_count);
            COUNTER_SET(_freed_memory_counter, pruned_info.pruned_size);
        }
        COUNTER_UPDATE(_adjust_capacity_weighted_number_counter, 1);
        LOG(INFO) << fmt::format(
                "[MemoryGC] {} update capacity, old <capacity {}, consumption {}, usage {}>, "
                "adjust_weighted {}, new <capacity {}, consumption {}, usage {}>, prune {} "
                "entries, {} bytes, cost {}, {} times prune",
                type_string(_type), old_capacity, old_mem_consumption, old_usage, adjust_weighted,
                get_capacity(), mem_consumption(), get_usage(), _freed_entrys_counter->value(),
                _freed_memory_counter->value(), _cost_timer->value(),
                _adjust_capacity_weighted_number_counter->value());
        return _freed_entrys_counter->value();
    }

    int64_t adjust_capacity_weighted(double adjust_weighted) override {
        std::lock_guard<std::mutex> l(_lock);
        return adjust_capacity_weighted_unlocked(adjust_weighted);
    }

    int64_t reset_initial_capacity(double adjust_weighted) override {
        DCHECK(adjust_weighted != 0.0); // otherwise initial_capacity will always to be 0.
        std::lock_guard<std::mutex> l(_lock);
        int64_t prune_num = adjust_capacity_weighted_unlocked(adjust_weighted);
        size_t old_capacity = _initial_capacity;
        _initial_capacity =
                static_cast<size_t>(static_cast<double>(_initial_capacity) * adjust_weighted);
        LOG(INFO) << fmt::format(
                "[MemoryGC] {} reset initial capacity, new capacity {}, old capacity {}, prune num "
                "{}",
                type_string(_type), _initial_capacity, old_capacity, prune_num);
        return prune_num;
    };

protected:
    void _init_mem_tracker(const std::string& type_name) {
        if (std::find(CachePolicy::MetadataCache.begin(), CachePolicy::MetadataCache.end(),
                      _type) == CachePolicy::MetadataCache.end()) {
            _mem_tracker = MemTrackerLimiter::create_shared(
                    MemTrackerLimiter::Type::CACHE,
                    fmt::format("{}[{}]", type_string(_type), type_name));
        } else {
            _mem_tracker = MemTrackerLimiter::create_shared(
                    MemTrackerLimiter::Type::METADATA,
                    fmt::format("{}[{}]", type_string(_type), type_name));
        }
        _value_mem_tracker = std::make_shared<MemTracker>(
                fmt::format("{}::Value[{}]", type_string(_type), type_name));
    }

    // if check_capacity failed, will return dummy lru cache,
    // compatible with ShardedLRUCache usage, but will not actually cache.
    std::shared_ptr<Cache> _cache;
    std::mutex _lock;
    LRUCacheType _lru_cache_type;

    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    std::shared_ptr<MemTracker> _value_mem_tracker;
};

#include "common/compile_check_end.h"
} // namespace doris
