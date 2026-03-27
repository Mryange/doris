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

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

#include "exec/common/hash_table/hash.h"
#include "exec/common/hash_table/phmap_fwd_decl.h"

// ============================================================================
// Benchmark: Doris HashSet vs SR-style HashSet for 10M int64 insert
//
// DorisHashSet:    HashCRC32 + EqualTo + Allocator_ (original Doris config)
// SRStyleHashSet:  StdHashWithPhmapMix + std::equal_to + std::allocator
// ============================================================================

static constexpr size_t NUM_ELEMENTS = 10'000'000;
static constexpr size_t PREFETCH_DIST = 16;

template <typename Key>
struct StdHashWithPhmapMix {
    size_t operator()(Key key) const {
        if constexpr (std::is_arithmetic_v<Key>) {
            return phmap::phmap_mix<sizeof(size_t)>()(std::hash<Key>()(key));
        } else {
            return phmap::phmap_mix<sizeof(size_t)>()(HashCRC32<Key>()(key));
        }
    }
};

// SR-style config: StdHashWithPhmapMix + std::equal_to + std::allocator
using SRStyleHashSet = phmap::flat_hash_set<int64_t, StdHashWithPhmapMix<int64_t>,
                                            std::equal_to<int64_t>, std::allocator<int64_t>>;

// Doris original config: HashCRC32 + EqualTo + Allocator_
using DorisHashSet = phmap::flat_hash_set<int64_t, HashCRC32<int64_t>, doris::EqualTo<int64_t>,
                                          doris::Allocator_<int64_t>>;

// Doris original config: HashCRC32 + EqualTo + Allocator_
using DorisHashSetWithStdAlloc =
        phmap::flat_hash_set<int64_t, HashCRC32<int64_t>, doris::EqualTo<int64_t>,
                             std::allocator<int64_t>>;
using SRStyleHashSetWithDorisAlloc =
        phmap::flat_hash_set<int64_t, StdHashWithPhmapMix<int64_t>, std::equal_to<int64_t>,
                             doris::Allocator_<int64_t>>;

// Generate shuffled 1..N data
static std::vector<int64_t> init_test_data() {
    std::vector<int64_t> v(NUM_ELEMENTS);
    std::iota(v.begin(), v.end(), 1);
    std::mt19937_64 rng(42);
    std::shuffle(v.begin(), v.end(), rng);
    return v;
}

// Global data, initialized before main() — no lazy init cost
static std::vector<int64_t> g_test_data = init_test_data();

// Benchmark: hash compute + prefetch insert (full path, all timed)
template <typename HashSet>
static void BM_HashSetInsert(benchmark::State& state) {
    const auto& data = g_test_data;

    for (auto _ : state) {
        HashSet set;
        // Precompute hash values (timed)
        std::vector<size_t> hashes(NUM_ELEMENTS);
        auto hash_fn = typename HashSet::hasher {};
        for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
            hashes[i] = hash_fn(data[i]);
        }

        // Insert with prefetch (timed)
        for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
            if (i + PREFETCH_DIST < NUM_ELEMENTS) {
                set.prefetch_hash(hashes[i + PREFETCH_DIST]);
            }
            set.emplace_with_hash(hashes[i], data[i]);
        }
        benchmark::DoNotOptimize(set);
    }
    state.SetItemsProcessed(state.iterations() * NUM_ELEMENTS);
}

BENCHMARK(BM_HashSetInsert<SRStyleHashSet>)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_HashSetInsert<SRStyleHashSetWithDorisAlloc>)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(10);

BENCHMARK(BM_HashSetInsert<DorisHashSet>)->Unit(benchmark::kMillisecond)->Iterations(10);
BENCHMARK(BM_HashSetInsert<DorisHashSetWithStdAlloc>)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(10);

BENCHMARK_MAIN();
