// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// #include <gtest/gtest.h>

// #include <vector>

// #include "common/object_pool.h"
// #include "pipeline/common/join_utils.h"
// #include "pipeline/exec/join/process_hash_table_probe.h"
// #include "testutil/column_helper.h"
// #include "vec/core/types.h"

// namespace doris::vectorized {

// template<typename HashTableCtxType ,TJoinOp  join_op >
// void test_for_hash_map(){

// }


// TEST(JoinHashMapTest, hash_maptest) {
//     ObjectPool pool;
//     using HashTableCtxType = PrimaryTypeHashTableContext<vectorized::UInt64>;
//     HashTableCtxType hash_table_ctx;

//     const size_t rows = 5;
//     const size_t batch_size = 10;
//     ColumnRawPtrs build_raw_ptrs;
//     auto build_column = ColumnHelper::create_column<DataTypeInt64>({1, 2, 3, 4, 5});

//     build_raw_ptrs.push_back(build_column.get());

//     hash_table_ctx.hash_table->prepare_build<TJoinOp::INNER_JOIN>(rows, batch_size, false);

//     hash_table_ctx.init_serialized_keys(build_raw_ptrs, rows, nullptr, true, true,
//                                         hash_table_ctx.hash_table->get_bucket_size());

//     hash_table_ctx.hash_table->build(hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(), rows,
//                                      false);
//     hash_table_ctx.bucket_nums.resize(batch_size);
//     hash_table_ctx.bucket_nums.shrink_to_fit();

//     hash_table_ctx.reset();

//     ColumnRawPtrs probe_raw_ptrs;
//     const int32_t probe_rows = 4;
//     auto probe_column = ColumnHelper::create_column<DataTypeInt64>({3, 5, 1, 6});
//     probe_raw_ptrs.push_back(probe_column.get());

//     hash_table_ctx.init_serialized_keys(probe_raw_ptrs, probe_rows, nullptr, true, false,
//                                         hash_table_ctx.hash_table->get_bucket_size());
//     hash_table_ctx.hash_table->pre_build_idxs(hash_table_ctx.bucket_nums);

//     uint32_t build_index = 0;
//     int probe_index = 0;

//     std::vector<uint32_t> probe_idxs;
//     std::vector<uint32_t> build_idxs;
//     bool probe_visited = false;
//     probe_idxs.resize(batch_size + 1);
//     build_idxs.resize(batch_size + 1);

//     std::cout << hash_table_ctx.bucket_nums.size() << std::endl;

//     auto [new_probe_idx, new_build_idx, matched_cnt] =
//             hash_table_ctx.hash_table->find_batch<TJoinOp::INNER_JOIN>(
//                     hash_table_ctx.keys, hash_table_ctx.bucket_nums.data(), probe_index,
//                     build_index, probe_rows, probe_idxs.data(), probe_visited, build_idxs.data(),
//                     nullptr, false, false, false);
//     probe_index = new_probe_idx;
//     build_index = new_build_idx;


//     std::cout << "probe_index:" << probe_index << std::endl;
//     std::cout << "build_index:" << build_index << std::endl;
//     std::cout << "matched_cnt:" << matched_cnt << std::endl;

//     for (int i = 0; i < matched_cnt; i++) {
//         std::cout << probe_idxs[i] << "\t" << build_idxs[i] << std::endl;
//     }
// }




// } // namespace doris::vectorized
