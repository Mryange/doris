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

#include "pipeline/exec/exchange_sink_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_data_stream_sender.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_exchange_sink_buffer.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
namespace doris::pipeline {

TUniqueId create_TUniqueId(int64_t hi, int64_t lo) {
    TUniqueId t {};
    t.hi = hi;
    t.lo = lo;
    return t;
}

TEST(ExchangeSinkOperatorTest, test_remote_and_local) {
    using namespace vectorized;

    OperatorContext ctx;

    TDataStreamSink sink;
    sink.dest_node_id = 0;
    sink.output_partition.type = TPartitionType::UNPARTITIONED;
    MockRowDescriptor row_desc {{std::make_shared<DataTypeInt32>()}, &ctx.pool};

    // init operator
    ExchangeSinkOperatorX op {&ctx.state, row_desc, 0, sink, {}, {}};
    op._sink_buffer = std::make_shared<MockExchangeSinkBuffer>(&ctx.state, 3);
    op._state = &ctx.state;
    op._compression_type = segment_v2::CompressionTypePB::NO_COMPRESSION;

    std::vector<std::shared_ptr<MockChannel>> mock_channel;
    {
        //init local state

        auto local_state = ExchangeSinkLocalState::create_unique(&op, &ctx.state);

        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 1), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 2), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 3), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 4), false));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 5), false));

        for (auto c : mock_channel) {
            local_state->channels.push_back(c);
        }

        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = &ctx.profile,
                                 .sender_id = 0,
                                 .shared_state = nullptr,
                                 .le_state_map = {},
                                 .tsink = TDataSink {}};
        EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
        ctx.state.emplace_sink_local_state(0, std::move(local_state));
    }
    {
        // open local state
        auto* sink_local_state = ctx.state.get_sink_local_state();
        EXPECT_TRUE(sink_local_state->open(&ctx.state).ok());
    }

    {
        //execute sink

        bool eos = true;
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        auto st = op.sink(&ctx.state, &block, eos);

        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        for (auto c : mock_channel) {
            auto block = c->get_block();

            EXPECT_TRUE(ColumnHelper::block_equal(
                    ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}), block));
        }
    }
}

TEST(ExchangeSinkOperatorTest, test_remote) {
    using namespace vectorized;

    OperatorContext ctx;

    TDataStreamSink sink;
    sink.dest_node_id = 0;
    sink.output_partition.type = TPartitionType::UNPARTITIONED;
    MockRowDescriptor row_desc {{std::make_shared<DataTypeInt32>()}, &ctx.pool};

    // init operator
    ExchangeSinkOperatorX op {&ctx.state, row_desc, 0, sink, {}, {}};
    op._sink_buffer = std::make_shared<MockExchangeSinkBuffer>(&ctx.state, 3);
    op._state = &ctx.state;
    op._compression_type = segment_v2::CompressionTypePB::NO_COMPRESSION;

    std::vector<std::shared_ptr<MockChannel>> mock_channel;
    {
        //init local state

        auto local_state = ExchangeSinkLocalState::create_unique(&op, &ctx.state);

        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 1), false));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 2), false));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 3), false));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 4), false));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 5), false));

        for (auto c : mock_channel) {
            local_state->channels.push_back(c);
        }

        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = &ctx.profile,
                                 .sender_id = 0,
                                 .shared_state = nullptr,
                                 .le_state_map = {},
                                 .tsink = TDataSink {}};
        EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
        ctx.state.emplace_sink_local_state(0, std::move(local_state));
    }
    {
        // open local state
        auto* sink_local_state = ctx.state.get_sink_local_state();
        EXPECT_TRUE(sink_local_state->open(&ctx.state).ok());
    }

    {
        //execute sink

        bool eos = true;
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        auto st = op.sink(&ctx.state, &block, eos);

        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        for (auto c : mock_channel) {
            auto block = c->get_block();

            EXPECT_TRUE(ColumnHelper::block_equal(
                    ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}), block));
        }
    }
}

TEST(ExchangeSinkOperatorTest, test_local) {
    using namespace vectorized;

    OperatorContext ctx;

    TDataStreamSink sink;
    sink.dest_node_id = 0;
    sink.output_partition.type = TPartitionType::UNPARTITIONED;
    MockRowDescriptor row_desc {{std::make_shared<DataTypeInt32>()}, &ctx.pool};

    // init operator
    ExchangeSinkOperatorX op {&ctx.state, row_desc, 0, sink, {}, {}};
    op._sink_buffer = std::make_shared<MockExchangeSinkBuffer>(&ctx.state, 3);
    op._state = &ctx.state;
    op._compression_type = segment_v2::CompressionTypePB::NO_COMPRESSION;

    std::vector<std::shared_ptr<MockChannel>> mock_channel;
    {
        //init local state

        auto local_state = ExchangeSinkLocalState::create_unique(&op, &ctx.state);

        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 1), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 2), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 3), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 4), true));
        mock_channel.push_back(
                std::make_shared<MockChannel>(local_state.get(), create_TUniqueId(1, 5), true));

        for (auto c : mock_channel) {
            local_state->channels.push_back(c);
        }

        LocalSinkStateInfo info {.task_idx = 0,
                                 .parent_profile = &ctx.profile,
                                 .sender_id = 0,
                                 .shared_state = nullptr,
                                 .le_state_map = {},
                                 .tsink = TDataSink {}};
        EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
        ctx.state.emplace_sink_local_state(0, std::move(local_state));
    }
    {
        // open local state
        auto* sink_local_state = ctx.state.get_sink_local_state();
        EXPECT_TRUE(sink_local_state->open(&ctx.state).ok());
    }

    {
        //execute sink

        bool eos = true;
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt32>({1, 2, 3});
        auto st = op.sink(&ctx.state, &block, eos);

        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        for (auto c : mock_channel) {
            auto block = c->get_block();

            EXPECT_TRUE(ColumnHelper::block_equal(
                    ColumnHelper::create_block<DataTypeInt32>({1, 2, 3}), block));
        }
    }
}

} // namespace doris::pipeline
