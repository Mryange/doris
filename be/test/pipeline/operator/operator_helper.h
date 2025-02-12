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
#include <gtest/gtest.h>

#include "pipeline/exec/operator.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
namespace doris::pipeline {

struct OperatorContext {
    OperatorContext() : profile("test") {};

    RuntimeProfile profile;
    MockRuntimeState state;

    ObjectPool pool;
};

struct OperatorHelper {
    static void init_local_state(OperatorContext& ctx, auto& op) {
        ctx.state.resize_op_id_to_local_state(-100);
        LocalStateInfo info {&ctx.profile, {}, 0, {}, 0};
        EXPECT_TRUE(op.setup_local_state(&ctx.state, info).ok());
    }

    static void init_local_state(OperatorContext& ctx, auto& op,
                                 const std::vector<TScanRangeParams>& scan_ranges) {
        ctx.state.resize_op_id_to_local_state(-100);
        LocalStateInfo info {&ctx.profile, scan_ranges, 0, {}, 0};
        EXPECT_TRUE(op.setup_local_state(&ctx.state, info).ok());
    }

    template <typename SinkOperator, typename SourceOperator>
    auto static init_sink_and_source(std::shared_ptr<SinkOperator> sink_op,
                                     std::shared_ptr<SourceOperator> source_op,
                                     OperatorContext& ctx) {
        auto shared_state = sink_op->create_shared_state();
        {
            auto local_state = SinkOperator::LocalState ::create_unique(sink_op.get(), &ctx.state);
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &ctx.profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state.get(),
                                     .le_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
            ctx.state.emplace_sink_local_state(0, std::move(local_state));
        }

        {
            auto local_state =
                    SourceOperator::LocalState::create_unique(&ctx.state, source_op.get());
            LocalStateInfo info {.parent_profile = &ctx.profile,
                                 .scan_ranges = {},
                                 .shared_state = shared_state.get(),
                                 .le_state_map = {},
                                 .task_idx = 0};

            EXPECT_TRUE(local_state->init(&ctx.state, info).ok());
            ctx.state.resize_op_id_to_local_state(-100);
            ctx.state.emplace_local_state(source_op->operator_id(), std::move(local_state));
        }

        {
            auto* sink_local_state = ctx.state.get_sink_local_state();
            EXPECT_TRUE(sink_local_state->open(&ctx.state).ok());
        }

        {
            auto* source_local_state = ctx.state.get_local_state(source_op->operator_id());
            EXPECT_TRUE(source_local_state->open(&ctx.state).ok());
        }
        return shared_state;
    }
};

} // namespace doris::pipeline