
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

#include <gtest/gtest.h>

#include <memory>

#include "pipeline/exec/aggregation_sink_operator.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/assert_num_rows_operator.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_agg_fn_evaluator.h"
#include "vec/data_types/data_type_number.h"

namespace doris::pipeline {

std::shared_ptr<AggSinkOperatorX> create_agg_sink_op(OperatorContext& ctx, bool is_merge,
                                                     bool without_key) {
    auto op = std::make_shared<AggSinkOperatorX>();
    op->_aggregate_evaluators.push_back(
            vectorized::create_mock_agg_fn_evaluator(ctx.pool, is_merge, without_key));
    op->_pool = &ctx.pool;
    EXPECT_TRUE(op->_calc_aggregate_evaluators().ok());
    return op;
}

std::shared_ptr<AggSourceOperatorX> create_agg_source_op(OperatorContext& ctx) {
    auto op = std::make_shared<AggSourceOperatorX>();
    op->mock_row_descriptor.reset(
            new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &ctx.pool});
    return op;
}

TEST(AggOperatorTest, test_without_key_needs_finalize) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);
    sink_op->_is_merge = false;

    auto source_op = create_agg_source_op(ctx);
    source_op->_without_key = true;
    source_op->_needs_finalize = true;

    auto shared_state = OperatorHelper::init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        std::cout << "eos : " << eos << std::endl;
        std::cout << "block rows : " << block.rows() << std::endl;
        std::cout << "block : " << block.dump_data() << std::endl;
        EXPECT_TRUE(
                ColumnHelper::block_equal(block, ColumnHelper::create_block<DataTypeInt64>({6})));
    }
}

TEST(AggOperatorTest, test_without_key_no_needs_finalize) {
    using namespace vectorized;
    OperatorContext ctx;

    auto sink_op = create_agg_sink_op(ctx, false, true);
    sink_op->_is_merge = false;

    auto source_op = create_agg_source_op(ctx);
    source_op->_without_key = true;
    source_op->_needs_finalize = false;

    auto shared_state = OperatorHelper::init_sink_and_source(sink_op, source_op, ctx);

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3});
        auto st = sink_op->sink(&ctx.state, &block, true);
        EXPECT_TRUE(st.ok()) << st.msg();
    }

    {
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>({});
        bool eos = false;
        auto st = source_op->get_block(&ctx.state, &block, &eos);
        EXPECT_TRUE(st.ok()) << st.msg();
        EXPECT_TRUE(eos);
        EXPECT_EQ(block.rows(), 1);
        EXPECT_TRUE(
                check_and_get_column<ColumnFixedLengthObject>(*block.get_by_position(0).column));
    }
}

vectorized::Block test_agg_1_phase(vectorized::Block origin_block) {
    using namespace vectorized;
    OperatorContext ctx1;

    auto sink_op1 = create_agg_sink_op(ctx1, false, true);
    sink_op1->_is_merge = false;

    auto source_op1 = create_agg_source_op(ctx1);
    source_op1->_without_key = true;
    source_op1->_needs_finalize = false;

    auto shared_state = OperatorHelper::init_sink_and_source(sink_op1, source_op1, ctx1);

    EXPECT_TRUE(sink_op1->sink(&ctx1.state, &origin_block, true).ok());

    vectorized::Block serialize_block = ColumnHelper::create_block<DataTypeInt64>({});

    bool eos = false;
    EXPECT_TRUE(source_op1->get_block(&ctx1.state, &serialize_block, &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(serialize_block.rows(), 1);
    EXPECT_TRUE(check_and_get_column<ColumnFixedLengthObject>(
            *serialize_block.get_by_position(0).column));

    return serialize_block;
}

void test_agg_2_phase(vectorized::Block serialize_block) {
    using namespace vectorized;
    OperatorContext ctx2;

    auto sink_op2 = create_agg_sink_op(ctx2, true, false);
    sink_op2->_is_merge = true;

    auto source_op2 = create_agg_source_op(ctx2);
    source_op2->_without_key = true;
    source_op2->_needs_finalize = true;

    auto shared_state2 = OperatorHelper::init_sink_and_source(sink_op2, source_op2, ctx2);

    EXPECT_TRUE(sink_op2->sink(&ctx2.state, &serialize_block, true).ok());

    vectorized::Block result_block = ColumnHelper::create_block<DataTypeInt64>({});

    bool eos = false;
    EXPECT_TRUE(source_op2->get_block(&ctx2.state, &result_block, &eos).ok());

    EXPECT_TRUE(eos);
    EXPECT_EQ(result_block.rows(), 1);
    std::cout << "block : " << result_block.dump_data() << std::endl;
}

TEST(AggOperatorTest, test_without_key_2_phase) {
    using namespace vectorized;
    auto serialize_block = test_agg_1_phase(ColumnHelper::create_block<DataTypeInt64>({1, 2, 3}));
    test_agg_2_phase(serialize_block);
}

} // namespace doris::pipeline
