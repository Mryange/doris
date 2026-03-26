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

#include "exec/operator/distinct_streaming_aggregation_operator.h"

#include <gen_cpp/Metrics_types.h>

#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "exprs/vectorized_agg_fn.h"

namespace doris {
class ExecNode;
class RuntimeState;
} // namespace doris

namespace doris {
#include "common/compile_check_begin.h"

DistinctStreamingAggLocalState::DistinctStreamingAggLocalState(RuntimeState* state,
                                                               OperatorXBase* parent)
        : PipelineXLocalState<FakeSharedState>(state, parent),
          batch_size(state->batch_size()),
          _agg_data(std::make_unique<DistinctDataVariants>()),
          _child_block(Block::create_unique()) {}

Status DistinctStreamingAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_init_timer);
    _build_timer = ADD_TIMER(Base::custom_profile(), "BuildTime");
    _hash_table_compute_timer = ADD_TIMER(Base::custom_profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(Base::custom_profile(), "HashTableEmplaceTime");
    _hash_table_input_counter =
            ADD_COUNTER(Base::custom_profile(), "HashTableInputCount", TUnit::UNIT);
    _hash_table_size_counter = ADD_COUNTER(custom_profile(), "HashTableSize", TUnit::UNIT);
    _insert_keys_to_column_timer = ADD_TIMER(custom_profile(), "InsertKeysToColumnTime");

    return Status::OK();
}

Status DistinctStreamingAggLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_open_timer);
    RETURN_IF_ERROR(Base::open(state));
    auto& p = Base::_parent->template cast<DistinctStreamingAggOperatorX>();
    _probe_expr_ctxs.resize(p._probe_expr_ctxs.size());
    for (size_t i = 0; i < _probe_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._probe_expr_ctxs[i]->clone(state, _probe_expr_ctxs[i]));
    }
    RETURN_IF_ERROR(_init_hash_method(_probe_expr_ctxs));
    return Status::OK();
}

Status DistinctStreamingAggLocalState::_init_hash_method(const VExprContextSPtrs& probe_exprs) {
    RETURN_IF_ERROR(init_hash_method<DistinctDataVariants>(
            _agg_data.get(), get_data_types(probe_exprs),
            Base::_parent->template cast<DistinctStreamingAggOperatorX>()._is_first_phase));
    return Status::OK();
}

void DistinctStreamingAggLocalState::_make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : Base::_parent->cast<DistinctStreamingAggOperatorX>()._make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

void DistinctStreamingAggLocalState::_emplace_into_hash_table(ColumnRawPtrs& key_columns,
                                                              const uint32_t num_rows) {
    std::visit(
            Overload {[&](std::monostate& arg) -> void {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& agg_method) -> void {
                          SCOPED_TIMER(_hash_table_compute_timer);
                          using HashMethodType = std::decay_t<decltype(agg_method)>;
                          using AggState = typename HashMethodType::State;
                          AggState state(key_columns);
                          agg_method.init_serialized_keys(key_columns, num_rows);
                          auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                              HashMethodType::try_presis_key(key, origin, _arena);
                              ctor(key);
                          };
                          auto creator_for_null_key = [&]() {};

                          SCOPED_TIMER(_hash_table_emplace_timer);
                          lazy_emplace_batch_void(agg_method, state, num_rows, creator,
                                                  creator_for_null_key, [](uint32_t) {});

                          COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                      }},
            _agg_data->method_variant);
}

void DistinctStreamingAggLocalState::_output_distinct_keys(Block* block) {
    SCOPED_TIMER(_insert_keys_to_column_timer);
    size_t key_size = _probe_expr_ctxs.size();

    MutableColumns key_columns;
    for (size_t i = 0; i < key_size; ++i) {
        key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
    }

    std::visit(
            Overload {[&](std::monostate& arg) -> void {
                          throw doris::Exception(ErrorCode::INTERNAL_ERROR, "uninited hash table");
                      },
                      [&](auto& agg_method) -> void {
                          agg_method.init_iterator();
                          using KeyType = typename std::decay_t<decltype(agg_method)>::Key;
                          std::vector<KeyType> keys(batch_size);

                          uint32_t num_rows = 0;
                          auto& it = agg_method.begin;
                          while (it != agg_method.end && num_rows < batch_size) {
                              keys[num_rows] = it->get_first();
                              ++it;
                              ++num_rows;
                          }

                          agg_method.insert_keys_into_columns(keys, key_columns, num_rows);

                          // Handle null key after all non-null keys are exhausted
                          if (it == agg_method.end &&
                              agg_method.hash_table->has_null_key_data()) {
                              DCHECK(key_columns.size() == 1);
                              DCHECK(key_columns[0]->is_nullable());
                              key_columns[0]->insert_data(nullptr, 0);
                          }

                          _output_done = (it == agg_method.end);
                      }},
            _agg_data->method_variant);

    ColumnsWithTypeAndName columns_with_schema;
    for (size_t i = 0; i < key_size; ++i) {
        columns_with_schema.emplace_back(std::move(key_columns[i]),
                                         _probe_expr_ctxs[i]->root()->data_type(),
                                         _probe_expr_ctxs[i]->root()->expr_name());
    }
    block->swap(Block(columns_with_schema));
}

DistinctStreamingAggOperatorX::DistinctStreamingAggOperatorX(ObjectPool* pool, int operator_id,
                                                             const TPlanNode& tnode,
                                                             const DescriptorTbl& descs)
        : StatefulOperatorX<DistinctStreamingAggLocalState>(pool, tnode, operator_id, descs),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_first_phase(tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
        if (_is_streaming_preagg) {
            DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
        }
    } else {
        _is_streaming_preagg = false;
    }
}

Status DistinctStreamingAggOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    _op_name = "DISTINCT_STREAMING_AGGREGATION_OPERATOR";
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(StatefulOperatorX<DistinctStreamingAggLocalState>::prepare(state));
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, _child->row_desc()));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    init_make_nullable(state);
    return Status::OK();
}

void DistinctStreamingAggOperatorX::init_make_nullable(RuntimeState* state) {
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);

    for (size_t i = 0; i < _probe_expr_ctxs.size(); ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
}

Status DistinctStreamingAggOperatorX::push(RuntimeState* state, Block* in_block, bool eos) const {
    auto& local_state = get_local_state(state);
    local_state._input_num_rows += in_block->rows();
    if (in_block->rows() == 0) {
        return Status::OK();
    }

    // Build phase: only emplace into hash table, no output
    SCOPED_TIMER(local_state._build_timer);
    DCHECK(!local_state._probe_expr_ctxs.empty());

    size_t key_size = local_state._probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(local_state._expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(
                    local_state._probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
            key_columns[i]->assume_mutable()->replace_float_special_values();
        }
    }

    local_state._emplace_into_hash_table(key_columns, (uint32_t)in_block->rows());
    return Status::OK();
}

Status DistinctStreamingAggOperatorX::pull(RuntimeState* state, Block* block, bool* eos) const {
    auto& local_state = get_local_state(state);

    // Output phase: iterate hash table and produce output block
    local_state._output_distinct_keys(block);

    local_state._make_nullable_output_key(block);
    if (!_is_streaming_preagg) {
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(local_state.filter_block(local_state._conjuncts, block));
    }
    local_state.add_num_rows_returned(block->rows());

    // Handle limit
    if (_limit != -1 && local_state._num_rows_returned >= _limit) {
        auto over = local_state._num_rows_returned - _limit;
        block->set_num_rows(block->rows() - over);
        local_state._reach_limit = true;
    }

    *eos = local_state._output_done || local_state._reach_limit;
    return Status::OK();
}

bool DistinctStreamingAggOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    // Blocking mode: consume all input before producing output
    return !local_state._child_eos && !local_state._reach_limit;
}

Status DistinctStreamingAggLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter && !_probe_expr_ctxs.empty()) {
        std::visit(Overload {[&](std::monostate& arg) {
                                 // Do nothing
                             },
                             [&](auto& agg_method) {
                                 COUNTER_SET(_hash_table_size_counter,
                                             int64_t(agg_method.hash_table->size()));
                             }},
                   _agg_data->method_variant);
    }
    if (Base::_closed) {
        return Status::OK();
    }
    _arena.clear();
    return Base::close(state);
}

} // namespace doris
