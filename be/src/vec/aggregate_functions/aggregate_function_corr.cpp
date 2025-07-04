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

#include "aggregate_function_corr.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_corr_function(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    assert_binary(name, argument_types);

    DCHECK(argument_types[0]->get_primitive_type() == argument_types[1]->get_primitive_type());

    switch ((argument_types[0]->get_primitive_type())) {
    case PrimitiveType::TYPE_TINYINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<TYPE_TINYINT, TYPE_TINYINT, CorrMoment>>>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_SMALLINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<TYPE_SMALLINT, TYPE_SMALLINT, CorrMoment>>>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_INT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<TYPE_INT, TYPE_INT, CorrMoment>>>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_BIGINT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<TYPE_BIGINT, TYPE_BIGINT, CorrMoment>>>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_FLOAT:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<TYPE_FLOAT, TYPE_FLOAT, CorrMoment>>>(
                argument_types, result_is_nullable);
    case PrimitiveType::TYPE_DOUBLE:
        return creator_without_type::create<
                AggregateFunctionBinary<StatFunc<TYPE_DOUBLE, TYPE_DOUBLE, CorrMoment>>>(
                argument_types, result_is_nullable);
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Aggregate function {} only support numeric types", name);
    }
    return nullptr;
}

void register_aggregate_functions_corr(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("corr", create_aggregate_corr_function);
}

AggregateFunctionPtr create_aggregate_corr_welford_function(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const bool result_is_nullable,
                                                            const AggregateFunctionAttr& attr) {
    assert_binary(name, argument_types);

    if (argument_types[0]->get_primitive_type() != TYPE_DOUBLE ||
        argument_types[1]->get_primitive_type() != TYPE_DOUBLE) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Aggregate function {} only support double", name);
    }

    return creator_without_type::create<
            AggregateFunctionBinary<StatFunc<TYPE_DOUBLE, TYPE_DOUBLE, CorrMomentWelford>>>(
            argument_types, result_is_nullable);
}

void register_aggregate_functions_corr_welford(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("corr_welford", create_aggregate_corr_welford_function);
}

} // namespace doris::vectorized
