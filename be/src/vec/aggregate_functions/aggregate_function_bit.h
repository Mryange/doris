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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionBitwise.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <memory>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class BufferReadable;
class BufferWritable;
class IColumn;
template <PrimitiveType T>
class DataTypeNumber;
template <PrimitiveType T>
class ColumnVector;

template <PrimitiveType T>
struct AggregateFunctionBaseData {
public:
    AggregateFunctionBaseData(typename PrimitiveTypeTraits<T>::CppType init_value)
            : res_bit(init_value) {}
    void write(BufferWritable& buf) const { buf.write_binary(res_bit); }
    void read(BufferReadable& buf) { buf.read_binary(res_bit); }
    typename PrimitiveTypeTraits<T>::CppType get() const { return res_bit; }

protected:
    typename PrimitiveTypeTraits<T>::CppType res_bit = {};
};

template <PrimitiveType T>
struct AggregateFunctionGroupBitOrData : public AggregateFunctionBaseData<T> {
public:
    static constexpr auto name = "group_bit_or";
    AggregateFunctionGroupBitOrData() : AggregateFunctionBaseData<T>(0) {}

    void add(typename PrimitiveTypeTraits<T>::CppType value) {
        AggregateFunctionBaseData<T>::res_bit |= value;
    }

    void merge(const AggregateFunctionGroupBitOrData<T>& rhs) {
        AggregateFunctionBaseData<T>::res_bit |= rhs.res_bit;
    }

    void reset() { AggregateFunctionBaseData<T>::res_bit = 0; }
};

template <PrimitiveType T>
struct AggregateFunctionGroupBitAndData : public AggregateFunctionBaseData<T> {
public:
    static constexpr auto name = "group_bit_and";
    AggregateFunctionGroupBitAndData() : AggregateFunctionBaseData<T>(-1) {}

    void add(typename PrimitiveTypeTraits<T>::CppType value) {
        AggregateFunctionBaseData<T>::res_bit &= value;
    }

    void merge(const AggregateFunctionGroupBitAndData<T>& rhs) {
        AggregateFunctionBaseData<T>::res_bit &= rhs.res_bit;
    }

    void reset() { AggregateFunctionBaseData<T>::res_bit = -1; }
};

template <PrimitiveType T>
struct AggregateFunctionGroupBitXorData : public AggregateFunctionBaseData<T> {
    static constexpr auto name = "group_bit_xor";
    AggregateFunctionGroupBitXorData() : AggregateFunctionBaseData<T>(0) {}

    void add(typename PrimitiveTypeTraits<T>::CppType value) {
        AggregateFunctionBaseData<T>::res_bit ^= value;
    }

    void merge(const AggregateFunctionGroupBitXorData& rhs) {
        AggregateFunctionBaseData<T>::res_bit ^= rhs.res_bit;
    }

    void reset() { AggregateFunctionBaseData<T>::res_bit = 0; }
};

/// Counts bitwise operation on numbers.
template <PrimitiveType T, typename Data>
class AggregateFunctionBitwise final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>> {
public:
    AggregateFunctionBitwise(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>(
                      argument_types_) {}

    String get_name() const override { return Data::name; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<typename PrimitiveTypeTraits<T>::DataType>();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto& column = assert_cast<const typename PrimitiveTypeTraits<T>::ColumnType&,
                                         TypeCheckOnRelease::DISABLE>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = assert_cast<typename PrimitiveTypeTraits<T>::ColumnType&>(to);
        column.get_data().push_back(this->data(place).get());
    }
};

} // namespace doris::vectorized
#include "common/compile_check_end.h"
