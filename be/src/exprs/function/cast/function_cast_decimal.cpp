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

// This translation unit is the ONLY place that includes cast_to_decimal.h.
// All CastToImpl<CastMode, From, ToDecimalType> template instantiations are
// confined here, keeping them out of function_cast.cpp.

#include "core/data_type/data_type_decimal.h"
#include "exprs/function/cast/cast_to_decimal.h"

namespace doris {
#include "common/compile_check_begin.h"

// Casting from string to decimal types.
template <CastModeType Mode, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType>)
class CastToImpl<Mode, DataTypeString, ToDataType> : public CastToBase {
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());

        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();

        // by default framework, to_type is already unwrapped nullable
        MutableColumnPtr column_to = to_type->create_column();
        ColumnNullable::MutablePtr nullable_col_to = ColumnNullable::create(
                std::move(column_to), ColumnUInt8::create(input_rows_count, 0));

        if constexpr (Mode == CastModeType::NonStrictMode) {
            // may write nulls to nullable_col_to
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, *nullable_col_to, {}));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            // WON'T write nulls to nullable_col_to, just raise errors. null_map is only used to skip invalid rows
            RETURN_IF_ERROR(serde->from_string_strict_mode_batch(
                    *col_from, nullable_col_to->get_nested_column(), {}, null_map));
        } else {
            return Status::InternalError("Unsupported cast mode");
        }

        block.get_by_position(result).column = std::move(nullable_col_to);
        return Status::OK();
    }
};

// cast bool and int to decimal. when may overflow, result column is nullable.
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType> &&
             (IsDataTypeInt<FromDataType> || IsDataTypeBool<FromDataType>))
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
        constexpr UInt32 from_scale = 0;

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        // may overflow. nullable result column.
        bool narrow_integral = (to_max_int_digit_count < from_max_int_digit_count);
        // only in non-strict mode and may overflow, we set nullable
        bool set_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        constexpr UInt32 to_max_digits =
                NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }
        using MaxNativeType = std::conditional_t<(sizeof(FromFieldType) >
                                                  sizeof(typename ToFieldType::NativeType)),
                                                 FromFieldType, typename ToFieldType::NativeType>;
        MaxNativeType scale_multiplier =
                DataTypeDecimal<ToFieldType::PType>::get_scale_multiplier(to_scale);
        typename ToFieldType::NativeType max_result =
                DataTypeDecimal<ToFieldType::PType>::get_max_digits_number(to_precision);
        typename ToFieldType::NativeType min_result = -max_result;

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (narrow_integral) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            null_map_data = col_null_map_to->get_data().data();
        }

        auto col_to = ToDataType::ColumnType::create(input_rows_count, to_scale);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        size_t size = vec_from.size();

        RETURN_IF_ERROR(std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral) {
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::_from_int<typename FromDataType::FieldType,
                                                      typename ToDataType::FieldType,
                                                      multiply_may_overflow, narrow_integral>(
                                    vec_from_data[i], vec_to_data[i], to_precision, to_scale,
                                    scale_multiplier, min_result, max_result, params)) {
                            if (set_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                    return Status::OK();
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral)));

        if (narrow_integral) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

// cast float and double to decimal. ALWAYS nullable result column.
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType> && IsDataTypeFloat<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
        UInt32 from_scale = 0;

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral =
                (to_max_int_digit_count < from_max_int_digit_count) ||
                (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);
        // only in non-strict mode and may overflow, we set nullable
        bool set_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        ColumnUInt8::MutablePtr col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
        NullMap::value_type* null_map_data = col_null_map_to->get_data().data();

        auto col_to = ToDataType::ColumnType::create(input_rows_count, to_scale);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        size_t size = vec_from.size();

        typename ToFieldType::NativeType scale_multiplier =
                DataTypeDecimal<ToFieldType::PType>::get_scale_multiplier(to_scale);
        typename ToFieldType::NativeType max_result =
                DataTypeDecimal<ToFieldType::PType>::get_max_digits_number(to_precision);
        typename ToFieldType::NativeType min_result = -max_result;
        for (size_t i = 0; i < size; i++) {
            if (!CastToDecimal::_from_float<typename FromDataType::FieldType,
                                            typename ToDataType::FieldType>(
                        vec_from_data[i], vec_to_data[i], to_precision, to_scale, scale_multiplier,
                        min_result, max_result, params)) {
                if (set_nullable) {
                    null_map_data[i] = 1;
                } else {
                    return params.status;
                }
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        return Status::OK();
    }
};

// cast decimalv3 types to decimalv2 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV3<ToDataType> && IsDataTypeDecimalV2<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return Status::RuntimeError(
                "not support {} ",
                cast_mode_type_to_string(CastMode, block.get_by_position(arguments[0]).type,
                                         block.get_by_position(result).type));
    }
};

// cast decimalv2 types to decimalv3 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV2<FromDataType> && IsDataTypeDecimalV3<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
        UInt32 from_precision = from_decimal_type.get_precision();
        UInt32 from_scale = from_decimal_type.get_scale();
        UInt32 from_original_precision = from_decimal_type.get_original_precision();
        UInt32 from_original_scale = from_decimal_type.get_original_scale();

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        auto from_max_int_digit_count = from_original_precision - from_original_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral = (to_max_int_digit_count < from_max_int_digit_count) ||
                               (to_max_int_digit_count == from_max_int_digit_count &&
                                to_scale < from_original_scale);
        // only in non-strict mode and may overflow, we set nullable
        bool set_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        size_t size = col_from->size();
        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (narrow_integral) {
            col_null_map_to = ColumnUInt8::create(size, 0);
            null_map_data = col_null_map_to->get_data().data();
        }
        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        auto col_to = ToDataType::ColumnType::create(size, to_scale);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        using MaxFieldType =
                std::conditional_t<(sizeof(FromFieldType) == sizeof(ToFieldType)) &&
                                           (std::is_same_v<ToFieldType, Decimal128V3> ||
                                            std::is_same_v<FromFieldType, Decimal128V3>),
                                   Decimal128V3,
                                   std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                                      FromFieldType, ToFieldType>>;
        using MaxNativeType = typename MaxFieldType::NativeType;

        constexpr UInt32 to_max_digits =
                NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }

        typename ToFieldType::NativeType max_result =
                DataTypeDecimal<ToFieldType::PType>::get_max_digits_number(to_precision);
        typename ToFieldType::NativeType min_result = -max_result;

        MaxNativeType multiplier {};
        if (from_scale < to_scale) {
            multiplier = DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(to_scale -
                                                                                    from_scale);
        } else if (from_scale > to_scale) {
            multiplier = DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(from_scale -
                                                                                    to_scale);
        }
        RETURN_IF_ERROR(std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral) {
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::_from_decimal<FromFieldType, ToFieldType,
                                                          multiply_may_overflow, narrow_integral>(
                                    vec_from_data[i], from_precision, from_scale, vec_to_data[i],
                                    to_precision, to_scale, min_result, max_result, multiplier,
                                    params)) {
                            if (set_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                    return Status::OK();
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral)));
        if (narrow_integral) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

// cast between decimalv3 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV3<ToDataType> && IsDataTypeDecimalV3<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
        UInt32 from_precision = from_decimal_type.get_precision();
        UInt32 from_scale = from_decimal_type.get_scale();

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral =
                (to_max_int_digit_count < from_max_int_digit_count) ||
                (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);
        // only in non-strict mode and may overflow, we set nullable
        bool set_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        size_t size = col_from->size();
        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (narrow_integral) {
            col_null_map_to = ColumnUInt8::create(size, 0);
            null_map_data = col_null_map_to->get_data().data();
        }
        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        auto col_to = ToDataType::ColumnType::create(size, to_scale);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        using MaxFieldType =
                std::conditional_t<(sizeof(FromFieldType) == sizeof(ToFieldType)) &&
                                           (std::is_same_v<ToFieldType, Decimal128V3> ||
                                            std::is_same_v<FromFieldType, Decimal128V3>),
                                   Decimal128V3,
                                   std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                                      FromFieldType, ToFieldType>>;
        using MaxNativeType = typename MaxFieldType::NativeType;

        UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }

        typename ToFieldType::NativeType max_result =
                DataTypeDecimal<ToFieldType::PType>::get_max_digits_number(to_precision);
        typename ToFieldType::NativeType min_result = -max_result;

        MaxNativeType multiplier {};
        if (from_scale < to_scale) {
            multiplier = DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(to_scale -
                                                                                    from_scale);
        } else if (from_scale > to_scale) {
            multiplier = DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(from_scale -
                                                                                    to_scale);
        }
        RETURN_IF_ERROR(std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral) {
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::_from_decimal<FromFieldType, ToFieldType,
                                                          multiply_may_overflow, narrow_integral>(
                                    vec_from_data[i], from_precision, from_scale, vec_to_data[i],
                                    to_precision, to_scale, min_result, max_result, multiplier,
                                    params)) {
                            if (set_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                    return Status::OK();
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral)));
        if (narrow_integral) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

template <typename T>
constexpr static bool type_allow_cast_to_decimal =
        std::is_same_v<T, DataTypeString> || IsDataTypeNumber<T> || IsDataTypeDecimal<T>;

#include "common/compile_check_end.h"

namespace CastWrapper {

template <typename ToDataType>
WrapperType create_decimal_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_impl;

    auto make_cast_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (type_allow_cast_to_decimal<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, ToDataType>>();
            } else {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(), make_cast_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS decimal not supported {}", from_type->get_name()));
    }

    return [cast_impl](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       uint32_t result, size_t input_rows_count,
                       const NullMap::value_type* null_map = nullptr) {
        return cast_impl->execute_impl(context, block, arguments, result, input_rows_count,
                                       null_map);
    };
}

WrapperType create_decimal_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                   PrimitiveType to_type) {
    switch (to_type) {
    case TYPE_DECIMALV2:
        return create_decimal_wrapper<DataTypeDecimalV2>(context, from_type);
    case TYPE_DECIMAL32:
        return create_decimal_wrapper<DataTypeDecimal32>(context, from_type);
    case TYPE_DECIMAL64:
        return create_decimal_wrapper<DataTypeDecimal64>(context, from_type);
    case TYPE_DECIMAL128I:
        return create_decimal_wrapper<DataTypeDecimal128>(context, from_type);
    case TYPE_DECIMAL256:
        return create_decimal_wrapper<DataTypeDecimal256>(context, from_type);
    default:
        return create_unsupport_wrapper(
                fmt::format("CAST AS decimal: unsupported to_type {}", type_to_string(to_type)));
    }
}

} // namespace CastWrapper
} // namespace doris
