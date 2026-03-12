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

#include <cmath>
#include <type_traits>

#include "common/status.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/cast/cast_to_basic_number_common.h"
#include "util/io_helper.h"

namespace doris {
#include "common/compile_check_begin.h"

#define DECIMAL_CONVERT_OVERFLOW_ERROR(value, from_type_name, precision, scale)                    \
    Status(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,                                                  \
           fmt::format(                                                                            \
                   "Arithmetic overflow when converting value {} from type {} to decimal({}, {})", \
                   value, from_type_name, precision, scale))
struct CastToDecimal {
    template <typename ToCppT>
        requires(IsDecimalNumber<ToCppT>)
    static inline bool from_string(const StringRef& from, ToCppT& to, UInt32 precision,
                                   UInt32 scale, CastParameters& params) {
        if constexpr (IsDecimalV2<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMALV2>(to, from, precision, scale);
        }

        if constexpr (IsDecimal32<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL32>(to, from, precision, scale);
        }

        if constexpr (IsDecimal64<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL64>(to, from, precision, scale);
        }

        if constexpr (IsDecimal128V3<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL128I>(to, from, precision, scale);
        }

        if constexpr (IsDecimal256<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL256>(to, from, precision, scale);
        }
    }

    // cast int to decimal
    template <typename FromCppT, typename ToCppT,
              typename MaxNativeType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalNumber<ToCppT> &&
                 (IsCppTypeInt<FromCppT> || std::is_same_v<FromCppT, UInt8>))
    static inline bool from_int(const FromCppT& from, ToCppT& to, UInt32 to_precision,
                                UInt32 to_scale, CastParameters& params) {
        MaxNativeType scale_multiplier =
                DataTypeDecimal<ToCppT::PType>::get_scale_multiplier(to_scale);
        typename ToCppT::NativeType max_result =
                DataTypeDecimal<ToCppT::PType>::get_max_digits_number(to_precision);
        typename ToCppT::NativeType min_result = -max_result;

        UInt32 from_precision = NumberTraits::max_ascii_len<FromCppT>();
        constexpr UInt32 from_scale = 0;
        constexpr UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToCppT::NativeType>();

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral = (to_max_int_digit_count < from_max_int_digit_count);
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }
        return std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral) {
                    return _from_int<FromCppT, ToCppT, multiply_may_overflow, narrow_integral>(
                            from, to, to_precision, to_scale, scale_multiplier, min_result,
                            max_result, params);
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral));
    }

    // cast bool to decimal
    template <typename FromCppT, typename ToCppT,
              typename MaxNativeType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalNumber<ToCppT> && std::is_same_v<FromCppT, UInt8>)
    static inline bool from_bool(const FromCppT& from, ToCppT& to, UInt32 to_precision,
                                 UInt32 to_scale, CastParameters& params) {
        return from_int<FromCppT, ToCppT, MaxNativeType>(from, to, to_precision, to_scale, params);
    }

    template <typename FromCppT, typename ToCppT>
        requires(IsDecimalNumber<ToCppT> && IsCppTypeFloat<FromCppT>)
    static inline bool from_float(const FromCppT& from, ToCppT& to, UInt32 to_precision,
                                  UInt32 to_scale, CastParameters& params) {
        typename ToCppT::NativeType scale_multiplier =
                DataTypeDecimal<ToCppT::PType>::get_scale_multiplier(to_scale);
        typename ToCppT::NativeType max_result =
                DataTypeDecimal<ToCppT::PType>::get_max_digits_number(to_precision);
        typename ToCppT::NativeType min_result = -max_result;

        return _from_float<FromCppT, ToCppT>(from, to, to_precision, to_scale, scale_multiplier,
                                             min_result, max_result, params);
    }

    template <typename FromCppT, typename ToCppT>
        requires(IsDecimalNumber<ToCppT> && IsCppTypeFloat<FromCppT> && !IsDecimal128V2<ToCppT>)
    static inline bool _from_float(const FromCppT& from, ToCppT& to, UInt32 to_precision,
                                   UInt32 to_scale,
                                   const typename ToCppT::NativeType& scale_multiplier,
                                   const typename ToCppT::NativeType& min_result,
                                   const typename ToCppT::NativeType& max_result,
                                   CastParameters& params) {
        if (!std::isfinite(from)) {
            params.status = Status(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                   "Decimal convert overflow. Cannot convert infinity or NaN "
                                   "to decimal");
            return false;
        }
        // For decimal256, we need to use long double to avoid overflow when
        // static casting the multiplier to floating type, and also to be as precise as possible;
        // For other decimal types, we use double to be as precise as possible.
        using DoubleType = std::conditional_t<IsDecimal256<ToCppT>, long double, double>;
        DoubleType tmp = from * static_cast<DoubleType>(scale_multiplier);
        if (tmp <= DoubleType(min_result) || tmp >= DoubleType(max_result)) {
            if (params.is_strict) {
                params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(from, "float/double", to_precision,
                                                               to_scale);
            }
            return false;
        }
        to.value = static_cast<typename ToCppT::NativeType>(static_cast<double>(
                from * static_cast<DoubleType>(scale_multiplier) + ((from >= 0) ? 0.5 : -0.5)));
        return true;
    }
    template <typename FromCppT, typename ToCppT>
        requires(IsDecimal128V2<ToCppT> && IsCppTypeFloat<FromCppT>)
    static inline bool _from_float(const FromCppT& from, ToCppT& to, UInt32 to_precision,
                                   UInt32 to_scale,
                                   const typename ToCppT::NativeType& scale_multiplier,
                                   const typename ToCppT::NativeType& min_result,
                                   const typename ToCppT::NativeType& max_result,
                                   CastParameters& params) {
        if (!std::isfinite(from)) {
            params.status = Status(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                   "Decimal convert overflow. Cannot convert infinity or NaN "
                                   "to decimal");
            return false;
        }
        // For decimal256, we need to use long double to avoid overflow when
        // static casting the multiplier to floating type, and also to be as precise as possible;
        // For other decimal types, we use double to be as precise as possible.
        using DoubleType = std::conditional_t<IsDecimal256<ToCppT>, long double, double>;
        DoubleType tmp = from * static_cast<DoubleType>(scale_multiplier);
        if (tmp <= DoubleType(min_result) || tmp >= DoubleType(max_result)) {
            if (params.is_strict) {
                params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(from, "float/double", to_precision,
                                                               to_scale);
            }
            return false;
        }
        to = DecimalV2Value(static_cast<typename ToCppT::NativeType>(static_cast<double>(
                from * static_cast<DoubleType>(scale_multiplier) + ((from >= 0) ? 0.5 : -0.5))));
        return true;
    }

    template <typename FromCppT, typename ToCppT,
              typename MaxFieldType = std::conditional_t<
                      (sizeof(FromCppT) == sizeof(ToCppT)) &&
                              (std::is_same_v<ToCppT, Decimal128V3> ||
                               std::is_same_v<FromCppT, Decimal128V3>),
                      Decimal128V3,
                      std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)), FromCppT, ToCppT>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimalv2(const FromCppT& from, const UInt32 from_precision,
                                      const UInt32 from_scale, UInt32 from_original_precision,
                                      UInt32 from_original_scale, ToCppT& to, UInt32 to_precision,
                                      UInt32 to_scale, CastParameters& params) {
        using MaxNativeType = typename MaxFieldType::NativeType;

        auto from_max_int_digit_count = from_original_precision - from_original_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral = (to_max_int_digit_count < from_max_int_digit_count) ||
                               (to_max_int_digit_count == from_max_int_digit_count &&
                                to_scale < from_original_scale);

        constexpr UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToCppT::NativeType>();
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }

        typename ToCppT::NativeType max_result =
                DataTypeDecimal<ToCppT::PType>::get_max_digits_number(to_precision);
        typename ToCppT::NativeType min_result = -max_result;

        return std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral) {
                    if (from_scale < to_scale) {
                        MaxNativeType multiplier =
                                DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(
                                        to_scale - from_scale);
                        return _from_decimal_smaller_scale<FromCppT, ToCppT, multiply_may_overflow,
                                                           narrow_integral>(
                                from, from_precision, from_scale, to, to_precision, to_scale,
                                multiplier, min_result, max_result, params);
                    } else if (from_scale == to_scale) {
                        return _from_decimal_same_scale<FromCppT, ToCppT, MaxNativeType,
                                                        narrow_integral>(
                                from, from_precision, from_scale, to, to_precision, to_scale,
                                min_result, max_result, params);
                    } else {
                        MaxNativeType multiplier =
                                DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(
                                        from_scale - to_scale);
                        return _from_decimal_bigger_scale<FromCppT, ToCppT, multiply_may_overflow,
                                                          narrow_integral>(
                                from, from_precision, from_scale, to, to_precision, to_scale,
                                multiplier, min_result, max_result, params);
                    }
                    return true;
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral));
    }

    template <typename FromCppT, typename ToCppT,
              typename MaxFieldType = std::conditional_t<
                      (sizeof(FromCppT) == sizeof(ToCppT)) &&
                              (std::is_same_v<ToCppT, Decimal128V3> ||
                               std::is_same_v<FromCppT, Decimal128V3>),
                      Decimal128V3,
                      std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)), FromCppT, ToCppT>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimalv3(const FromCppT& from, const UInt32 from_precision,
                                      const UInt32 from_scale, ToCppT& to, UInt32 to_precision,
                                      UInt32 to_scale, CastParameters& params) {
        using MaxNativeType = typename MaxFieldType::NativeType;

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral =
                (to_max_int_digit_count < from_max_int_digit_count) ||
                (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);

        UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToCppT::NativeType>();
        bool multiply_may_overflow = false;
        if (to_scale > from_scale) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }

        typename ToCppT::NativeType max_result =
                DataTypeDecimal<ToCppT::PType>::get_max_digits_number(to_precision);
        typename ToCppT::NativeType min_result = -max_result;

        MaxNativeType multiplier {};
        if (from_scale < to_scale) {
            multiplier = DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(to_scale -
                                                                                    from_scale);
        } else if (from_scale > to_scale) {
            multiplier = DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(from_scale -
                                                                                    to_scale);
        }

        return std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral) {
                    return _from_decimal<FromCppT, ToCppT, multiply_may_overflow, narrow_integral>(
                            from, from_precision, from_scale, to, to_precision, to_scale,
                            min_result, max_result, multiplier, params);
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral));
    }

    template <typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
              typename MaxFieldType = std::conditional_t<
                      (sizeof(FromCppT) == sizeof(ToCppT)) &&
                              (std::is_same_v<ToCppT, Decimal128V3> ||
                               std::is_same_v<FromCppT, Decimal128V3>),
                      Decimal128V3,
                      std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)), FromCppT, ToCppT>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool _from_decimal(const FromCppT& from, const UInt32 from_precision,
                                     const UInt32 from_scale, ToCppT& to, UInt32 to_precision,
                                     UInt32 to_scale, const ToCppT::NativeType& min_result,
                                     const ToCppT::NativeType& max_result,
                                     const typename MaxFieldType::NativeType& scale_multiplier,
                                     CastParameters& params) {
        using MaxNativeType = typename MaxFieldType::NativeType;

        if (from_scale < to_scale) {
            return _from_decimal_smaller_scale<FromCppT, ToCppT, multiply_may_overflow,
                                               narrow_integral>(
                    from, from_precision, from_scale, to, to_precision, to_scale, scale_multiplier,
                    min_result, max_result, params);
        } else if (from_scale == to_scale) {
            return _from_decimal_same_scale<FromCppT, ToCppT, MaxNativeType, narrow_integral>(
                    from, from_precision, from_scale, to, to_precision, to_scale, min_result,
                    max_result, params);
        } else {
            return _from_decimal_bigger_scale<FromCppT, ToCppT, multiply_may_overflow,
                                              narrow_integral>(
                    from, from_precision, from_scale, to, to_precision, to_scale, scale_multiplier,
                    min_result, max_result, params);
        }
        return true;
    }

    template <
            typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
            typename MaxNativeType = std::conditional_t<
                    (sizeof(FromCppT) == sizeof(ToCppT)) &&
                            (std::is_same_v<ToCppT, Decimal128V3> ||
                             std::is_same_v<FromCppT, Decimal128V3>),
                    Decimal128V3::NativeType,
                    std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)),
                                       typename FromCppT::NativeType, typename ToCppT::NativeType>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool _from_decimal_smaller_scale(
            const FromCppT& from, const UInt32 precision_from, const UInt32 scale_from, ToCppT& to,
            UInt32 precision_to, UInt32 scale_to, const MaxNativeType& scale_multiplier,
            const typename ToCppT::NativeType& min_result,
            const typename ToCppT::NativeType& max_result, CastParameters& params) {
        MaxNativeType res;
        if constexpr (multiply_may_overflow) {
            if constexpr (IsDecimal128V2<FromCppT>) {
                if (common::mul_overflow(static_cast<MaxNativeType>(from.value()), scale_multiplier,
                                         res)) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                decimal_to_string(from.value(), scale_from),
                                fmt::format("decimal({}, {})", precision_from, scale_from),
                                precision_to, scale_to);
                    }
                    return false;
                } else {
                    if (UNLIKELY(res > max_result || res < -max_result)) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value(), scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                        return false;
                    } else {
                        to = ToCppT(res);
                    }
                }
            } else {
                if (common::mul_overflow(static_cast<MaxNativeType>(from.value), scale_multiplier,
                                         res)) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                decimal_to_string(from.value, scale_from),
                                fmt::format("decimal({}, {})", precision_from, scale_from),
                                precision_to, scale_to);
                    }
                    return false;
                } else {
                    if (UNLIKELY(res > max_result || res < -max_result)) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value, scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                        return false;
                    } else {
                        to = ToCppT(res);
                    }
                }
            }
        } else {
            if constexpr (IsDecimal128V2<FromCppT>) {
                res = from.value() * scale_multiplier;
            } else {
                res = from.value * scale_multiplier;
            }
            if constexpr (narrow_integral) {
                if (UNLIKELY(res > max_result || res < -max_result)) {
                    if constexpr (IsDecimal128V2<FromCppT>) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value(), scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                    } else {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value, scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                    }
                    return false;
                }
            }
            to = ToCppT(res);
        }
        return true;
    }

    template <typename FromCppT, typename ToCppT, typename ScaleT, bool narrow_integral>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool _from_decimal_same_scale(const FromCppT& from, const UInt32 precision_from,
                                                const UInt32 scale_from, ToCppT& to,
                                                UInt32 precision_to, UInt32 scale_to,
                                                const typename ToCppT::NativeType& min_result,
                                                const typename ToCppT::NativeType& max_result,
                                                CastParameters& params) {
        if constexpr (IsDecimal128V2<FromCppT>) {
            if constexpr (narrow_integral) {
                if (UNLIKELY(from.value() > max_result || from.value() < -max_result)) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                decimal_to_string(from.value(), scale_from),
                                fmt::format("decimal({}, {})", precision_from, scale_from),
                                precision_to, scale_to);
                    }
                    return false;
                }
            }
            to = ToCppT(from.value());
        } else {
            if constexpr (narrow_integral) {
                if (UNLIKELY(from.value > max_result || from.value < -max_result)) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                decimal_to_string(from.value, scale_from),
                                fmt::format("decimal({}, {})", precision_from, scale_from),
                                precision_to, scale_to);
                    }
                    return false;
                }
            }
            to = ToCppT(from.value);
        }
        return true;
    }

    template <
            typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
            typename MaxNativeType = std::conditional_t<
                    (sizeof(FromCppT) == sizeof(ToCppT)) &&
                            (std::is_same_v<ToCppT, Decimal128V3> ||
                             std::is_same_v<FromCppT, Decimal128V3>),
                    Decimal128V3::NativeType,
                    std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)),
                                       typename FromCppT::NativeType, typename ToCppT::NativeType>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool _from_decimal_bigger_scale(const FromCppT& from, const UInt32 precision_from,
                                                  const UInt32 scale_from, ToCppT& to,
                                                  UInt32 precision_to, UInt32 scale_to,
                                                  const MaxNativeType& scale_multiplier,
                                                  const typename ToCppT::NativeType& min_result,
                                                  const typename ToCppT::NativeType& max_result,
                                                  CastParameters& params) {
        MaxNativeType res;
        if (from >= FromCppT(0)) {
            if constexpr (narrow_integral) {
                if constexpr (IsDecimal128V2<FromCppT>) {
                    res = (from.value() + scale_multiplier / 2) / scale_multiplier;
                    if (UNLIKELY(res > max_result)) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value(), scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                        return false;
                    }
                } else {
                    res = (from.value + scale_multiplier / 2) / scale_multiplier;
                    if (UNLIKELY(res > max_result)) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value, scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                        return false;
                    }
                }
                to = ToCppT(res);
            } else {
                if constexpr (IsDecimal128V2<FromCppT>) {
                    to = ToCppT((from.value() + scale_multiplier / 2) / scale_multiplier);
                } else {
                    to = ToCppT((from.value + scale_multiplier / 2) / scale_multiplier);
                }
            }
        } else {
            if constexpr (narrow_integral) {
                if constexpr (IsDecimal128V2<FromCppT>) {
                    res = (from.value() - scale_multiplier / 2) / scale_multiplier;
                    if (UNLIKELY(res < -max_result)) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value(), scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                        return false;
                    }
                } else {
                    res = (from.value - scale_multiplier / 2) / scale_multiplier;
                    if (UNLIKELY(res < -max_result)) {
                        if (params.is_strict) {
                            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                    decimal_to_string(from.value, scale_from),
                                    fmt::format("decimal({}, {})", precision_from, scale_from),
                                    precision_to, scale_to);
                        }
                        return false;
                    }
                }
                to = ToCppT(res);
            } else {
                if constexpr (IsDecimal128V2<FromCppT>) {
                    to = ToCppT((from.value() - scale_multiplier / 2) / scale_multiplier);
                } else {
                    to = ToCppT((from.value - scale_multiplier / 2) / scale_multiplier);
                }
            }
        }
        return true;
    }

    template <typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
              typename MaxNativeType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalNumber<ToCppT> && !IsDecimal128V2<ToCppT> &&
                 (IsCppTypeInt<FromCppT> || std::is_same_v<FromCppT, UInt8>))
    static inline bool _from_int(const FromCppT& from, ToCppT& to, UInt32 precision, UInt32 scale,
                                 const MaxNativeType& scale_multiplier,
                                 const typename ToCppT::NativeType& min_result,
                                 const typename ToCppT::NativeType& max_result,
                                 CastParameters& params) {
        MaxNativeType tmp;
        if constexpr (multiply_may_overflow) {
            if (common::mul_overflow(static_cast<MaxNativeType>(from), scale_multiplier, tmp)) {
                if (params.is_strict) {
                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(from, int_type_name<FromCppT>,
                                                                   precision, scale);
                }
                return false;
            }
            if constexpr (narrow_integral) {
                if (tmp < min_result || tmp > max_result) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                from, int_type_name<FromCppT>, precision, scale);
                    }
                    return false;
                }
            }
            to.value = static_cast<typename ToCppT::NativeType>(tmp);
        } else {
            tmp = scale_multiplier * from;
            if constexpr (narrow_integral) {
                if (tmp < min_result || tmp > max_result) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                from, int_type_name<FromCppT>, precision, scale);
                    }
                    return false;
                }
            }
            to.value = static_cast<typename ToCppT::NativeType>(tmp);
        }

        return true;
    }

    template <typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
              typename MaxNativeType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalV2<ToCppT> && (IsCppTypeInt<FromCppT> || std::is_same_v<FromCppT, UInt8>))
    static inline bool _from_int(const FromCppT& from, ToCppT& to, UInt32 precision, UInt32 scale,
                                 const MaxNativeType& scale_multiplier,
                                 const typename ToCppT::NativeType& min_result,
                                 const typename ToCppT::NativeType& max_result,
                                 CastParameters& params) {
        MaxNativeType tmp;
        if constexpr (multiply_may_overflow) {
            if (common::mul_overflow(static_cast<MaxNativeType>(from), scale_multiplier, tmp)) {
                if (params.is_strict) {
                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(from, int_type_name<FromCppT>,
                                                                   precision, scale);
                }
                return false;
            }
            if constexpr (narrow_integral) {
                if (tmp < min_result || tmp > max_result) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                from, int_type_name<FromCppT>, precision, scale);
                    }
                    return false;
                }
            }
            to = DecimalV2Value(static_cast<typename ToCppT::NativeType>(tmp));
        } else {
            tmp = scale_multiplier * from;
            if constexpr (narrow_integral) {
                if (tmp < min_result || tmp > max_result) {
                    if (params.is_strict) {
                        params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                from, int_type_name<FromCppT>, precision, scale);
                    }
                    return false;
                }
            }
            to = DecimalV2Value(static_cast<typename ToCppT::NativeType>(tmp));
        }

        return true;
    }
};

#include "common/compile_check_end.h"
} // namespace doris