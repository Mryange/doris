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

#include "core/types.h"
#include "exprs/function/cast/cast_parameters.h"
#include "util/io_helper.h"

namespace doris {

struct CastToBool {
    template <class SRC>
    static inline bool from_number(const SRC& from, UInt8& to, CastParameters& params);

    template <class SRC>
    static inline bool from_decimal(const SRC& from, UInt8& to, UInt32 precision, UInt32 scale,
                                    CastParameters& params);

    static inline bool from_string(const StringRef& from, UInt8& to, CastParameters& params);
};

template <>
inline bool CastToBool::from_number(const UInt8& from, UInt8& to, CastParameters&) {
    to = from;
    return true;
}

template <>
inline bool CastToBool::from_number(const Int8& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int16& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int32& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int64& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Int128& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}

template <>
inline bool CastToBool::from_number(const Float32& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}
template <>
inline bool CastToBool::from_number(const Float64& from, UInt8& to, CastParameters&) {
    to = (from != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal32& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal64& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const DecimalV2Value& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value() != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal128V3& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

template <>
inline bool CastToBool::from_decimal(const Decimal256& from, UInt8& to, UInt32, UInt32,
                                     CastParameters&) {
    to = (from.value != 0);
    return true;
}

inline bool CastToBool::from_string(const StringRef& from, UInt8& to, CastParameters&) {
    return try_read_bool_text(to, from);
}

} // namespace doris