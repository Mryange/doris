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

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/primitive_type.h"
#include "core/value/ipv6_value.h"
#include "exprs/function/cast/cast_parameters.h"

namespace doris {
#include "common/compile_check_begin.h"

struct CastToIPv4 {
    static bool from_string(const StringRef& from, IPv4& to, CastParameters&);
};

inline bool CastToIPv4::from_string(const StringRef& from, IPv4& to, CastParameters&) {
    return IPv4Value::from_string(to, from.data, from.size);
}

struct CastToIPv6 {
    static bool from_string(const StringRef& from, IPv6& to, CastParameters&);
    static bool from_ipv4(const IPv4& from, IPv6& to, CastParameters&);
};

inline bool CastToIPv6::from_string(const StringRef& from, IPv6& to, CastParameters&) {
    return IPv6Value::from_string(to, from.data, from.size);
}

inline bool CastToIPv6::from_ipv4(const IPv4& from, IPv6& to, CastParameters&) {
    map_ipv4_to_ipv6(from, reinterpret_cast<UInt8*>(&to));
    return true;
}

#include "common/compile_check_end.h"
} // namespace doris