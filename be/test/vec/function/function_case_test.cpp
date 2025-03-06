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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>

#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "geo/geo_common.h"
#include "geo/geo_types.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(VFunctionCase, test_case) {
    std::string func_name = "case";
    {
        //  case
        //  when bool expr then expr
        //  when bool expr then expr
        InputTypeSet input_types = {TypeIndex::UInt8, TypeIndex::Int32, TypeIndex::UInt8,
                                    TypeIndex::Int32};

        DataSet data_set = {{{true, 2, true, 3}, 2}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}
} // namespace doris::vectorized
