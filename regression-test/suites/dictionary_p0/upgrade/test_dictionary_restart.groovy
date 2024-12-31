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

 suite('test_dictionary_restart', 'p0,restart_fe') {
    // 切换到目标数据库
    sql "use test_dictionary_upgrade"

    // 验证字典数量保持一致
    def dict_res = sql "show dictionaries"
    log.info("Dictionaries after restart: " + dict_res.toString())
    assertTrue(dict_res.size() == 2)

    // 验证字典名称和状态
    def remaining_dicts = dict_res.collect { it[1] }
    assertTrue(remaining_dicts.contains("user_dict"))
    assertTrue(remaining_dicts.contains("area_dict"))

    // 验证每个字典的状态是NORMAL
    dict_res.each { row ->
        assertTrue(row[2] == "NORMAL")
    }

    // 验证函数可用
    expalin {
        sql "select dict_get('test_dictionary_upgrade.area_dict', 'city', 'abc')"
        verbose true
        contains "varchar(64)"
        notContains "varchar(32)"
    }
}