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

suite('load', 'p0,restart_fe') {
    sql "drop database if exists test_dictionary_upgrade"
    sql "create database test_dictionary_upgrade"
    sql "use test_dictionary_upgrade"

    // 创建用于IP_TRIE字典的基表
    sql """
        create table ip_base(
            ip varchar(32) null,
            region varchar(64) null,
            isp varchar(32) null
        )
        DISTRIBUTED BY HASH(`ip`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // 创建用于HASH_MAP字典的基表
    sql """
        create table user_base(
            user_id varchar(32) null,
            user_name varchar(64) null,
            age int null
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // 创建IP_TRIE类型的字典
    sql """
        create dictionary ip_dict using ip_base
        (
            ip KEY,
            region VALUE,
            isp VALUE
        )LAYOUT(IP_TRIE);
    """

    // 创建HASH_MAP类型的字典
    sql """
        create dictionary user_dict using user_base
        (
            user_id KEY,
            user_name VALUE,
            age VALUE
        )LAYOUT(HASH_MAP);
    """

    // 增加第三个基表(IP_TRIE)
    sql """
        create table area_base(
            area_ip varchar(32) null,
            city varchar(64) null,
            country varchar(32) null
        )
        DISTRIBUTED BY HASH(`area_ip`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // 增加第四个基表(HASH_MAP)
    sql """
        create table product_base(
            product_id varchar(32) null,
            product_name varchar(64) null,
            price decimal(10,2) null
        )
        DISTRIBUTED BY HASH(`product_id`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // 创建第三个字典(IP_TRIE)
    sql """
        create dictionary area_dict using area_base
        (
            area_ip KEY,
            city VALUE,
            country VALUE
        )LAYOUT(IP_TRIE);
    """

    // 创建第四个字典(HASH_MAP)
    sql """
        create dictionary product_dict using product_base
        (
            product_id KEY,
            product_name VALUE,
            price VALUE
        )LAYOUT(HASH_MAP);
    """

    // 验证现有字典数量
    def dict_res = sql "show dictionaries"
    log.info("After creating all dictionaries: " + dict_res.toString())
    assertTrue(dict_res.size() == 4)

    // 删除两个字典
    sql "drop dictionary ip_dict"
    sql "drop dictionary product_dict"

    // 验证删除后的字典数量
    dict_res = sql "show dictionaries"
    log.info("After dropping dictionaries: " + dict_res.toString())
    assertTrue(dict_res.size() == 2)

    // 验证剩余的字典名称
    def remaining_dicts = dict_res.collect { it[1] }
    assertTrue(remaining_dicts.contains("user_dict"))
    assertTrue(remaining_dicts.contains("area_dict"))
}