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

suite("test_ddl") {
    sql "drop database if exists test_dictionary_ddl"
    sql "create database test_dictionary_ddl"
    sql "use test_dictionary_ddl"

    sql """
        create table dc(
            k0 datetime(6) null,
            k1 varchar
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """

    test { // wrong grammar. no using
        sql """
        create dictionary dic1
        (
            col1 KEY, 
            col2 VALUE,
            col3 VALUE
        )
        properties("x"="x", "y"="y");
        """
        exception "mismatched input"
    }

    test { // wrong grammar. no properties keyword
        sql """
        create dictionary dic1 using dc
        (
            col1 KEY, 
            col2 VALUE,
            col3 VALUE
        )
        ("x"="x", "y"="y");
        """
        exception "mismatched input"
    }

    test { // no source table
        sql """
        create dictionary dic1 using dcxxx
        (
            col1 KEY, 
            col2 VALUE,
            col3 VALUE
        )
        properties("x"="x", "y"="y");
        """
        exception "Unknown table"
    }

    test { // wrong column name
        sql """
        create dictionary dic1 using dc
        (
            col1 KEY, 
            col2 VALUE,
            col3 VALUE
        )
        properties("x"="x", "y"="y");
        """
        exception "Column col1 not found in source table dc"
    }

    test { // wrong column type
        sql """
        create dictionary dic1 using dc
        (
            k0 KEY, 
            k1 VALUE,
            k1 VARCHAR
        )
        properties("x"="x", "y"="y");
        """
        exception "mismatched input 'VARCHAR'"
    }

    sql """
        create dictionary dic1 using dc
        (
            k1 KEY, 
            k0 VALUE
        )
        properties("x"="x", "y"="y");
    """
    def origin_res = (sql "show dictionaries")[0]
    log.info(origin_res.toString())
    assertTrue(origin_res[1] == "dic1" && origin_res[2] == "NORMAL")

    test { // duplicate dictionary
        sql """
        create dictionary dic1 using dc
        (
            k1 KEY, 
            k0 VALUE
        )
        properties("x"="x", "y"="y");
        """
        exception "Dictionary dic1 already exists in database test"
    }

    // drop databases
    sql "use mysql"
    sql "drop database test_dictionary_ddl"
    sql "create database test_dictionary_ddl"
    sql "use test_dictionary_ddl"
    origin_res = sql "show dictionaries"
    log.info(origin_res.toString())
    assertTrue(origin_res.size() == 0) // should also be removed
}