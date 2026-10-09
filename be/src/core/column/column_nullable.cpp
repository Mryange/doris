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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnNullable.cpp
// and modified by Doris

#include "core/column/column_nullable.h"

#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type.h"
#include "exec/common/sip_hash.h"
#include "exec/sort/sort_block.h"

namespace doris {
namespace {

const ColumnUInt8& check_nullable_null_map_column(const IColumn& null_map) {
    const auto* concrete = check_and_get_column<ColumnUInt8>(null_map);
    if (!concrete) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "ColumnNullable null map must be ColumnUInt8, but got {}",
                               null_map.get_name());
        __builtin_unreachable();
    }
    return *concrete;
}

ColumnUInt8::Ptr check_nullable_null_map_column_ptr(const ColumnPtr& null_map) {
    if (!null_map) {
        return {};
    }
    return ColumnUInt8::cast_to_column_ptr(&check_nullable_null_map_column(*null_map));
}

void check_nullable_sizes(const IColumn& nested_column, const ColumnUInt8* null_map) {
    if (null_map && nested_column.size() != null_map->size()) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "Size of nested column {} with size {} is not equal to size of null map {}",
                nested_column.get_name(), nested_column.size(), null_map->size());
    }
}

} // namespace

namespace {
ColumnUInt8::MutablePtr assert_mutable_null_map(MutableColumnPtr&& null_map) {
    if (!null_map) {
        return {};
    }
    if (is_column_const(*null_map)) [[unlikely]] {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "ColumnNullable cannot have constant null map");
    }
    auto mutable_null_map = ColumnUInt8::cast_to_column_mutptr(
            assert_cast<ColumnUInt8*, TypeCheckOnRelease::DISABLE>(null_map.get()));
    null_map = nullptr;
    return mutable_null_map;
}
} // namespace

ColumnNullable::ColumnNullable(MutableColumnPtr&& nested_column_)
        : ColumnNullable(std::move(nested_column_), ColumnUInt8::MutablePtr {}) {}

ColumnNullable::ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_)
        : ColumnNullable(std::move(nested_column_), assert_mutable_null_map(std::move(null_map_))) {
}

ColumnNullable::ColumnNullable(MutableColumnPtr&& nested_column_,
                               ColumnUInt8::MutablePtr&& null_map_)
        : _nested_column(std::move(nested_column_)), _null_map(std::move(null_map_)) {
    check_const_only_in_top_level();
    check_consistency();
    // after convert const column to full column, it may be a nullable column
    if (_nested_column->is_nullable()) {
        auto& nested_nullable = assert_cast<ColumnNullable&>(*_nested_column);
        if (_null_map) {
            nested_nullable.apply_null_map(*_null_map);
        }
        _null_map = nested_nullable._null_map;
        _nested_column = nested_nullable.get_nested_column_ptr();
    }
    check_consistency();
}

ColumnNullable::ColumnNullable(SharedTag, ColumnPtr nested_column_, ColumnPtr null_map_) {
    auto outer_null_map = check_nullable_null_map_column_ptr(null_map_);
    check_nullable_sizes(*nested_column_, outer_null_map.get());

    if (const auto* nullable_nested = check_and_get_column<ColumnNullable>(nested_column_.get())) {
        nullable_nested->check_consistency();
        const auto* nested_null_map = nullable_nested->null_map_data_if_materialized();
        if (outer_null_map && nested_null_map) {
            auto merged =
                    assert_mutable_null_map(outer_null_map->clone_resized(outer_null_map->size()));
            auto& merged_data = merged->get_data();
            for (size_t i = 0; i != merged_data.size(); ++i) {
                merged_data[i] |= (*nested_null_map)[i];
            }
            static_cast<ColumnUInt8::Ptr&>(_null_map) = std::move(merged);
        } else {
            static_cast<ColumnUInt8::Ptr&>(_null_map) =
                    outer_null_map ? std::move(outer_null_map)
                                   : nullable_nested->get_null_map_column_ptr();
        }

        static_cast<IColumn::Ptr&>(_nested_column) = nullable_nested->get_nested_column_ptr();
    } else {
        static_cast<IColumn::Ptr&>(_nested_column) = std::move(nested_column_);
        static_cast<ColumnUInt8::Ptr&>(_null_map) = std::move(outer_null_map);
    }

    check_const_only_in_top_level();
    check_consistency();
}

void ColumnNullable::replace_columns(ColumnPtr nested_column, ColumnPtr null_map) {
    auto typed_null_map = check_nullable_null_map_column_ptr(null_map);
    check_nullable_sizes(*nested_column, typed_null_map.get());
    static_cast<IColumn::Ptr&>(_nested_column) = std::move(nested_column);
    static_cast<ColumnUInt8::Ptr&>(_null_map) = std::move(typed_null_map);
    check_const_only_in_top_level();
}

ColumnUInt8& ColumnNullable::materialize_null_map_for_write() {
    check_consistency();
    if (!_null_map) {
        _null_map = ColumnUInt8::create(size(), 0);
    }
    return *_null_map;
}

void ColumnNullable::check_append_after_nested(size_t old_rows, size_t appended_rows) const {
    DORIS_CHECK_LE(old_rows, size());
    DORIS_CHECK_EQ(size() - old_rows, appended_rows);
    if (_null_map) {
        DORIS_CHECK_EQ(_null_map->size(), old_rows);
    }
}

void ColumnNullable::append_non_null_after_nested(size_t old_rows, size_t appended_rows) {
    append_repeated_null_flag_after_nested(old_rows, 0, appended_rows);
}

void ColumnNullable::append_repeated_null_flag_after_nested(size_t old_rows, UInt8 flag,
                                                            size_t appended_rows) {
    check_append_after_nested(old_rows, appended_rows);
    if (appended_rows == 0) {
        return;
    }
    if (!_null_map) {
        if (flag == 0) {
            return;
        }
        _null_map = ColumnUInt8::create(old_rows, 0);
    }
    _null_map->insert_many_vals(flag, appended_rows);
}

void ColumnNullable::append_null_flags_after_nested(size_t old_rows, const UInt8* flags,
                                                    size_t appended_rows) {
    check_append_after_nested(old_rows, appended_rows);
    if (appended_rows == 0) {
        return;
    }
    if (!flags) {
        append_non_null_after_nested(old_rows, appended_rows);
        return;
    }
    if (!_null_map) {
        if (!simd::contain_one(flags, appended_rows)) {
            return;
        }
        _null_map = ColumnUInt8::create(old_rows, 0);
    }
    auto& data = _null_map->get_data();
    data.resize(size());
    memcpy(data.data() + old_rows, flags, appended_rows);
}

void ColumnNullable::append_null_map_range_after_nested(size_t old_rows, const ColumnNullable& src,
                                                        size_t start, size_t length) {
    check_append_after_nested(old_rows, length);
    DCHECK_LE(start, src.size());
    DCHECK_LE(length, src.size() - start);
    if (length == 0) {
        return;
    }
    if (!src._null_map) {
        append_non_null_after_nested(old_rows, length);
        return;
    }
    if (!_null_map) {
        if (!src.has_null(start, start + length)) {
            return;
        }
        _null_map = ColumnUInt8::create(old_rows, 0);
    }
    _null_map->insert_range_from(*src._null_map, start, length);
}

void ColumnNullable::set_null_at(size_t row) {
    DORIS_CHECK_LT(row, size());
    materialize_null_map_for_write().get_data()[row] = 1;
}

void ColumnNullable::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                              const uint8_t* __restrict null_data) const {
    if (!has_null(start, end)) {
        _nested_column->update_xxHash_with_value(start, end, hash, nullptr);
    } else {
        const auto* __restrict real_null_data = get_null_map_column().get_data().data();
        for (size_t i = start; i < end; ++i) {
            if (real_null_data[i] != 0) {
                hash = HashUtil::xxHash64NullWithSeed(hash);
            }
        }
        _nested_column->update_xxHash_with_value(start, end, hash, real_null_data);
    }
}

void ColumnNullable::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                           const uint8_t* __restrict null_data) const {
    if (!has_null(start, end)) {
        _nested_column->update_crc_with_value(start, end, hash, nullptr);
    } else {
        const auto* __restrict real_null_data = get_null_map_column().get_data().data();
        for (size_t i = start; i < end; ++i) {
            if (real_null_data[i] != 0) {
                hash = HashUtil::zlib_crc_hash_null(hash);
            }
        }
        _nested_column->update_crc_with_value(start, end, hash, real_null_data);
    }
}

void ColumnNullable::update_hash_with_value(size_t n, SipHash& hash) const {
    if (is_null_at(n)) {
        hash.update(0);
    } else {
        get_nested_column().update_hash_with_value(n, hash);
    }
}

void ColumnNullable::update_crcs_with_value(uint32_t* __restrict hashes, doris::PrimitiveType type,
                                            uint32_t rows, uint32_t offset,
                                            const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = rows;
    DCHECK(s == size());
    if (!has_null()) {
        _nested_column->update_crcs_with_value(hashes, type, rows, offset, nullptr);
    } else {
        const auto* __restrict real_null_data = _null_map->get_data().data();
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) {
                hashes[i] = HashUtil::zlib_crc_hash_null(hashes[i]);
            }
        }
        _nested_column->update_crcs_with_value(hashes, type, rows, offset, real_null_data);
    }
}

void ColumnNullable::update_crc32c_batch(uint32_t* __restrict hashes,
                                         const uint8_t* __restrict /* null_map */) const {
    if (!has_null()) {
        _nested_column->update_crc32c_batch(hashes, nullptr);
        return;
    }

    const auto* __restrict real_null_data = _null_map->get_data().data();
    if (_nested_column->support_replace_column_null_data()) {
        // Keep the old optimized hash semantics: NULL rows are hashed as the nested type's
        // default value. Do this in a read-only way; replacing NULL payloads in _nested_column
        // from this const hash path can race with other readers of the same column.
        _nested_column->update_crc32c_batch_default_on_null(hashes, real_null_data);
    } else {
        auto s = size();
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) {
                hashes[i] = HashUtil::crc32c_null(hashes[i]);
            }
        }
        _nested_column->update_crc32c_batch(hashes, real_null_data);
    }
}

void ColumnNullable::update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                                          const uint8_t* __restrict /* null_map */) const {
    if (!_null_map) {
        _nested_column->update_crc32c_single(start, end, hash, nullptr);
        return;
    }
    const auto* __restrict real_null_data = _null_map->get_data().data();
    constexpr int NULL_VALUE = 0;
    for (size_t i = start; i < end; ++i) {
        if (real_null_data[i] != 0) {
            hash = HashUtil::crc32c_fixed(NULL_VALUE, hash);
        }
    }
    _nested_column->update_crc32c_single(start, end, hash, real_null_data);
}

void ColumnNullable::update_hashes_with_value(uint64_t* __restrict hashes,
                                              const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = size();
    if (!has_null()) {
        _nested_column->update_hashes_with_value(hashes, nullptr);
    } else {
        const auto* __restrict real_null_data = _null_map->get_data().data();
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) {
                hashes[i] = HashUtil::xxHash64NullWithSeed(hashes[i]);
            }
        }
        _nested_column->update_hashes_with_value(hashes, real_null_data);
    }
}

MutableColumnPtr ColumnNullable::clone_resized(size_t new_size) const {
    MutableColumnPtr new_nested_col = get_nested_column().clone_resized(new_size);
    ColumnUInt8::MutablePtr new_null_map;

    if (new_size > 0 && (_null_map || new_size > size())) {
        new_null_map = ColumnUInt8::create(new_size, 0);

        size_t count = std::min(size(), new_size);
        if (_null_map && count != 0) {
            memcpy(new_null_map->get_data().data(), _null_map->get_data().data(), count);
        }

        /// If resizing to bigger one, set all new values to NULLs.
        if (new_size > count) {
            memset(&new_null_map->get_data()[count], 1, new_size - count);
        }
    }

    return ColumnNullable::create(std::move(new_nested_col), std::move(new_null_map));
}

Field ColumnNullable::operator[](size_t n) const {
    return is_null_at(n) ? Field::create_field<TYPE_NULL>(Null()) : get_nested_column()[n];
}

void ColumnNullable::get(size_t n, Field& res) const {
    if (is_null_at(n)) {
        res = Field();
    } else {
        get_nested_column().get(n, res);
    }
}

StringRef ColumnNullable::get_data_at(size_t n) const {
    if (is_null_at(n)) {
        return {(const char*)nullptr, 0};
    }
    return get_nested_column().get_data_at(n);
}

void ColumnNullable::insert_data(const char* pos, size_t length) {
    if (pos == nullptr) {
        insert_default();
    } else {
        const size_t old_rows = size();
        get_nested_column().insert_data(pos, length);
        append_non_null_after_nested(old_rows, 1);
    }
}

void ColumnNullable::insert_many_strings(const StringRef* strings, size_t num) {
    const size_t old_rows = size();
    // Decide whether a map is needed before appending either subcolumn.
    if (!_null_map) {
        for (size_t i = 0; i != num; ++i) {
            if (strings[i].data == nullptr) {
                materialize_null_map_for_write();
                break;
            }
        }
    }
    _nested_column->insert_many_strings(strings, num);
    check_append_after_nested(old_rows, num);
    if (!_null_map) {
        return;
    }
    auto& flags = _null_map->get_data();
    flags.resize(size());
    for (size_t i = 0; i != num; ++i) {
        flags[old_rows + i] = strings[i].data == nullptr;
    }
}

void ColumnNullable::insert_many_from(const IColumn& src, size_t position, size_t length) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(src);
    const size_t old_rows = size();
    const UInt8 flag = nullable_col._null_map ? nullable_col._null_map->get_data()[position] : 0;
    get_nested_column().insert_many_from(*nullable_col._nested_column, position, length);
    append_repeated_null_flag_after_nested(old_rows, flag, length);
}

StringRef ColumnNullable::serialize_value_into_arena(size_t n, Arena& arena,
                                                     char const*& begin) const {
    auto* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

const char* ColumnNullable::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

size_t ColumnNullable::deserialize_impl(const char* pos) {
    const size_t old_rows = size();
    size_t sz = 0;
    UInt8 val = *reinterpret_cast<const UInt8*>(pos);
    sz += sizeof(val);

    if (val == 0) {
        sz += get_nested_column().deserialize_impl(pos + sz);
    } else {
        get_nested_column().insert_default();
    }
    append_repeated_null_flag_after_nested(old_rows, val, 1);
    return sz;
}

size_t ColumnNullable::get_max_row_byte_size() const {
    constexpr auto flag_size = sizeof(NullMap::value_type);
    return flag_size + get_nested_column().get_max_row_byte_size();
}

size_t ColumnNullable::serialize_impl(char* pos, const size_t row) const {
    const UInt8 flag = _null_map ? _null_map->get_data()[row] : 0;
    memcpy_fixed<NullMap::value_type>(pos, reinterpret_cast<const char*>(&flag));
    if (flag) {
        return sizeof(NullMap::value_type);
    }
    return sizeof(NullMap::value_type) +
           get_nested_column().serialize_impl(pos + sizeof(NullMap::value_type), row);
}

void ColumnNullable::serialize(StringRef* keys, size_t num_rows) const {
    const bool has_null = this->has_null(0, num_rows);
    const auto* __restrict null_map = _null_map ? _null_map->get_data().data() : nullptr;
    _nested_column->serialize_with_nullable(keys, num_rows, has_null, null_map);
}

void ColumnNullable::deserialize(StringRef* keys, const size_t num_rows) {
    const size_t old_rows = size();
    if (_null_map) {
        auto& flags = _null_map->get_data();
        flags.reserve(old_rows + num_rows);
        _nested_column->deserialize_with_nullable(keys, num_rows, flags);
        check_consistency();
    } else {
        NullMap flags;
        flags.reserve(num_rows);
        _nested_column->deserialize_with_nullable(keys, num_rows, flags);
        DORIS_CHECK_EQ(flags.size(), num_rows);
        append_null_flags_after_nested(old_rows, flags.data(), num_rows);
    }
}

void ColumnNullable::insert_range_from_ignore_overflow(const doris::IColumn& src, size_t start,
                                                       size_t length) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(src);
    const size_t old_rows = size();
    get_nested_column().insert_range_from_ignore_overflow(*nullable_col._nested_column, start,
                                                          length);
    append_null_map_range_after_nested(old_rows, nullable_col, start, length);
}

void ColumnNullable::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(src);
    const size_t old_rows = size();
    get_nested_column().insert_range_from(*nullable_col._nested_column, start, length);
    append_null_map_range_after_nested(old_rows, nullable_col, start, length);
}

void ColumnNullable::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                         const uint32_t* indices_end) {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    const size_t old_rows = size();
    const size_t count = indices_end - indices_begin;
    materialize_null_map_for_selected_rows(src_concrete, indices_begin, count);
    get_nested_column().insert_indices_from(src_concrete.get_nested_column(), indices_begin,
                                            indices_end);
    check_append_after_nested(old_rows, count);
    if (_null_map && src_concrete._null_map) {
        _null_map->insert_indices_from(*src_concrete._null_map, indices_begin, indices_end);
    } else {
        append_non_null_after_nested(old_rows, count);
    }
}

void ColumnNullable::insert_indices_from_not_has_null(const IColumn& src,
                                                      const uint32_t* indices_begin,
                                                      const uint32_t* indices_end) {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    const size_t old_rows = size();
    get_nested_column().insert_indices_from(src_concrete.get_nested_column(), indices_begin,
                                            indices_end);
    append_non_null_after_nested(old_rows, indices_end - indices_begin);
}

void ColumnNullable::insert(const Field& x) {
    if (x.is_null()) {
        insert_default();
    } else {
        const size_t old_rows = size();
        get_nested_column().insert(x);
        append_non_null_after_nested(old_rows, 1);
    }
}

void ColumnNullable::insert_duplicate_fields(const Field& x, const size_t n) {
    if (x.is_null()) {
        insert_many_defaults(n);
    } else {
        const size_t old_rows = size();
        get_nested_column().insert_duplicate_fields(x, n);
        append_non_null_after_nested(old_rows, n);
    }
}

void ColumnNullable::insert_from(const IColumn& src, size_t n) {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    const size_t old_rows = size();
    const UInt8 flag = src_concrete._null_map ? src_concrete._null_map->get_data()[n] : 0;
    get_nested_column().insert_from(src_concrete.get_nested_column(), n);
    append_repeated_null_flag_after_nested(old_rows, flag, 1);
}

void ColumnNullable::append_data_by_selector(IColumn::MutablePtr& res,
                                             const IColumn::Selector& selector) const {
    append_data_by_selector(res, selector, 0, selector.size());
}

void ColumnNullable::append_data_by_selector(IColumn::MutablePtr& res,
                                             const IColumn::Selector& selector, size_t begin,
                                             size_t end) const {
    auto& res_column = assert_cast<ColumnNullable&>(*res);
    const size_t old_rows = res_column.size();
    res_column.materialize_null_map_for_selected_rows(*this, selector.data() + begin, end - begin);
    MutableColumnPtr res_nested_column = res_column.get_nested_column_ptr();
    get_nested_column().append_data_by_selector(res_nested_column, selector, begin, end);
    res_column.check_append_after_nested(old_rows, end - begin);
    if (_null_map && res_column._null_map) {
        IColumn::MutablePtr res_null_map_column = res_column.get_null_map_column_ptr();
        _null_map->append_data_by_selector(res_null_map_column, selector, begin, end);
    } else {
        res_column.append_non_null_after_nested(old_rows, end - begin);
    }
}

void ColumnNullable::pop_back(size_t n) {
    get_nested_column().pop_back(n);
    if (_null_map) {
        _null_map->pop_back(n);
    }
}

ColumnPtr ColumnNullable::filter(const Filter& filt, ssize_t result_size_hint) const {
    ColumnPtr filtered_data = get_nested_column().filter(filt, result_size_hint);
    ColumnPtr filtered_null_map;
    if (_null_map) {
        filtered_null_map = _null_map->filter(filt, result_size_hint);
    }
    return ColumnNullable::create(filtered_data, filtered_null_map);
}

size_t ColumnNullable::filter(const Filter& filter) {
    const auto data_result_size = get_nested_column().filter(filter);
    if (_null_map) {
        const auto map_result_size = _null_map->filter(filter);
        CHECK_EQ(data_result_size, map_result_size);
    }
    return data_result_size;
}

Status ColumnNullable::filter_by_selector(const uint16_t* sel, size_t sel_size,
                                          IColumn* col_ptr) const {
    auto* nullable_col_ptr = assert_cast<ColumnNullable*>(col_ptr);
    DCHECK_EQ(nullable_col_ptr->size(), 0);
    nullable_col_ptr->materialize_null_map_for_selected_rows(*this, sel, sel_size);
    auto nested_column = nullable_col_ptr->get_nested_column_ptr();

    RETURN_IF_ERROR(get_nested_column().filter_by_selector(sel, sel_size, nested_column.get()));
    nullable_col_ptr->check_append_after_nested(0, sel_size);
    if (_null_map && nullable_col_ptr->_null_map) {
        auto& res_nullmap = nullable_col_ptr->_null_map->get_data();
        res_nullmap.resize(sel_size);
        const auto& cur_nullmap = _null_map->get_data();
        for (size_t i = 0; i < sel_size; ++i) {
            res_nullmap[i] = cur_nullmap[sel[i]];
        }
    } else {
        nullable_col_ptr->append_non_null_after_nested(0, sel_size);
    }
    return Status::OK();
}

MutableColumnPtr ColumnNullable::permute(const Permutation& perm, size_t limit) const {
    MutableColumnPtr permuted_data = get_nested_column().permute(perm, limit);
    MutableColumnPtr permuted_null_map;
    if (_null_map) {
        permuted_null_map = _null_map->permute(perm, limit);
    }
    return ColumnNullable::create(std::move(permuted_data), std::move(permuted_null_map));
}

int ColumnNullable::compare_at(size_t n, size_t m, const IColumn& rhs_,
                               int null_direction_hint) const {
    /// NULL values share the properties of NaN values.
    /// Here the last parameter of compare_at is called null_direction_hint
    /// instead of the usual nan_direction_hint and is used to implement
    /// the ordering specified by either NULLS FIRST or NULLS LAST in the
    /// ORDER BY construction.
    const auto& nullable_rhs = assert_cast<const ColumnNullable&>(rhs_);

    if (is_null_at(n)) {
        return nullable_rhs.is_null_at(m) ? 0 : null_direction_hint;
    }
    if (nullable_rhs.is_null_at(m)) {
        return -null_direction_hint;
    }

    return get_nested_column().compare_at(n, m, nullable_rhs.get_nested_column(),
                                          null_direction_hint);
}

void ColumnNullable::compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                                      int direction, std::vector<uint8_t>& cmp_res,
                                      uint8_t* __restrict filter) const {
    const auto& rhs_null_column = assert_cast<const ColumnNullable&>(rhs);
    const bool right_is_null = rhs.is_null_at(rhs_row_id);
    const bool left_contains_null = has_null();
    if (!left_contains_null && !right_is_null) {
        get_nested_column().compare_internal(rhs_row_id, rhs_null_column.get_nested_column(),
                                             nan_direction_hint, direction, cmp_res, filter);
    } else {
        auto sz = this->size();
        DCHECK(cmp_res.size() == sz);

        size_t begin = simd::find_zero(cmp_res, 0);
        while (begin < sz) {
            size_t end = simd::find_one(cmp_res, begin + 1);
            if (right_is_null) {
                for (size_t row_id = begin; row_id < end; row_id++) {
                    if (!is_null_at(row_id)) {
                        if ((-nan_direction_hint * direction) < 0) {
                            filter[row_id] = 1;
                            cmp_res[row_id] = 1;
                        } else if ((-nan_direction_hint * direction) > 0) {
                            cmp_res[row_id] = 1;
                        }
                    }
                }
            } else {
                for (size_t row_id = begin; row_id < end; row_id++) {
                    if (is_null_at(row_id)) {
                        if (nan_direction_hint * direction < 0) {
                            filter[row_id] = 1;
                            cmp_res[row_id] = 1;
                        } else if (nan_direction_hint * direction > 0) {
                            cmp_res[row_id] = 1;
                        }
                    }
                }
            }
            begin = simd::find_zero(cmp_res, end + 1);
        }
        if (!right_is_null) {
            get_nested_column().compare_internal(rhs_row_id, rhs_null_column.get_nested_column(),
                                                 nan_direction_hint, direction, cmp_res, filter);
        }
    }
}

void ColumnNullable::get_permutation(bool reverse, size_t limit, int null_direction_hint,
                                     HybridSorter& sorter, Permutation& res) const {
    /// Cannot pass limit because of unknown amount of NULLs.
    get_nested_column().get_permutation(reverse, 0, null_direction_hint, sorter, res);
    if (!_null_map) {
        return;
    }

    if ((null_direction_hint > 0) != reverse) {
        /// Shift all NULL values to the end.

        size_t read_idx = 0;
        size_t write_idx = 0;
        size_t end_idx = res.size();

        if (!limit) {
            limit = end_idx;
        } else {
            limit = std::min(end_idx, limit);
        }

        while (read_idx < limit && !is_null_at(res[read_idx])) {
            ++read_idx;
            ++write_idx;
        }

        ++read_idx;

        /// Invariants:
        ///  write_idx < read_idx
        ///  write_idx points to NULL
        ///  read_idx will be incremented to position of next not-NULL
        ///  there are range of NULLs between write_idx and read_idx - 1,
        /// We are moving elements from end to begin of this range,
        ///  so range will "bubble" towards the end.
        /// Relative order of NULL elements could be changed,
        ///  but relative order of non-NULLs is preserved.

        while (read_idx < end_idx && write_idx < limit) {
            if (!is_null_at(res[read_idx])) {
                std::swap(res[read_idx], res[write_idx]);
                ++write_idx;
            }
            ++read_idx;
        }
    } else {
        /// Shift all NULL values to the beginning.

        ssize_t read_idx = res.size() - 1;
        ssize_t write_idx = res.size() - 1;

        while (read_idx >= 0 && !is_null_at(res[read_idx])) {
            --read_idx;
            --write_idx;
        }

        --read_idx;

        while (read_idx >= 0 && write_idx >= 0) {
            if (!is_null_at(res[read_idx])) {
                std::swap(res[read_idx], res[write_idx]);
                --write_idx;
            }
            --read_idx;
        }
    }
}

void ColumnNullable::reserve(size_t n) {
    get_nested_column().reserve(n);
    if (_null_map) {
        _null_map->get_data().reserve(n);
    }
}

void ColumnNullable::resize(size_t n) {
    get_nested_column().resize(n);
    if (_null_map) {
        _null_map->get_data().resize(n);
    }
}

size_t ColumnNullable::byte_size() const {
    return get_nested_column().byte_size() + (_null_map ? _null_map->byte_size() : 0);
}

size_t ColumnNullable::allocated_bytes() const {
    return get_nested_column().allocated_bytes() + (_null_map ? _null_map->allocated_bytes() : 0);
}

bool ColumnNullable::has_enough_capacity(const IColumn& src) const {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    if (!get_nested_column().has_enough_capacity(src_concrete.get_nested_column())) {
        return false;
    }
    if (!src_concrete._null_map) {
        return !_null_map || _null_map->get_data().capacity() - size() > src_concrete.size();
    }
    return _null_map && _null_map->has_enough_capacity(*src_concrete._null_map);
}

template <bool negative>
void ColumnNullable::apply_null_map_impl(const ColumnUInt8& map) {
    const NullMap& arr2 = map.get_data();

    if (size() != arr2.size()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Inconsistent sizes of ColumnNullable objects. Self: {}. Expect: {}",
                               size(), arr2.size());
    }

    if (!_null_map) {
        const bool adds_null = negative ? std::any_of(arr2.begin(), arr2.end(),
                                                      [](UInt8 flag) { return (1 ^ flag) != 0; })
                                        : simd::contain_one(arr2.data(), arr2.size());
        if (!adds_null) {
            return;
        }
    }

    NullMap& arr1 = materialize_null_map_for_write().get_data();
    for (size_t i = 0, size = arr1.size(); i < size; ++i) {
        arr1[i] |= negative ^ arr2[i];
    }
}

void ColumnNullable::apply_null_map(const ColumnUInt8& map) {
    apply_null_map_impl<false>(map);
}

void ColumnNullable::apply_negated_null_map(const ColumnUInt8& map) {
    apply_null_map_impl<true>(map);
}

void ColumnNullable::apply_null_map(const ColumnNullable& other) {
    if (size() != other.size()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Inconsistent sizes of ColumnNullable objects. Self: {}. Expect: {}",
                               size(), other.size());
    }
    if (other._null_map) {
        apply_null_map(*other._null_map);
    }
}

void ColumnNullable::check_consistency() const {
    check_nullable_sizes(*_nested_column, null_map_column_if_materialized());
}

void ColumnNullable::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                 IColumn::Permutation& perms, EqualRange& range,
                                 bool last_column) const {
    sorter->sort_column(static_cast<const ColumnNullable&>(*this), flags, perms, range,
                        last_column);
}

bool ColumnNullable::only_null() const {
    if (size() == 0) {
        return true;
    }
    return _null_map && !simd::contain_zero(_null_map->get_data().data(), size());
}

ColumnNullable::NullMapState ColumnNullable::get_null_map_state() const {
    if (!_null_map || size() == 0) {
        return {.has_null = false, .only_null = size() == 0};
    }

    const auto& null_map = _null_map->get_data();
    if (null_map[0]) {
        return {.has_null = true,
                .only_null = !simd::contain_zero(null_map.data() + 1, null_map.size() - 1)};
    }
    return {.has_null = simd::contain_one(null_map.data() + 1, null_map.size() - 1),
            .only_null = false};
}

bool ColumnNullable::try_replace_null_payload_with_default_without_cow() const {
    if (!is_exclusive()) {
        return false;
    }
    if (!_null_map) {
        return true;
    }

    const_cast<IColumn&>(get_nested_column()).replace_column_null_data(get_null_map_data().data());
    return true;
}

bool ColumnNullable::has_null(size_t begin, size_t end) const {
    DCHECK_LE(begin, end);
    DCHECK_LE(end, size());
    return _null_map && simd::contain_one(_null_map->get_data().data() + begin, end - begin);
}

bool ColumnNullable::has_null() const {
    return has_null(0, size());
}

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable) {
    if (is_column_nullable(*column)) {
        return column;
    }

    if (is_column_const(*column)) {
        return ColumnConst::create(
                make_nullable(assert_cast<const ColumnConst&>(*column).get_data_column_ptr(),
                              is_nullable),
                column->size());
    }

    return ColumnNullable::create(column, ColumnUInt8::create(column->size(), is_nullable ? 1 : 0));
}

ColumnPtr remove_nullable(const ColumnPtr& column) {
    if (is_column_nullable(*column)) {
        return assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column.get())
                ->get_nested_column_ptr();
    }

    if (is_column_const(*column)) {
        const auto& column_nested =
                assert_cast<const ColumnConst&, TypeCheckOnRelease::DISABLE>(*column)
                        .get_data_column_ptr();
        if (is_column_nullable(*column_nested)) {
            return ColumnConst::create(
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*column_nested)
                            .get_nested_column_ptr(),
                    column->size());
        }
    }

    return column;
}

} // namespace doris
