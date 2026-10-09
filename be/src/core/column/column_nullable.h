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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnNullable.h
// and modified by Doris

#pragma once

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "core/cow.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/typeid_cast.h"
#include "core/types.h"
#include "storage/rowset_id.h"

class SipHash;

namespace doris {
class Arena;
class ColumnSorter;

using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap*;

/// A nullable column owns a nested column and an optional byte null map.
/// An absent map means every existing row is non-NULL. A materialized map
/// has the same size as the nested column at completed operation boundaries
/// and may contain only zeros. Materialized maps are not automatically released.
class ColumnNullable final : public COWHelper<IColumn, ColumnNullable> {
private:
    friend class COWHelper<IColumn, ColumnNullable>;

    explicit ColumnNullable(MutableColumnPtr&& nested_column_);
    ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_);
    struct SharedTag {};
    ColumnNullable(SharedTag, ColumnPtr nested_column_, ColumnPtr null_map_);
    ColumnNullable(MutableColumnPtr&& nested_column_, ColumnUInt8::MutablePtr&& null_map_);
    ColumnNullable(const ColumnNullable&) = default;

public:
    struct NullMapState {
        bool has_null;
        bool only_null;
    };

    /** Create a column from immutable/shared subcolumns without cloning them.
      * Call IColumn::mutate before modifying the returned column tree.
      */
    using Base = COWHelper<IColumn, ColumnNullable>;
    static MutablePtr create(const ColumnPtr& nested_column_, const ColumnPtr& null_map_ = {}) {
        return Base::create(SharedTag {}, nested_column_, null_map_);
    }

    static MutablePtr create(MutableColumnPtr&& nested_column_,
                             ColumnUInt8::MutablePtr&& null_map_) {
        return Base::create(std::move(nested_column_), std::move(null_map_));
    }

    template <typename... Args>
    static MutablePtr create(Args&&... args)
        requires IsMutableColumns<Args...>::value
    {
        return Base::create(std::forward<Args>(args)...);
    }

    void sanity_check() const override {
        check_consistency();
        _nested_column->sanity_check();
        if (_null_map) {
            _null_map->sanity_check();
        }
    }

    bool is_variable_length() const override { return _nested_column->is_variable_length(); }

    std::string get_name() const override { return "Nullable(" + _nested_column->get_name() + ")"; }
    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override { return _nested_column->size(); }
    bool is_null_at(size_t n) const override { return _null_map && _null_map->get_data()[n] != 0; }
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool get_bool(size_t n) const override {
        return is_null_at(n) ? false : _nested_column->get_bool(n);
    }
    // column must be nullable(uint8)
    bool get_bool_inline(size_t n) const {
        return is_null_at(n) ? false
                             : assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                       _nested_column.get())
                                       ->get_bool(n);
    }
    StringRef get_data_at(size_t n) const override;

    /// Will insert null value if pos=nullptr
    void insert_data(const char* pos, size_t length) override;

    void insert_many_strings(const StringRef* strings, size_t num) override;

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t get_max_row_byte_size() const override;

    void serialize(StringRef* keys, size_t num_rows) const override;

    void deserialize(StringRef* keys, size_t num_rows) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;
    void insert_indices_from_not_has_null(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end);

    void insert(const Field& x) override;
    void insert_duplicate_fields(const Field& x, const size_t n) override;
    void insert_from(const IColumn& src, size_t n) override;

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    void append_data_by_selector(IColumn::MutablePtr& res,
                                 const IColumn::Selector& selector) const override;

    void append_data_by_selector(IColumn::MutablePtr& res, const IColumn::Selector& selector,
                                 size_t begin, size_t end) const override;

    template <typename ColumnType>
    void insert_from_with_type(const IColumn& src, size_t n) {
        const auto& src_concrete =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(src);
        const size_t old_rows = size();
        const UInt8 flag = src_concrete._null_map ? src_concrete._null_map->get_data()[n] : 0;
        assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(_nested_column.get())
                ->insert_from(src_concrete.get_nested_column(), n);
        append_repeated_null_flag_after_nested(old_rows, flag, 1);
    }

    void insert_many_fix_len_data(const char* pos, size_t num) override {
        const size_t old_rows = size();
        get_nested_column().insert_many_fix_len_data(pos, num);
        append_non_null_after_nested(old_rows, num);
    }

    void insert_many_raw_data(const char* pos, size_t num) override {
        DCHECK(pos);
        const size_t old_rows = size();
        get_nested_column().insert_many_raw_data(pos, num);
        append_non_null_after_nested(old_rows, num);
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t data_num, uint32_t dict_num) override {
        const size_t old_rows = size();
        get_nested_column().insert_many_dict_data(data_array, start_index, dict, data_num,
                                                  dict_num);
        append_non_null_after_nested(old_rows, data_num);
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        const size_t old_rows = size();
        get_nested_column().insert_many_continuous_binary_data(data, offsets, num);
        append_non_null_after_nested(old_rows, num);
    }

    void insert_offsets_from_lengths(const uint32_t* lengths, size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        const size_t old_rows = size();
        get_nested_column().insert_offsets_from_lengths(lengths, num);
        append_non_null_after_nested(old_rows, num);
    }

    // Default value in `ColumnNullable` is null
    void insert_default() override {
        const size_t old_rows = size();
        get_nested_column().insert_default();
        append_repeated_null_flag_after_nested(old_rows, 1, 1);
    }

    void insert_many_defaults(size_t length) override {
        const size_t old_rows = size();
        get_nested_column().insert_many_defaults(length);
        append_repeated_null_flag_after_nested(old_rows, 1, length);
    }

    void insert_not_null_elements(size_t num) {
        const size_t old_rows = size();
        get_nested_column().insert_many_defaults(num);
        append_non_null_after_nested(old_rows, num);
    }

    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const Filter& filter) override;

    Status filter_by_selector(const uint16_t* sel, size_t sel_size,
                              IColumn* col_ptr) const override;
    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int null_direction_hint) const override;

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8_t>& cmp_res,
                          uint8_t* __restrict filter) const override;
    void get_permutation(bool reverse, size_t limit, int null_direction_hint, HybridSorter& sorter,
                         Permutation& res) const override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;
    void update_crc32c_batch(uint32_t* __restrict hashes,
                             const uint8_t* __restrict null_map) const override;

    void update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                              const uint8_t* __restrict null_map) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    ColumnPtr convert_column_if_overflow() override {
        _nested_column = _nested_column->convert_column_if_overflow();
        return get_ptr();
    }

    void mutate_subcolumns() override {
        mutate_subcolumn(_nested_column);
        if (_null_map) {
            mutate_subcolumn<ColumnUInt8>(_null_map);
        }
    }

    void for_each_subcolumn(ColumnCallback callback) const override {
        callback(*static_cast<const IColumn::Ptr&>(_nested_column));
        if (_null_map) {
            callback(*static_cast<const ColumnUInt8::Ptr&>(_null_map));
        }
    }

    bool structure_equals(const IColumn& rhs) const override {
        if (const auto* rhs_nullable = check_and_get_column<ColumnNullable>(&rhs)) {
            return _nested_column->structure_equals(*rhs_nullable->_nested_column);
        }
        return false;
    }

    bool is_nullable() const override { return true; }
    bool is_column_string() const override { return get_nested_column().is_column_string(); }

    bool is_exclusive() const override {
        return IColumn::is_exclusive() && _nested_column->is_exclusive() &&
               (!_null_map || _null_map->is_exclusive());
    }

    bool try_replace_null_payload_with_default_without_cow() const override;

    bool only_null() const override;
    NullMapState get_null_map_state() const;

    // used in schema change
    void change_nested_column(ColumnPtr& other) { ((ColumnPtr&)_nested_column) = other; }

    void replace_columns(ColumnPtr nested_column, ColumnPtr null_map);

    /// Return the column that represents values.
    IColumn& get_nested_column() { return *_nested_column; }
    const IColumn& get_nested_column() const { return *_nested_column; }

    const ColumnPtr& get_nested_column_ptr() const { return _nested_column; }

    MutableColumnPtr get_nested_column_ptr() { return _nested_column->assert_mutable(); }

    void clear() override {
        if (_null_map) {
            _null_map->clear();
        }
        _nested_column->clear();
    }

    /// Apply the null byte map of a specified nullable column onto the
    /// null byte map of the current column by performing an element-wise OR
    /// between both byte maps. This method is used to determine the null byte
    /// map of the result column of a function taking one or more nullable
    /// columns.
    void apply_null_map(const ColumnNullable& other);
    void apply_null_map(const ColumnUInt8& map);
    void apply_negated_null_map(const ColumnUInt8& map);

    /// Check that size of null map equals to size of nested column.
    void check_consistency() const;

    bool has_null(size_t begin, size_t end) const override;

    bool has_null() const override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        const auto& nullable_rhs =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(rhs);
        const UInt8 flag = nullable_rhs._null_map ? nullable_rhs._null_map->get_data()[row] : 0;
        if (flag) {
            materialize_null_map_for_write().get_data()[self_row] = flag;
        } else if (_null_map) {
            _null_map->get_data()[self_row] = 0;
        }

        if (!flag) {
            _nested_column->replace_column_data(*nullable_rhs._nested_column, row, self_row);
        }
    }

    // Batch replace continuous range of data (for sparse compaction optimization)
    // NOTE: This function is designed for "all non-NULL" scenario where the caller
    // has already verified that all source values are non-NULL using SIMD count.
    // For mixed NULL/non-NULL cases, use replace_column_data in a loop.
    void replace_column_data_range(const IColumn& rhs, size_t src_start, size_t count,
                                   size_t self_start) override {
        DCHECK(size() >= self_start + count);
        const auto& nullable_rhs =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(rhs);
        DCHECK(nullable_rhs.size() >= src_start + count);

        DCHECK(!nullable_rhs.has_null(src_start, src_start + count));
        if (_null_map && count != 0) {
            memset(_null_map->get_data().data() + self_start, 0, count);
        }

        // Batch copy nested column data using optimized replace_column_data_range
        // This leverages memcpy in ColumnVector/ColumnDecimal for maximum performance
        _nested_column->replace_column_data_range(*nullable_rhs._nested_column, src_start, count,
                                                  self_start);
    }

    // Delegate to nested column - only fixed-width types support efficient replacement
    bool support_replace_column_data_range() const override {
        return _nested_column->support_replace_column_data_range();
    }

    void replace_float_special_values() override { _nested_column->replace_float_special_values(); }

    MutableColumnPtr convert_to_predicate_column_if_dictionary() override {
        _nested_column = get_nested_column().convert_to_predicate_column_if_dictionary();
        return get_ptr();
    }

    void convert_dict_codes_if_necessary() override {
        get_nested_column().convert_dict_codes_if_necessary();
    }

    void initialize_hash_values_for_runtime_filter() override {
        get_nested_column().initialize_hash_values_for_runtime_filter();
    }

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void set_rowset_segment_id(std::pair<RowsetId, uint32_t> rowset_segment_id) override {
        _nested_column->set_rowset_segment_id(rowset_segment_id);
    }

    std::pair<RowsetId, uint32_t> get_rowset_segment_id() const override {
        return _nested_column->get_rowset_segment_id();
    }

    void finalize() override { get_nested_column().finalize(); }

    void erase(size_t start, size_t length) override {
        get_nested_column().erase(start, length);
        if (_null_map) {
            _null_map->erase(start, length);
        }
    }

    size_t serialize_impl(char* pos, const size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_size_at(size_t row) const override {
        return sizeof(NullMap::value_type) +
               (is_null_at(row) ? 0 : _nested_column->serialize_size_at(row));
    }

    /// Read-only probes never allocate; nullptr means all existing rows are non-NULL.
    const ColumnUInt8* null_map_column_if_materialized() const { return _null_map.get(); }
    const NullMap* null_map_data_if_materialized() const {
        return _null_map ? &_null_map->get_data() : nullptr;
    }

    /// Requires a mutable column tree and consistent subcolumn sizes. Call before
    /// modifying either subcolumn through the legacy getters, not after nested grows.
    ColumnUInt8& materialize_null_map_for_write();

    /// Record old_rows before changing nested. Until commit, the column must not be read.
    void append_non_null_after_nested(size_t old_rows, size_t appended_rows);
    /// flags must not alias this column's map; nullptr denotes all non-NULL flags.
    void append_null_flags_after_nested(size_t old_rows, const UInt8* flags, size_t appended_rows);
    void set_null_at(size_t row);

    /// Legacy pointer reads may return nullptr. Reference getters require a materialized map.
    const ColumnUInt8::Ptr& get_null_map_column_ptr() const { return _null_map; }
    const ColumnUInt8& get_null_map_column() const {
        DORIS_CHECK(_null_map) << "Use null_map_column_if_materialized for an implicit null map";
        return *_null_map;
    }
    const NullMap& get_null_map_data() const { return get_null_map_column().get_data(); }

    ColumnUInt8::MutablePtr get_null_map_column_ptr() {
        DORIS_CHECK(_null_map) << "Materialize the null map before modifying either subcolumn";
        auto null_map = _null_map->assert_mutable();
        return ColumnUInt8::cast_to_column_mutptr(
                assert_cast<ColumnUInt8*, TypeCheckOnRelease::DISABLE>(null_map.get()));
    }
    ColumnUInt8& get_null_map_column() {
        DORIS_CHECK(_null_map) << "Materialize the null map before modifying either subcolumn";
        return *_null_map;
    }
    NullMap& get_null_map_data() { return get_null_map_column().get_data(); }

    /// Legacy map-only writes require explicit materialization before either subcolumn grows.
    void push_false_to_nullmap(size_t num) { get_null_map_column().insert_many_vals(0, num); }
    void fill_false_to_nullmap(size_t num) {
        get_null_map_column().clear();
        get_null_map_column().insert_many_vals(0, num);
    }

private:
    void check_append_after_nested(size_t old_rows, size_t appended_rows) const;
    void append_repeated_null_flag_after_nested(size_t old_rows, UInt8 flag, size_t appended_rows);
    void append_null_map_range_after_nested(size_t old_rows, const ColumnNullable& src,
                                            size_t start, size_t length);

    template <typename Index>
    void materialize_null_map_for_selected_rows(const ColumnNullable& src, const Index* indices,
                                                size_t count) {
        if (!_null_map && src._null_map) {
            for (size_t i = 0; i != count; ++i) {
                if (src.is_null_at(indices[i])) {
                    materialize_null_map_for_write();
                    return;
                }
            }
        }
    }
    template <bool negative>
    void apply_null_map_impl(const ColumnUInt8& map);

    IColumn::WrappedPtr _nested_column;
    ColumnUInt8::WrappedPtr _null_map;
};

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable = false);
ColumnPtr remove_nullable(const ColumnPtr& column);
} // namespace doris
