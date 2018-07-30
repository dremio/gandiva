// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef GANDIVA_SELECTION_VECTOR__H
#define GANDIVA_SELECTION_VECTOR__H

#include "gandiva/arrow.h"
#include "gandiva/logging.h"
#include "gandiva/status.h"

namespace gandiva {

/// \brief Selection Vector : vector of indices in a row-batch for a selection.
template <typename C_TYPE, typename A_TYPE>
class SelectionVector {
 public:
  SelectionVector(int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer)
      : max_slots_(max_slots), buffer_(buffer) {
    raw_data_ = reinterpret_cast<C_TYPE *>(buffer->mutable_data());
  }

  ~SelectionVector() = default;

  /// Get the value at a given index.
  int GetIndex(int index) {
    DCHECK_LE(index, max_slots_);
    return raw_data_[index];
  }

  /// Set the value at a given index.
  void SetIndex(int index, int value) {
    DCHECK_LE(index, max_slots_);
    DCHECK_LE(value, GetMaxSupportedValue());

    raw_data_[index] = value;
  }

  virtual int GetMaxSupportedValue() const = 0;

  /// Convert to arrow-array.
  ArrayPtr ToArray();

  /// The maximum slots (capacity) of the selection vector.
  int GetMaxSlots() const { return max_slots_; }

  /// The number of slots (size) of the selection vector.
  int GetNumSlots() const { return num_slots_; }

  /// Set the number of slots in the selection vector.
  void SetNumSlots(int num_slots) {
    DCHECK_LE(num_slots, max_slots_);
    num_slots_ = num_slots;
  }

  /// populate selection vector for all the set bits in the bitmap.
  ///
  /// \param[in] : bitmap the bitmap
  /// \param[in] : bitmap_size size of the bitmap in bytes
  /// \param[in] : max_bitmap_index max valid index in bitmap (can be lesser than
  ///              capacity in the bitmap, due to alignment/padding).
  Status PopulateFromBitMap(const uint8_t *bitmap, int bitmap_size, int max_bitmap_index);

 protected:
  static Status AllocateBuffer(int max_slots, arrow::MemoryPool *pool,
                               std::shared_ptr<arrow::MutableBuffer> *buffer);

  static Status ValidateBuffer(int max_slots,
                               std::shared_ptr<arrow::MutableBuffer> buffer);

  /// maximum slots in the vector
  int max_slots_;

  /// number of slots in the vector
  int num_slots_;

  std::shared_ptr<arrow::MutableBuffer> buffer_;
  C_TYPE *raw_data_;
};

template <typename C_TYPE, typename A_TYPE>
Status SelectionVector<C_TYPE, A_TYPE>::PopulateFromBitMap(const uint8_t *bitmap,
                                                           int bitmap_size,
                                                           int max_bitmap_index) {
  GANDIVA_RETURN_FAILURE_IF_FALSE(
      bitmap_size % 8 == 0, Status::Invalid("bitmap must be padded to 64-bit size"));
  GANDIVA_RETURN_FAILURE_IF_FALSE(
      max_bitmap_index <= GetMaxSlots(),
      Status::Invalid("max_bitmap_index must be <= buffer capacity"));

  // jump  8-bytes at a time, add the index corresponding to each valid bit to the
  // the selection vector.
  int selection_idx = 0;
  const uint64_t *bitmap_64 = reinterpret_cast<const uint64_t *>(bitmap);
  for (int bitmap_idx = 0; bitmap_idx < bitmap_size / 8; ++bitmap_idx) {
    uint64_t current_word = bitmap_64[bitmap_idx];

    while (current_word != 0) {
      uint64_t highest_only = current_word & -current_word;
      int pos_in_word = __builtin_ctzl(highest_only);

      int pos_in_bitmap = bitmap_idx * 64 + pos_in_word;
      if (pos_in_bitmap > max_bitmap_index) {
        // the bitmap may be slighly larger for alignment/padding.
        break;
      }

      SetIndex(selection_idx, pos_in_bitmap);
      ++selection_idx;

      current_word ^= highest_only;
    }
  }

  SetNumSlots(selection_idx);
  return Status::OK();
}

template <typename C_TYPE, typename A_TYPE>
ArrayPtr SelectionVector<C_TYPE, A_TYPE>::ToArray() {
  auto data_type = arrow::TypeTraits<A_TYPE>::type_singleton();
  auto array_data = arrow::ArrayData::Make(data_type, num_slots_, {nullptr, buffer_});
  return arrow::MakeArray(array_data);
}

class SelectionVectorInt16 : public SelectionVector<int16_t, arrow::Int16Type> {
 public:
  SelectionVectorInt16(int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer)
      : SelectionVector(max_slots, buffer) {}

  int GetMaxSupportedValue() const override { return INT16_MAX; }

  /// \param[in] : max_slots max number of slots
  /// \param[in] : buffer buffer sized to accomadate max_slots
  /// \param[out]: selection_vector selection vector backed by 'buffer'
  static Status Make(int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer,
                     std::shared_ptr<SelectionVectorInt16> *selection_vector);

  /// \param[in] : max_slots max number of slots
  /// \param[in] : pool memory pool to allocate buffer
  /// \param[out]: selection_vector selection vector backed by a buffer allocated from the
  /// pool.
  static Status Make(int max_slots, arrow::MemoryPool *pool,
                     std::shared_ptr<SelectionVectorInt16> *selection_vector);
};

class SelectionVectorInt32 : public SelectionVector<int32_t, arrow::Int32Type> {
 public:
  SelectionVectorInt32(int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer)
      : SelectionVector(max_slots, buffer) {}

  int GetMaxSupportedValue() const override { return INT32_MAX; }

  /// \param[in] : max_slots max number of slots
  /// \param[in] : buffer buffer sized to accomadate max_slots
  /// \param[out]: selection_vector selection vector backed by 'buffer'
  static Status Make(int max_slots, std::shared_ptr<arrow::MutableBuffer> buffer,
                     std::shared_ptr<SelectionVectorInt32> *selection_vector);

  /// \param[in] : max_slots max number of slots
  /// \param[in] : pool memory pool to allocate buffer
  /// \param[out]: selection_vector selection vector backed by a buffer allocated from the
  /// pool.
  static Status Make(int max_slots, arrow::MemoryPool *pool,
                     std::shared_ptr<SelectionVectorInt32> *selection_vector);
};

}  // namespace gandiva

#endif  // GANDIVA_SELECTION_VECTOR__H
