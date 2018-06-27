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

#include <chrono>
#include <vector>
#include <utility>
#include "arrow/test-util.h"
#include "gandiva/arrow.h"
#include "gandiva/projector.h"

#ifndef GANDIVA_TEST_UTIL_H
#define GANDIVA_TEST_UTIL_H

#define THOUSAND (1024)
#define MILLION  (1024 * 1024)

namespace gandiva {

// Helper function to create an arrow-array of type ARROWTYPE
// from primitive vectors of data & validity.
//
// arrow/test-util.h has good utility classes for this purpose.
// Using those
template<typename TYPE, typename C_TYPE>
static ArrayPtr MakeArrowArray(std::vector<C_TYPE> values,
                               std::vector<bool> validity) {
  ArrayPtr out;
  arrow::ArrayFromVector<TYPE, C_TYPE>(validity, values, &out);
  return out;
}
#define MakeArrowArrayBool MakeArrowArray<arrow::BooleanType, bool>
#define MakeArrowArrayInt8 MakeArrowArray<arrow::Int8Type, int8_t>
#define MakeArrowArrayInt16 MakeArrowArray<arrow::Int16Type, int16_t>
#define MakeArrowArrayInt32 MakeArrowArray<arrow::Int32Type, int32_t>
#define MakeArrowArrayInt64 MakeArrowArray<arrow::Int64Type, int64_t>
#define MakeArrowArrayUint8 MakeArrowArray<arrow::Unt8Type, uint8_t>
#define MakeArrowArrayUint16 MakeArrowArray<arrow::Unt16Type, uint16_t>
#define MakeArrowArrayUint32 MakeArrowArray<arrow::Unt32Type, uint32_t>
#define MakeArrowArrayUint64 MakeArrowArray<arrow::Unt64Type, uint64_t>
#define MakeArrowArrayFloat32 MakeArrowArray<arrow::FloatType, float>
#define MakeArrowArrayFloat64 MakeArrowArray<arrow::DoubleType, double>

#define EXPECT_ARROW_ARRAY_EQUALS(a, b)         \
  EXPECT_TRUE((a)->Equals(b))                   \
      << "expected array: " << (a)->ToString()  \
      << " actual array: " << (b)->ToString();

template<typename C_TYPE>
std::vector<C_TYPE> GenerateData(int field_num,
                                 int num_records,
                                 C_TYPE (*fn_ptr)(int)) {
  std::vector<C_TYPE> data;

  for (int i = 0; i < num_records; i++) {
    data.push_back((*fn_ptr)(field_num));
  }

  return data;
}

// all types are of the same type
template<typename TYPE, typename C_TYPE>
Status TimedEvaluate(SchemaPtr schema,
                     std::shared_ptr<Projector> projector,
                     C_TYPE (*fn_ptr)(int),
                     int num_records,
                     int batch_size,
                     int64_t& num_millis) {
  int num_remaining = num_records;
  int num_fields = schema->num_fields();
  int num_calls = 0;
  Status status;
  std::chrono::duration<int64_t, std::micro> micros(0);
  std::chrono::time_point<std::chrono::high_resolution_clock> start;
  std::chrono::time_point<std::chrono::high_resolution_clock> finish;

  while (num_remaining > 0) {
    int num_in_batch = batch_size;
    if (batch_size > num_remaining) {
      num_in_batch = num_remaining;
    }

    // generate data for all columns in the schema
    std::vector<ArrayPtr> columns;
    for (int col = 0; col < num_fields; col++) {
      std::vector<C_TYPE> data = GenerateData<C_TYPE>(col, num_in_batch, fn_ptr);
      std::vector<bool> validity (num_in_batch, true);
      ArrayPtr col_data = MakeArrowArray<TYPE, C_TYPE>(data, validity);

      columns.push_back(col_data);
    }

    // make the record batch
    auto in_batch = arrow::RecordBatch::Make(schema, num_in_batch, columns);

    // evaluate
    arrow::ArrayVector outputs;
    start = std::chrono::high_resolution_clock::now();
    status = projector->Evaluate(*in_batch, &outputs);
    finish = std::chrono::high_resolution_clock::now();
    if (!status.ok()) {
      return status;
    }

    micros += std::chrono::duration_cast<std::chrono::microseconds>(finish - start);
    num_calls++;
    num_remaining -= num_in_batch;
  }

  num_millis = micros.count() / 1000;
  return status;
}

} // namespace gandiva

#endif // GANDIVA_TEST_UTIL_H
