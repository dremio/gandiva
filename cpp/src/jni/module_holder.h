/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 */

#ifndef JNI_MODULE_HOLDER_H
#define JNI_MODULE_HOLDER_H

#include <utility>
#include <memory>

#include "codegen/stop_watch.h"
#include "gandiva/arrow.h"
#include "gandiva/projector.h"

namespace gandiva {

class ReusableBuffer : public arrow::Buffer {
 public:
     ReusableBuffer(uint8_t* data, const int64_t size) : Buffer(data, size) {}

     void set_sz(const int64_t size) {
       size_ = size;
       capacity_ = size;
     }

     void set_buf(const uint8_t *buf) {
       data_ = buf;
     }
};

class ReusableMutableBuffer : public arrow::MutableBuffer {
 public:
     ReusableMutableBuffer(uint8_t* data, const int64_t size)
       : MutableBuffer(data, size) {}

     void set_sz(const int64_t size) {
       size_ = size;
       capacity_ = size;
     }

     void set_buf(const uint8_t *buf) {
       data_ = buf;
     }
};

class ProjectorHolder {
 public:
    ProjectorHolder(SchemaPtr schema,
                    FieldVector ret_types,
                    std::shared_ptr<Projector> projector)
    : schema_(schema),
      ret_types_(ret_types),
      projector_(std::move(projector)) {

      for (auto &field : schema->fields()) {
        auto validity = std::make_shared<ReusableBuffer>(nullptr, 0);
        auto data = std::make_shared<ReusableBuffer>(nullptr, 0);
        auto array_data = arrow::ArrayData::Make(field->type(), 0 /*num_rows*/,
                                                 {validity, data});
        inputs_.push_back(array_data);
      }

      for (auto &field : ret_types) {
        auto validity = std::make_shared<ReusableMutableBuffer>(nullptr, 0);
        auto data = std::make_shared<ReusableMutableBuffer>(nullptr, 0);

        auto array_data = arrow::ArrayData::Make(field->type(), 0 /*num_rows*/,
                                                 {validity, data});
        outputs_.push_back(array_data);
      }
    }

    SchemaPtr &schema() { return schema_; }
    const FieldVector &rettypes() { return ret_types_; }
    std::shared_ptr<Projector> &projector() { return projector_; }

    ArrayDataVector &inputs() { return inputs_; }
    ArrayDataVector &outputs() { return outputs_; }

    StopWatch &eval_timer() { return eval_timer_; }
    StopWatch &jni_timer() { return jni_timer_; }
    StopWatch &prepargs_timer() { return prepargs_timer_; }

 private:
    SchemaPtr schema_;
    FieldVector ret_types_;
    std::shared_ptr<Projector> projector_;

    ArrayDataVector inputs_;
    ArrayDataVector outputs_;

    StopWatch eval_timer_;
    StopWatch jni_timer_;
    StopWatch prepargs_timer_;

};

} // namespace gandiva

#endif // JNI_MODULE_HOLDER_H
