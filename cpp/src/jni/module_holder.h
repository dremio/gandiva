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

#include <memory>

#include "gandiva/arrow.h"
#include "gandiva/projector.h"

namespace gandiva {

class ProjectorHolder {
  public:
    ProjectorHolder(SchemaPtr schema, FieldVector retTypes, std::shared_ptr<Projector> projector)
    : schema_(schema), 
      retTypes_(retTypes), 
      projector_(std::move(projector)) {}

    SchemaPtr schema() { return schema_; }
    FieldVector retTypes() { return retTypes_; }
    std::shared_ptr<Projector> projector() { return projector_; }

  private:
    SchemaPtr schema_;
    FieldVector retTypes_;
    std::shared_ptr<Projector> projector_;
};

} // namespace gandiva

#endif // JNI_MODULE_HOLDER_H
