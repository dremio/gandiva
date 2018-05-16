/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_FWD_H
#define GANDIVA_FWD_H

#include <memory>
#include <vector>

namespace gandiva {

class Dex;
using DexSharedPtr = std::shared_ptr<Dex>;

class DexVisitor;

class ValueValidityPair;
using ValueValidityPairSharedPtr = std::shared_ptr<ValueValidityPair>;

class FieldDescriptor;
using FieldDescriptorSharedPtr = std::shared_ptr<FieldDescriptor>;

class FuncDescriptor;
using FuncDescriptorSharedPtr = std::shared_ptr<FuncDescriptor>;

class LValue;
using LValueSharedPtr = std::shared_ptr<LValue>;

class Expression;
using ExpressionSharedPtr = std::shared_ptr<Expression>;
using ExpressionVector = std::vector<ExpressionSharedPtr>;

class Node;
using NodeSharedPtr = std::shared_ptr<Node>;

class Annotator;

class EvalBatch;
using EvalBatchSharedPtr = std::shared_ptr<EvalBatch>;

} // namespace gandiva

#endif // GANDIVA_FWD_H
