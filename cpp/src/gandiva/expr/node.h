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
#ifndef GANDIVA_EXPR_NODE_H
#define GANDIVA_EXPR_NODE_H

#include "arrow.h"
#include "function_signature.h"
//#include "value_validity_pair.h"

namespace gandiva {

// Dummy class
// TODO: Remove this and add back the include
class ValueValidityPair {
};

/*
 * Represents a node in the expression tree. Validity and value are
 * in a joined state.
 */
class Node {
  public:
    Node(const DataTypeSharedPtr type)
      : type_(type) { }

    DataTypeSharedPtr getReturnType() { return type_; }

    /*
     * Called during code generation to separate out validity and value.
     */
    virtual ValueValidityPair *Decompose() = 0;

  protected:
    DataTypeSharedPtr type_;
};

using NodeSharedPtr = std::shared_ptr<Node>;

/*
 * Used to represent constants of various datatypes
 */
class LiteralNode : public Node {
  public:
    LiteralNode(const FieldSharedPtr field)
      : Node(field->type()), name_(field->name()) {}

    virtual ValueValidityPair *Decompose() override {
      return NULL;
    }

  private:
    const std::string name_;
};

/*
 * Used to represent functions
 */
class FunctionNode : public Node {
  public:
    FunctionNode(FunctionSignature func_signature, const std::vector<NodeSharedPtr> children, DataTypeSharedPtr retType)
      : Node(retType), func_signature_(func_signature), children_(children) { }

    static FunctionNode *createFunction(const std::string &name, const std::vector<NodeSharedPtr> children, DataTypeSharedPtr retType)
    {
      std::vector<DataTypeSharedPtr> paramTypes;
      for(std::vector<const NodeSharedPtr>::iterator it = children.begin(); it != children.end(); ++it) {
        auto arg = (*it)->getReturnType();
        paramTypes.push_back(arg);
      }

      auto func_signature = FunctionSignature(name, paramTypes, retType);
      return new FunctionNode(func_signature, children, retType);
    }

    virtual ValueValidityPair *Decompose() override {
      return NULL;
    }

    FunctionSignature func_signature() { return func_signature_; }

  private:
    FunctionSignature func_signature_;
    const std::vector<NodeSharedPtr> children_;
};

} // namespace gandiva

#endif // GANDIVA_EXPR_NODE_H
