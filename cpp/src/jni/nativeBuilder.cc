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
 * limitations under the License.
 */

#include <mutex>
#include <atomic>
#include <string>
#include <utility>
#include <vector>
#include <memory>
#include <map>

#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <google/protobuf/io/coded_stream.h>

#include "jni/nativeBuilder.h"
#include "jni/module_holder.h"
#include "Types.pb.h"
#include "gandiva/tree_expr_builder.h"

#define INIT_MODULE_ID   (4)

using gandiva::DataTypePtr;
using gandiva::FieldPtr;
using gandiva::FieldVector;
using gandiva::SchemaPtr;
using gandiva::NodePtr;
using gandiva::NodeVector;
using gandiva::ExpressionPtr;
using gandiva::ExpressionVector;
using gandiva::TreeExprBuilder;
using gandiva::Projector;

using gandiva::ProjectorHolder;

// forward declarations
NodePtr ProtoTypeToNode(const types::TreeNode& node);

// to do one time initialization
std::once_flag onceMtx_;

// pool used by Gandiva to evaluate expressions
arrow::MemoryPool* pool_;

// map from module ids returned to Java and module pointers
std::map<jlong, std::shared_ptr<ProjectorHolder>> projectorModulesMap_;

// atomic counter for projector module ids
std::atomic<jlong> projectorModuleId_(INIT_MODULE_ID);

void InitPool() {
  pool_ = arrow::default_memory_pool();
}

void InitMemoryPool() {
  std::call_once(onceMtx_, InitPool);
}

DataTypePtr ProtoTypeToDataType(const types::ExtGandivaType& extType) {
  switch (extType.type()) {
    case types::NONE:
      return arrow::null();
    case types::BOOL:
      return arrow::boolean();
    case types::UINT8:
      return arrow::uint8();
    case types::INT8:
      return arrow::int8();
    case types::UINT16:
      return arrow::uint16();
    case types::INT16:
      return arrow::int16();
    case types::UINT32:
      return arrow::uint32();
    case types::INT32:
      return arrow::int32();
    case types::UINT64:
      return arrow::uint64();
    case types::INT64:
      return arrow::int64();
    case types::HALF_FLOAT:
      return arrow::float16();
    case types::FLOAT:
      return arrow::float32();
    case types::DOUBLE:
      return arrow::float64();
    case types::UTF8:
      return arrow::utf8();
    case types::BINARY:
      return arrow::binary();
    case types::DATE32:
      return arrow::date32();
    case types::DATE64:
      return arrow::date64();
    case types::DECIMAL:
      // TODO: error handling
      return arrow::decimal(extType.precision(), extType.scale());

    case types::FIXED_SIZE_BINARY:
    case types::TIMESTAMP:
    case types::TIME32:
    case types::TIME64:
    case types::INTERVAL:
    case types::LIST:
    case types::STRUCT:
    case types::UNION:
    case types::DICTIONARY:
    case types::MAP:
      return NULL;
  }
}

FieldPtr ProtoTypeToField(const types::Field& f) {
  const std::string &name = f.name();
  DataTypePtr type = ProtoTypeToDataType(f.type());
  bool nullable = true;
  if (f.has_nullable()) {
    nullable = f.nullable();
  }

  return field(name, type, nullable);
}

NodePtr ProtoTypeToFieldNode(const types::FieldNode& node) {
  FieldPtr fieldPtr = ProtoTypeToField(node.field());
  if (fieldPtr == NULL) {
    return NULL;
  }

  return TreeExprBuilder::MakeField(fieldPtr);
}

NodePtr ProtoTypeToFnNode(const types::FunctionNode& node) {
  const std::string &name = node.functionname();
  NodeVector children;

  for (int i = 0; i < node.inargs_size(); i++) {
    const types::TreeNode& arg = node.inargs(i);

    NodePtr n = ProtoTypeToNode(arg);
    if (n == NULL) {
      return NULL;
    }

    children.push_back(n);
  }

  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());
  if (return_type == NULL) {
    return NULL;
  }

  return TreeExprBuilder::MakeFunction(name, children, return_type);
}

NodePtr ProtoTypeToIfNode(const types::IfNode& node) {
  NodePtr cond = ProtoTypeToNode(node.cond());
  NodePtr thenNode = ProtoTypeToNode(node.thennode());
  NodePtr elseNode = ProtoTypeToNode(node.elsenode());
  DataTypePtr return_type = ProtoTypeToDataType(node.returntype());

  if ((cond == NULL) ||
      (thenNode == NULL) ||
      (elseNode == NULL) ||
      (return_type == NULL)) {
    return NULL;
  }

  return TreeExprBuilder::MakeIf(cond, thenNode, elseNode, return_type);
}

NodePtr ProtoTypeToNode(const types::TreeNode& node) {
  if (node.has_fieldnode()) {
    return ProtoTypeToFieldNode(node.fieldnode());
  }

  if (node.has_fnnode()) {
    return ProtoTypeToFnNode(node.fnnode());
  }

  if (node.has_ifnode()) {
    return ProtoTypeToIfNode(node.ifnode());
  }

  return NULL;
}

ExpressionPtr ProtoTypeToExpression(const types::ExpressionRoot& root, FieldPtr field) {
  NodePtr rootNode = ProtoTypeToNode(root.root());
  field = ProtoTypeToField(root.resulttype());

  if ((rootNode == NULL) || (field == NULL)) {
    return NULL;
  }

  return TreeExprBuilder::MakeExpression(rootNode, field);
}

SchemaPtr ProtoTypeToSchema(const types::Schema& schema) {
  std::vector<FieldPtr> fields;

  for (int i = 0; i < schema.columns_size(); i++) {
    FieldPtr field = ProtoTypeToField(schema.columns(i));
    if (field == NULL) {
      return NULL;
    }

    fields.push_back(field);
  }

  return arrow::schema(fields);
}

bool ParseProtobuf(uint8_t *buf, int bufLen, google::protobuf::Message *msg) {
  google::protobuf::io::CodedInputStream cis(buf, bufLen);
  return msg->ParseFromCodedStream(&cis);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_gandiva_evaluator_NativeBuilder_buildNativeCode
  (JNIEnv *env, jclass cls, jbyteArray schemaArr, jbyteArray exprsArr) {
  std::cout << "In buildNativeCode\n";
  jlong moduleID = 0LL;
  std::shared_ptr<Projector> projector;
  std::shared_ptr<ProjectorHolder> holder;

  types::Schema schema;
  jsize schemaLen = env->GetArrayLength(schemaArr);
  jbyte *schemaBytes = env->GetByteArrayElements(schemaArr, 0);

  types::ExpressionList exprs;
  jsize exprsLen = env->GetArrayLength(exprsArr);
  jbyte *exprsBytes = env->GetByteArrayElements(exprsArr, 0);

  ExpressionVector exprVector;
  SchemaPtr schemaPtr;
  FieldVector retTypes;
  gandiva::Status status;

  std::cout << "Parsing schema protobuf\n";
  if (!ParseProtobuf(reinterpret_cast<uint8_t *>(schemaBytes), schemaLen, &schema)) {
    env->ReleaseByteArrayElements(schemaArr, schemaBytes, JNI_ABORT);
    goto out;
  }

  std::cout << "Parsing exprs protobuf\n";
  if (!ParseProtobuf(reinterpret_cast<uint8_t *>(exprsBytes), exprsLen, &exprs)) {
    env->ReleaseByteArrayElements(schemaArr, schemaBytes, JNI_ABORT);
    env->ReleaseByteArrayElements(exprsArr, exprsBytes, JNI_ABORT);
    goto out;
  }

  // convert types::Schema to arrow::Schema
  schemaPtr = ProtoTypeToSchema(schema);
  if (schemaPtr == NULL) {
    env->ReleaseByteArrayElements(schemaArr, schemaBytes, JNI_ABORT);
    env->ReleaseByteArrayElements(exprsArr, exprsBytes, JNI_ABORT);
    goto out;
  }

  // create Expression out of the list of exprs
  for (int i = 0; i < exprs.exprs_size(); i++) {
    FieldPtr retType;
    ExpressionPtr root = ProtoTypeToExpression(exprs.exprs(i), retType);

    if (root == NULL) {
      env->ReleaseByteArrayElements(schemaArr, schemaBytes, JNI_ABORT);
      env->ReleaseByteArrayElements(exprsArr, exprsBytes, JNI_ABORT);
      goto out;
    }

    exprVector.push_back(root);
    retTypes.push_back(retType);
  }

  InitMemoryPool();
  // good to invoke the evaluator now
  status = Projector::Make(schemaPtr, exprVector, pool_, &projector);
  if (!status.ok()) {
    env->ReleaseByteArrayElements(schemaArr, schemaBytes, JNI_ABORT);
    env->ReleaseByteArrayElements(exprsArr, exprsBytes, JNI_ABORT);
    goto out;
  }

  // store the result in a map
  moduleID = projectorModuleId_++;
  holder = std::shared_ptr<ProjectorHolder>(new ProjectorHolder(schemaPtr,
                                                                retTypes,
                                                                std::move(projector)));
  projectorModulesMap_.insert(
    std::pair<jlong, std::shared_ptr<ProjectorHolder>>(moduleID, holder));

  env->ReleaseByteArrayElements(schemaArr, schemaBytes, JNI_ABORT);
  env->ReleaseByteArrayElements(exprsArr, exprsBytes, JNI_ABORT);
out:
  return moduleID;
}

JNIEXPORT void JNICALL Java_org_apache_arrow_gandiva_evaluator_NativeBuilder_evaluate
  (JNIEnv *env, jclass cls,
   jlong moduleID,
   jlongArray bufAddrs, jlongArray bufSizes,
   jlongArray outValidityAddrs, jlongArray outValueAddrs) {
  std::map<jlong, std::shared_ptr<ProjectorHolder>>::iterator it;

  it = projectorModulesMap_.find(moduleID);
  if (it == projectorModulesMap_.end()) {
    // TODO: Need to handle this. Invalid module id
    return;
  }

  jsize num_rows = env->GetArrayLength(bufAddrs);
  jlong *inBufAddrs = env->GetLongArrayElements(bufAddrs, 0);
  jlong *inBufSizes = env->GetLongArrayElements(bufSizes, 0);

  std::shared_ptr<ProjectorHolder> holder = it->second;
  auto schema = holder->schema();
  std::vector<std::shared_ptr<arrow::ArrayData>> columns;
  auto numFields = schema->num_fields();
  int memBufIdx = 0;
  int memSzIdx = 0;

  for (int i = 0; i < numFields; i++) {
    auto field = schema->field(i);
    auto buf_addr = reinterpret_cast<uint8_t *>(inBufAddrs[memBufIdx++]);
    auto validity_addr = reinterpret_cast<uint8_t *>(inBufAddrs[memBufIdx++]);

    auto validity = std::shared_ptr<arrow::Buffer>(
      new arrow::Buffer(buf_addr, inBufSizes[memSzIdx++]));
    auto data = std::shared_ptr<arrow::Buffer>(
      new arrow::Buffer(validity_addr, inBufSizes[memSzIdx++]));

    auto arrayData = arrow::ArrayData::Make(field->type(), num_rows, {validity, data});
    columns.push_back(arrayData);
  }

  auto in_batch = arrow::RecordBatch::Make(schema, num_rows, columns);
  auto outputs = holder->projector()->Evaluate(*in_batch);

  env->ReleaseLongArrayElements(bufAddrs, inBufAddrs, JNI_ABORT);
  env->ReleaseLongArrayElements(bufSizes, inBufSizes, JNI_ABORT);
}

JNIEXPORT void JNICALL Java_org_apache_arrow_gandiva_evaluator_NativeBuilder_close
  (JNIEnv *env, jclass cls, jlong moduleID) {
  std::map<jlong, std::shared_ptr<ProjectorHolder>>::iterator it;

  it = projectorModulesMap_.find(moduleID);
  if (it == projectorModulesMap_.end()) {
    // TODO: Closing an already closed module
    // throw an exception
    return;
  }

  // remove holder from the map
  projectorModulesMap_.erase(moduleID);
}
