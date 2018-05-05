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
#ifndef GANDIVA_CODEGEN_EXCEPTION_H
#define GANDIVA_CODEGEN_EXCEPTION_H

#include <exception>
#include <string>

namespace gandiva {

/*
 * Exceptions from LLVMGenerator.
 * TODO : is it better to have error code instead ?
 */
class CodeGenException : public std::exception {
 public:
  CodeGenException(int error, const std::string &msg)
    : error_(error),
      error_msg_(msg) {}

  CodeGenException(const std::string &msg)
    : error_(1),
      error_msg_(msg) {}

  int error() { return error_; }
  std::string error_msg() { return error_msg_; }

 private:
  int error_;
  std::string error_msg_;
};

#endif // GANDIVA_CODEGEN_EXCEPTION_H

} // namespace gandiva
