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

#ifndef ERROR_HOLDER_H
#define ERROR_HOLDER_H

#include <string>

namespace gandiva {
/// Error holder for errors during llvm module execution
class ErrorHolder{
 public:
  std::string error_msg() const { return error_msg_;}

  void set_error_msg(std::string error_msg) {
    if (error_msg_.empty()) {
      error_msg_ = error_msg;
    }
  }
 private:
  std::string error_msg_;
};

}  // namespace gandiva
#endif // ERROR_HOLDER_H
