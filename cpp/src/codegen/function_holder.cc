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

#include "codegen/function_holder.h"

#include <regex>
#include "codegen/like_holder.h"
#include "codegen/node.h"

namespace gandiva {

Status FunctionHolder::Make(const std::string &name, const FunctionNode &node,
                            FunctionHolderPtr *holder) {
  if (name.compare("like") == 0) {
    std::shared_ptr<LikeHolder> like_holder;
    auto status = LikeHolder::Make(node, &like_holder);
    GANDIVA_RETURN_NOT_OK(status);

    *holder = static_cast<FunctionHolderPtr>(like_holder);
    return Status::OK();
  } else {
    return Status::Invalid("unknown function " + name);
  }
}

}  // namespace gandiva
