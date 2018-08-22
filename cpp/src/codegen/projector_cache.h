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

#ifndef GANDIVA_PROJECTOR_CACHE_H
#define GANDIVA_PROJECTOR_CACHE_H

#include "boost/functional/hash.hpp"
#include "codegen/lru_cache.h"
#include "gandiva/arrow.h"
#include "gandiva/projector.h"

namespace gandiva {

class ProjectorCache {
 public:
  class ProjectorCacheKey {
   public:
    ProjectorCacheKey(SchemaPtr schema, std::shared_ptr<Configuration> configuration,
                      ExpressionVector expression_vector)
        : schema_(schema), configuration_(configuration) {
      static const int kSeedValue = 4;
      size_t result = kSeedValue;
      for (auto &expr : expression_vector) {
        std::string expr_as_string = expr->ToString();
        expressions_as_strings.push_back(expr_as_string);
        boost::hash_combine(result, expr_as_string);
      }
      boost::hash_combine(result, configuration);
      boost::hash_combine(result, schema_->ToString());
      hash_code = result;
    }

    std::size_t Hash() const { return hash_code; }

    bool operator==(const ProjectorCacheKey &other) const {
      // arrow schema does not overload equality operators.
      if (!(schema_->Equals(*other.schema().get(), true))) {
        return false;
      }

      if (configuration_ != other.configuration_) {
        return false;
      }

      if (expressions_as_strings != other.expressions_as_strings) {
        return false;
      }
      return true;
    }

    bool operator!=(const ProjectorCacheKey &other) const { return !(*this == other); }

    SchemaPtr schema() const { return schema_; }

   private:
    const SchemaPtr schema_;
    const std::shared_ptr<Configuration> configuration_;
    std::vector<std::string> expressions_as_strings;
    size_t hash_code;
  };

  static std::shared_ptr<Projector> GetCachedProjector(ProjectorCacheKey cache_key) {
    boost::optional<std::shared_ptr<Projector>> result;
    result = cache_.get(cache_key);
    if (result != boost::none) {
      return result.value();
    }
    // mtx_.lock();
    result = cache_.get(cache_key);
    // mtx_.unlock();
    return result != boost::none ? result.value() : nullptr;
  }

  static void CacheProjector(ProjectorCacheKey cache_key,
                             std::shared_ptr<Projector> projector) {
    // mtx_.lock();
    cache_.insert(cache_key, projector);
    // mtx_.unlock();
  }

 private:
  static lru_cache<ProjectorCacheKey, std::shared_ptr<Projector>> cache_;
  // static std::mutex mtx_;
};
}  // namespace gandiva
#endif  // GANDIVA_PROJECTOR_CACHE_H
