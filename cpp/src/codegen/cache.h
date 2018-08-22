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

#ifndef GANDIVA_FILTER_CACHE_H
#define GANDIVA_FILTER_CACHE_H

#include "codegen/filter_cache_key.h"
#include "codegen/lru_cache.h"
#include "codegen/projector_cache_key.h"
#include "gandiva/arrow.h"

namespace gandiva {

template <class KeyType, typename ValueType>
class Cache {
 public:
  static ValueType GetCachedModule(KeyType cache_key) {
    boost::optional<ValueType> result;
    result = cache_.get(cache_key);
    if (result != boost::none) {
      return result.value();
    }
    mtx_.lock();
    result = cache_.get(cache_key);
    mtx_.unlock();
    return result != boost::none ? result.value() : nullptr;
  }

  static void CacheModule(KeyType cache_key, ValueType projector) {
    mtx_.lock();
    cache_.insert(cache_key, projector);
    mtx_.unlock();
  }
  static lru_cache<KeyType, ValueType> cache_;
  static std::mutex mtx_;
};

class FilterCache : public Cache<FilterCacheKey, std::shared_ptr<Filter>> {
 public:
  using Cache::cache_;
  using Cache<FilterCacheKey, std::shared_ptr<Filter>>::mtx_;
};

class ProjectorCache : public Cache<ProjectorCacheKey, std::shared_ptr<Projector>> {};
}  // namespace gandiva
#endif  // GANDIVA_FILTER_CACHE_H
