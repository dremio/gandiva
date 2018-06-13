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

#include "codegen/stop_watch.h"

#include <chrono>
#include <thread>
#include <gtest/gtest.h>

namespace gandiva {

TEST(TestStopWatch, Basic) {
  StopWatch timer;

  const int kMicrosPerIter = 10000;
  int iter = 0;
  for (; iter < 100; ++iter) {
    timer.Start();
    std::this_thread::sleep_for(std::chrono::microseconds(kMicrosPerIter));
    timer.Stop();
  }

  double tolerance = 0.25;
  EXPECT_GE(timer.ElapsedMicros(), (1 - tolerance) * iter * kMicrosPerIter);
  EXPECT_LE(timer.ElapsedMicros(), (1 + tolerance) * iter * kMicrosPerIter);
}

} // namespace gandiva
