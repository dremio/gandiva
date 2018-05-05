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

#include <unistd.h>
#include "llvm_generator.h"

int main(int argc, char *argv[]) {
  bool optimise_ir = false;
  bool trace_ir = true;
  int opt;

  while ((opt = getopt(argc, argv, "o:t:")) != -1) {
    switch (opt) {
      case 'o':
        // optimise
        if (optarg) {
          optimise_ir = (atoi(optarg) ? true : false);
        }
        break;
      case 't':
        // trace
        if (optarg) {
          trace_ir = (atoi(optarg) ? true : false);
        }
        break;
      default:
        fprintf(stderr, "usage: %s [-o 0/1] [-t 0/1]", argv[0]);
        exit(1);
    }
  }

  int ret = gandiva::LLVMGenerator::ReproReplay(optimise_ir, trace_ir);
  printf("replay %s\n", ret ? "failed" : "completed successfully");
  exit(ret);
}
