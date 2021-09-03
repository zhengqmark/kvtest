/*
 * Copyright (c) 2021 Triad National Security, LLC, as operator of Los Alamos
 * National Laboratory with the U.S. Department of Energy/National Nuclear
 * Security Administration. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * BSD LICENSE
 *
 * Copyright (c) 2018 Samsung Electronics Co., Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Samsung Electronics Co., Ltd. nor the names of
 *     its contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "fnv.h"
#include "pliops_port.h"
#include "random_val_gen.h"

#include <getopt.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>

class KVTest {
 public:
  KVTest(size_t klen, size_t vlen, int n) : klen_(klen), vlen_(vlen), n_(n) {
    PrepareKeys();
  }
  ~KVTest() {}

  struct ThreadArg {
    ThreadArg() : pid(0), t(NULL), ops(0), ops_per_thread(0), id(0) {}
    pthread_t pid;
    const KVTest* t;
    uint32_t ops;
    int ops_per_thread;
    int id;
  };

  static void* DoPuts(void* input) {
    ThreadArg* const arg = static_cast<ThreadArg*>(input);
    const char* k = &arg->t->keybuf_[arg->id * arg->ops_per_thread];
    RandomValueGenerator val(1 + arg->id);
    size_t vlen = arg->t->vlen_;
    for (int i = 0; i < arg->ops_per_thread; i++) {
      int ret =
          port::PliopsPutCommand(k, arg->t->klen_, val.Generate(vlen), vlen);
      if (ret != 0) {
        //
      }
      k += arg->t->klen_;
      arg->ops++;
    }
    return NULL;
  }

  static inline void JoinAll(int n, ThreadArg const* args) {
    for (int i = 0; i < n; i++) {
      pthread_join(args[i].pid, NULL);
    }
  }

  void Run(int j) {
    std::vector<ThreadArg> threads;
    threads.resize(j);
    for (int i = 0; i < j; i++) {
      ThreadArg* const arg = &threads[i];
      arg->t = this;
      arg->ops_per_thread = n_ / j;
      arg->id = i;
      int r = pthread_create(&arg->pid, NULL, DoPuts, arg);
      if (r != 0) {
        //
      }
    }
    JoinAll(j, &threads[0]);
    // Done!!
  }

 private:
  std::string keybuf_;
  size_t klen_;
  size_t vlen_;
  // Total ops across threads
  int n_;

  void PrepareKeys() {
    char tmp[17];
    keybuf_.resize(n_ * klen_, 0);
    char* k = &keybuf_[0];
    for (int i = 0; i < n_; i++) {
      uint64_t h = FNVHash64(i);
      sprintf(tmp, "%016llx", h);
      memcpy(k, tmp, klen_);
      k += klen_;
    }
  }
};

static void usage(char* argv0, const char* msg) {
  if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
  fprintf(stderr, "==============\n");
  fprintf(stderr, "Usage: %s [-n num_ops] [-k klen] [-v vlen] [-t threads]\n",
          argv0);
  fprintf(stderr,
          "-n      num_ops      :  total number of ops (across all threads)\n");
  fprintf(stderr, "-k      klen         :  key length\n");
  fprintf(stderr, "-v      vlen         :  value length\n");
  fprintf(stderr, "-t      threads      :  number of threads\n");
  fprintf(stderr, "==============\n");
  exit(EXIT_FAILURE);
}

int main(int argc, char* argv[]) {
  int c;
  int klen = 16;
  int vlen = 64;
  int n = 8;
  int j = 1;

  /* we want lines!! */
  setlinebuf(stdout);

  while ((c = getopt(argc, argv, "n:k:v:t:j:h")) != -1) {
    switch (c) {
      case 'n':
        n = atoi(optarg);
        if (n < 0) usage(argv[0], "bad num ops");
        break;
      case 'k':
        klen = atoi(optarg);
        if (klen < 1) usage(argv[0], "bad k len");
        break;
      case 'v':
        vlen = atoi(optarg);
        if (vlen < 1) usage(argv[0], "bad v len");
        break;
      case 't':
      case 'j':
        j = atoi(optarg);
        if (j < 1) usage(argv[0], "bad thread num");
        break;
      case 'h':
      default:
        usage(argv[0], NULL);
    }
  }

  KVTest test(klen, vlen, n);
  test.Run(j);

  return 0;
}