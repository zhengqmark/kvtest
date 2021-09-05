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
#include "pthread_helper.h"
#include "random_val_gen.h"

#include <assert.h>
#include <getopt.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

class KVTest {
 private:
  const bool stop_on_err_;
  std::string keybuf_;
  size_t klen_;
  size_t vlen_;
  int n_;  // Total number of keys for PUT/GET operations

 public:
  // State shared by all concurrent worker threads of a test run
  struct SharedState {
    SharedState(int j)
        : condvar(&mu),
          total(j),
          num_initialized(0),
          num_done(0),
          is_mon_running(false),
          start(false) {}
    port::Mutex mu;
    port::CondVar condvar;
    const int total;  // Total number of worker threads
    // Each worker thread goes through the following states:
    //    (1) initializing
    //    (2) waiting for others to be initialized
    //    (3) running
    //    (4) done
    int num_initialized;
    int num_done;
    bool is_mon_running;  // True if the mon thread is running
    bool start;
  };

  struct ThreadState {
    ThreadState()
        : shared_state(NULL),
          t(NULL),
          start(0),
          finish(0),
          err_ops(0),
          ops(0),
          ops_per_thread(0),
          id(0) {}
    SharedState* shared_state;
    const KVTest* t;
    uint64_t start;
    uint64_t finish;
    uint32_t err_ops;
    uint32_t ops;
    int ops_per_thread;
    int id;
  };

  static void DoPuts(ThreadState* state) {
    const char* k =
        &state->t->keybuf_[state->id * state->ops_per_thread * state->t->klen_];
    RandomValueGenerator val(1 + state->id);
    size_t vlen = state->t->vlen_;
    for (int i = 0; i < state->ops_per_thread; i++) {
      int ret =
          port::PliopsPutCommand(k, state->t->klen_, val.Generate(vlen), vlen);
      if (ret != 0) {
        fprintf(stderr, "Error executing PUT\n");
        state->err_ops++;
        if (state->t->stop_on_err_) {
          break;
        }
      }
      k += state->t->klen_;
      state->ops++;
    }
  }

  static void CheckData(ThreadState* state) {
    std::string buf;
    const char* k =
        &state->t->keybuf_[state->id * state->ops_per_thread * state->t->klen_];
    RandomValueGenerator val(1 + state->id);
    size_t vlen = state->t->vlen_;
    buf.resize(vlen);
    for (int i = 0; i < state->ops_per_thread; i++) {
      uint32_t object_size;
      int ret = port::PliopsGetCommand(k, state->t->klen_, &buf[0], vlen,
                                       object_size);
      if (ret != 0) {
        fprintf(stderr, "Error executing GET\n");
        state->err_ops++;
        if (state->t->stop_on_err_) {
          break;
        }
      } else if (object_size != vlen) {
        fprintf(stderr, "Bad object size: key=%d\n",
                state->id * state->ops_per_thread + i);
        state->err_ops++;
        if (state->t->stop_on_err_) {
          break;
        }
      } else if (memcmp(&buf[0], val.Generate(vlen), vlen) != 0) {
        fprintf(stderr, "Bad data: key=%d\n",
                state->id * state->ops_per_thread + i);
        state->err_ops++;
        if (state->t->stop_on_err_) {
          break;
        }
      }
      k += state->t->klen_;
      state->ops++;
    }
  }

  // Return the current time in microseconds.
  static inline uint64_t CurrentMicros() {
    uint64_t result;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    result = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    return result;
  }

  struct ThreadArg {
    void (*method)(ThreadState*);
    ThreadState state;
    pthread_t pid;
  };

  static void* ThreadBody(void* input) {
    ThreadState* const thread = &static_cast<ThreadArg*>(input)->state;
    SharedState* const shared = thread->shared_state;
    {
      MutexLock ml(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->condvar.SignalAll();
      }
      while (!shared->start) {
        shared->condvar.Wait();
      }
    }

    thread->start = CurrentMicros();
    static_cast<ThreadArg*>(input)->method(thread);
    thread->finish = CurrentMicros();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->condvar.SignalAll();
      }
    }
    return NULL;
  }

  struct MonitorArg {
    const ThreadState** states;
    SharedState* shared;
    pthread_t pid;
    uint32_t output_interval;
    FILE* output;
  };

  static void OutputStats(const MonitorArg* arg, uint64_t time) {
    uint64_t total_err_ops = 0;
    uint64_t total_ops = 0;
    for (int i = 0; i < arg->shared->total; i++) {
      total_err_ops += arg->states[i]->err_ops;
      total_ops += arg->states[i]->ops;
    }
    fprintf(arg->output, "%.3f,%llu,%llu", time / 1000. / 1000.,
            static_cast<unsigned long long>(total_ops),
            static_cast<unsigned long long>(total_err_ops));
    if (arg->output == stdout || arg->output == stderr) {
      fprintf(arg->output, ",%.3f\n",
              1000. * 1000. * double(total_ops) / double(time));
    } else {
      fputc('\n', arg->output);
    }
  }

  static void* MonitorBody(void* input) {
    MonitorArg* const arg = static_cast<MonitorArg*>(input);
    SharedState* const shared = arg->shared;
    MutexLock ml(&shared->mu);
    shared->is_mon_running = true;
    shared->condvar.SignalAll();
    while (!shared->start) {
      shared->condvar.Wait();
    }
    const uint64_t begin = CurrentMicros();
    while (shared->num_done < shared->total) {
      shared->mu.Unlock();
      uint64_t relative_time = CurrentMicros() - begin;
      OutputStats(arg, relative_time);
      usleep(arg->output_interval);
      shared->mu.Lock();
    }
    uint64_t d = CurrentMicros() - begin;
    OutputStats(arg, d);
    shared->is_mon_running = false;
    shared->condvar.SignalAll();
    return NULL;
  }

  void Run(void (*method)(ThreadState*), int j) {
    OpenDB();
    const uint64_t begin = CurrentMicros();
    SharedState shared(j);
    std::vector<const ThreadState*> per_thread_state;
    std::vector<ThreadArg> threads;
    threads.resize(j);
    for (int i = 0; i < j; i++) {
      ThreadArg* const arg = &threads[i];
      arg->method = method;
      ThreadState* const thread = &arg->state;
      thread->shared_state = &shared;
      thread->t = this;
      thread->ops_per_thread = n_ / j;
      thread->id = i;
      per_thread_state.push_back(thread);
      port::PthreadCall(
          "pthread_create",
          pthread_create(&arg->pid, NULL, KVTest::ThreadBody, arg));
      port::PthreadCall("pthread_detach", pthread_detach(arg->pid));
    }
    MonitorArg mon_arg;
    mon_arg.states = &per_thread_state[0];
    mon_arg.shared = &shared;
    mon_arg.output_interval = 2 * 1000 * 1000;
    mon_arg.output = stdout;
    port::PthreadCall(
        "pthread_create",
        pthread_create(&mon_arg.pid, NULL, KVTest::MonitorBody, &mon_arg));
    port::PthreadCall("pthread_detach", pthread_detach(mon_arg.pid));
    {
      MutexLock l(&shared.mu);
      while (!shared.is_mon_running) {
        shared.condvar.Wait();
      }
      while (shared.num_initialized < shared.total) {
        shared.condvar.Wait();
      }
      shared.start = true;
      shared.condvar.SignalAll();
      while (shared.num_done < shared.total) {
        shared.condvar.Wait();
      }
      while (shared.is_mon_running) {
        shared.condvar.Wait();
      }
    }
    const uint64_t end = CurrentMicros();
    Shutdown();
    // Summary
    uint64_t g_start = end;
    for (int i = 0; i < j; i++) {
      if (threads[i].state.start < g_start) {
        g_start = threads[i].state.start;
      }
    }
    uint64_t g_finish = begin;
    for (int i = 0; i < j; i++) {
      if (threads[i].state.finish > g_finish) {
        g_finish = threads[i].state.finish;
      }
    }
    assert(g_finish >= g_start);
    uint64_t total_elapsed = g_finish - g_start;
    uint64_t total_ops = 0;
    for (int i = 0; i < j; i++) total_ops += threads[i].state.ops;
    fprintf(stdout, "== Total elapsed time: %.3f s\n",
            double(total_elapsed) / 1000. / 1000.);
    fprintf(stdout, "== Total Ops: %llu\n",
            static_cast<unsigned long long>(total_ops));
    fprintf(stdout, "== Tput: %.3f op/s\n",
            1000. * 1000. * double(total_ops) / double(total_elapsed));
    fprintf(stdout, "Benchmark finished in %.3f s\n",
            double(end - begin) / 1000. / 1000.);
    // Done!!
  }

  KVTest(size_t klen, size_t vlen, int n)
      : stop_on_err_(false), klen_(klen), vlen_(vlen), n_(n) {
    PrepareKeys();
  }
  ~KVTest() {}

 private:
  void OpenDB() {
    if (!port::PliopsOpenDB(0)) {
      fprintf(stderr, "Cannot open db!!!\n");
      abort();
    }
  }

  void Shutdown() {
    if (!port::PliopsCloseDB()) {
      fprintf(stderr, "Error closing db\n");
    }
  }

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
  fprintf(stderr,
          "usage: %s [-w] [-c] [-n num_ops] [-k klen] [-v vlen] [-t threads]\n",
          argv0);
  fprintf(stderr, "-w      writes       :  do PUTS\n");
  fprintf(stderr, "-c      checks       :  do data checks\n");
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
  int writes = 0;
  int data_checks = 0;
  int klen = 16;
  int vlen = 64;
  int n = 8;
  int j = 2;

  /* we want lines!! */
  setlinebuf(stdout);

  while ((c = getopt(argc, argv, "n:k:v:t:j:wch")) != -1) {
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
      case 'w':
        writes = 1;
        break;
      case 'c':
        data_checks = 1;
        break;
      case 'h':
      default:
        usage(argv[0], NULL);
    }
  }

  KVTest test(klen, vlen, n);
  if (writes != 0) test.Run(KVTest::DoPuts, j);
  if (data_checks != 0) test.Run(KVTest::CheckData, j);
  fprintf(stdout, "Done!\n");

  return 0;
}
