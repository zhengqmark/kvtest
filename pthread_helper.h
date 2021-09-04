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
#pragma once

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

namespace port {

inline void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "Error performing %s: %s\n", label, strerror(result));
    abort();
  }
}

class CondVar;
class Mutex {
 public:
  Mutex() { PthreadCall("pthread_mutex_init", pthread_mutex_init(&mu_, NULL)); }
  void Lock() { PthreadCall("pthread_mutex_lock", pthread_mutex_lock(&mu_)); }
  void Unlock() {
    PthreadCall("pthread_mutex_unlock", pthread_mutex_unlock(&mu_));
  }
  ~Mutex() {
    PthreadCall("pthread_mutex_destroy", pthread_mutex_destroy(&mu_));
  }

 private:
  friend class CondVar;
  // No copying
  void operator=(const Mutex& mu);
  Mutex(const Mutex&);

  pthread_mutex_t mu_;
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(&mu->mu_) {
    PthreadCall("pthread_cond_init", pthread_cond_init(&cv_, NULL));
  }
  void Wait() {
    PthreadCall("pthread_cond_wait", pthread_cond_wait(&cv_, mu_));
  }
  void SignalAll() {
    PthreadCall("pthread_cond_broadcast", pthread_cond_broadcast(&cv_));
  }
  ~CondVar() {
    PthreadCall("pthread_cond_destroy", pthread_cond_destroy(&cv_));
  }

 private:
  // No copying
  void operator=(const CondVar& cv);
  CondVar(const CondVar&);

  pthread_mutex_t* const mu_;
  pthread_cond_t cv_;
};
}  // namespace port

class MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) : mu_(mu) { mu_->Lock(); }
  ~MutexLock() { mu_->Unlock(); }

 private:
  // No copying allowed
  void operator=(const MutexLock& ml);
  MutexLock(const MutexLock&);

  port::Mutex* const mu_;
};
