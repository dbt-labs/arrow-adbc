// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file is inspired by the Rust standard library's
// stack-overflow handling code.
//
//     std/src/sys/pal/unix/stack_overflow.rs

#[cfg(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "hurd",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "solaris",
    target_os = "illumos",
))]
mod imp {
    use libc::{
        sigaltstack, MAP_ANON, MAP_FAILED, MAP_PRIVATE, PROT_NONE, PROT_READ, PROT_WRITE,
        SS_DISABLE,
    };
    use std::{io, mem, panic, ptr};

    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    use libc::{mmap as mmap64, mprotect, munmap};
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    use libc::{mmap64, mprotect, munmap};

    fn get_page_size() -> usize {
        unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
    }

    /// Modern kernels on modern hardware can have dynamic signal stack sizes.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn sigstack_size() -> usize {
        let dynamic_sigstksz = unsafe { libc::getauxval(libc::AT_MINSIGSTKSZ) };
        // If getauxval couldn't find the entry, it returns 0,
        // so take the higher of the "constant" and auxval.
        // This transparently supports older kernels which don't provide AT_MINSIGSTKSZ
        libc::SIGSTKSZ.max(dynamic_sigstksz as _)
    }

    /// Not all OS support hardware where this is needed.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn sigstack_size() -> usize {
        libc::SIGSTKSZ
    }

    unsafe fn alloc_stack(page_size: usize) -> libc::stack_t {
        debug_assert!(page_size > 0);
        // OpenBSD requires this flag for stack mapping
        // otherwise the said mapping will fail as a no-op on most systems
        // and has a different meaning on FreeBSD
        #[cfg(any(
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "linux",
            target_os = "dragonfly",
        ))]
        let flags = MAP_PRIVATE | MAP_ANON | libc::MAP_STACK;
        #[cfg(not(any(
            target_os = "openbsd",
            target_os = "netbsd",
            target_os = "linux",
            target_os = "dragonfly",
        )))]
        let flags = MAP_PRIVATE | MAP_ANON;

        let sigstack_size = sigstack_size();

        // Allocate memory for the signal stack plus an extra page.
        let stackp = mmap64(
            ptr::null_mut(),
            sigstack_size + page_size,
            PROT_READ | PROT_WRITE,
            flags,
            -1,
            0,
        );
        if stackp == MAP_FAILED {
            panic!(
                "failed to allocate an alternative stack: {}",
                io::Error::last_os_error()
            );
        }
        // Protect the first page of the stack so any read/write access will cause
        // a SIGSEGV. "The Old New Thing" has a good explanation of the guard page
        // concept [1].
        //
        // [1] https://devblogs.microsoft.com/oldnewthing/20220203-00/?p=106215
        let error_code = mprotect(stackp, page_size, PROT_NONE);
        if error_code != 0 {
            panic!(
                "failed to set up alternative stack guard page: {}",
                io::Error::last_os_error()
            );
        }
        let stackp = stackp.add(page_size);

        libc::stack_t {
            ss_sp: stackp,
            ss_flags: 0,
            ss_size: sigstack_size,
        }
    }

    fn block_sigurg_on_current_thread() {
        // SAFETY: sigset_t is zero-initializable
        let mut set: libc::sigset_t = unsafe { mem::zeroed() };
        // SAFETY: common sequence of libc function calls
        unsafe {
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGURG);
            libc::pthread_sigmask(libc::SIG_BLOCK, &set, ptr::null_mut());
        };
    }

    pub fn sigaltstack_enabled() -> bool {
        // SAFETY: assuming stack_t is zero-initializable
        let mut stack = unsafe { mem::zeroed() };
        // SAFETY: reads current stack_t into stack
        unsafe { sigaltstack(ptr::null(), &mut stack) };
        (stack.ss_flags & libc::SS_DISABLE) == 0
    }

    pub struct SignalStackGuard {
        /// Cached result of `sysconf(_SC_PAGESIZE)` so it doesn't need to be
        /// loaded again during drop. 0 if `data` is not allocated.
        page_size: usize,
        /// Pointer to the memory allocated for the signal stack. Null if
        /// `data` didn't have to be allocated.
        data: *mut libc::c_void,
    }

    impl SignalStackGuard {
        #[inline(never)]
        pub fn new() -> Self {
            if !sigaltstack_enabled() {
                let page_size = get_page_size();
                // This guard struct might be used in destructors, so we need to
                // make sure that we don't panic in the destructor.
                if let Ok(stack) = panic::catch_unwind(|| {
                    // SAFETY: alloc_stack() is safe
                    unsafe { alloc_stack(page_size) }
                }) {
                    // SAFETY: sigaltstack() is safe and used to set up the signal stack
                    // for the current thread. We ignore the return value here since there
                    // is nothing we can do about errors at this point and according to
                    // failure modes described in the docs it should never fail here.
                    let error_code = unsafe { sigaltstack(&stack, ptr::null_mut()) };
                    debug_assert_eq!(error_code, 0);
                    return Self {
                        page_size,
                        data: stack.ss_sp,
                    };
                }
            }
            Self {
                page_size: 0,
                data: ptr::null_mut(),
            }
        }
    }

    impl Drop for SignalStackGuard {
        #[inline(never)]
        fn drop(&mut self) {
            if self.data.is_null() {
                return;
            }
            // !data.is_null() implies page_size > 0
            debug_assert!(self.page_size > 0);
            let sigstack_size = sigstack_size();
            let disabling_stack = libc::stack_t {
                ss_sp: ptr::null_mut(),
                ss_flags: SS_DISABLE,
                // Workaround for bug in macOS implementation of sigaltstack
                // UNIX2003 which returns ENOMEM when disabling a stack while
                // passing ss_size smaller than MINSIGSTKSZ. According to POSIX
                // both ss_sp and ss_size should be ignored in this case.
                ss_size: sigstack_size,
            };
            // Why we are here? This destructor runs when a new signal stack had to
            // be allocated. That is necessary when we got unlucky in the destructor
            // of a thread-local that runs *after* the thread deallocated its signal
            // stack. So now, after the driver code has run, we make sure that
            // no SIGURG (as emitted by the Go runtime [2]) is emitted in the very
            // short time window between now and the complete tear down of the thread.
            //
            // This means any spurious requests for preemption will be ignored, which
            // is totally fine since we are in the process of shutting down the thread.
            //
            // [2] https://github.com/golang/go/blob/a1c3e2f008267b976e69866b599b113399ad4724/src/runtime/signal_unix.go#L43
            block_sigurg_on_current_thread();
            // SAFETY: disables the signal stack for the current thread
            // iff it was set up by this `SignalStackGuard` instance.
            unsafe { sigaltstack(&disabling_stack, ptr::null_mut()) };
            // SAFETY: We know from `get_stackp` that the alternate stack we
            // installed is part of a mapping that started one page earlier,
            // so walk back a page and unmap from there.
            unsafe {
                munmap(
                    self.data.sub(self.page_size),
                    sigstack_size + self.page_size,
                )
            };
        }
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "freebsd",
    target_os = "hurd",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "solaris",
    target_os = "illumos",
)))]
mod imp {
    pub struct SignalStackGuard;

    impl SignalStackGuard {
        pub fn new() -> Self {
            SignalStackGuard
        }
    }

    pub fn sigaltstack_enabled() -> bool {
        true
    }
}

#[allow(unused_imports)]
pub(crate) use imp::sigaltstack_enabled;
pub(crate) use imp::SignalStackGuard;
