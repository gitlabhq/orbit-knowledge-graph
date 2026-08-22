//! Routes tree-sitter's C allocations through the Rust global allocator.
//!
//! tree-sitter calls libc `malloc` directly, so parse trees and their scratch
//! buffers are invisible to a `GlobalAlloc` wrapper and to dhat alike: every
//! reading of "live heap" silently excludes them. Pointing it at
//! [`std::alloc`] puts them back on the same books as everything else. Each
//! block carries a header holding its size, because Rust's deallocation needs
//! the layout that C's `free` does not pass back.

use std::alloc::Layout;
use std::ffi::c_void;

const HEADER: usize = 16;
const ALIGN: usize = 16;

#[allow(unsafe_code)]
unsafe fn layout_for(size: usize) -> Layout {
    unsafe { Layout::from_size_align_unchecked(size + HEADER, ALIGN) }
}

/// C's contract is a null return on a size that cannot be represented. Wrapping
/// instead would allocate a few bytes and hand back a pointer past their end.
fn checked_total(size: usize) -> Option<usize> {
    size.checked_add(HEADER)
}

#[allow(unsafe_code)]
unsafe fn attach(base: *mut u8, size: usize) -> *mut c_void {
    if base.is_null() {
        return std::ptr::null_mut();
    }
    unsafe {
        base.cast::<usize>().write(size);
        base.add(HEADER).cast()
    }
}

#[allow(unsafe_code)]
unsafe fn detach(ptr: *mut c_void) -> (*mut u8, usize) {
    unsafe {
        let base = ptr.cast::<u8>().sub(HEADER);
        (base, base.cast::<usize>().read())
    }
}

#[allow(unsafe_code)]
unsafe extern "C" fn ts_malloc(size: usize) -> *mut c_void {
    if checked_total(size).is_none() {
        return std::ptr::null_mut();
    }
    unsafe { attach(std::alloc::alloc(layout_for(size)), size) }
}

#[allow(unsafe_code)]
unsafe extern "C" fn ts_calloc(count: usize, size: usize) -> *mut c_void {
    let Some(total) = count
        .checked_mul(size)
        .filter(|t| checked_total(*t).is_some())
    else {
        return std::ptr::null_mut();
    };
    unsafe { attach(std::alloc::alloc_zeroed(layout_for(total)), total) }
}

#[allow(unsafe_code)]
unsafe extern "C" fn ts_realloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    if ptr.is_null() {
        return unsafe { ts_malloc(size) };
    }
    if checked_total(size).is_none() {
        return std::ptr::null_mut();
    }
    unsafe {
        let (base, old) = detach(ptr);
        attach(
            std::alloc::realloc(base, layout_for(old), size + HEADER),
            size,
        )
    }
}

#[allow(unsafe_code)]
unsafe extern "C" fn ts_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let (base, old) = detach(ptr);
        std::alloc::dealloc(base, layout_for(old));
    }
}

/// Must run before anything parses, since blocks handed out by libc `malloc`
/// cannot be returned to the Rust allocator.
#[allow(unsafe_code)]
pub fn install() {
    unsafe {
        tree_sitter::set_allocator(
            Some(ts_malloc),
            Some(ts_calloc),
            Some(ts_realloc),
            Some(ts_free),
        );
    }
}
