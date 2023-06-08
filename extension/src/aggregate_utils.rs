use std::ptr::null_mut;

use pgrx::pg_sys;

// TODO move to func_utils once there are enough function to warrant one
pub unsafe fn get_collation(fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Option<pgrx::pg_sys::Oid> {
    if (*fcinfo).fncollation == pgrx::pg_sys::Oid::INVALID {
        None
    } else {
        Some((*fcinfo).fncollation)
    }
}

pub fn get_collation_or_default(fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Option<pgrx::pg_sys::Oid> {
    if fcinfo.is_null() {
        Some(unsafe { pgrx::pg_sys::Oid::from_u32_unchecked(100) }) // TODO: default OID, there should be a constant for this
    } else {
        unsafe { get_collation(fcinfo) }
    }
}

pub unsafe fn in_aggregate_context<T, F: FnOnce() -> T>(
    fcinfo: pgrx::pg_sys::FunctionCallInfo,
    f: F,
) -> T {
    let mctx =
        aggregate_mctx(fcinfo).unwrap_or_else(|| pgrx::error!("cannot call as non-aggregate"));
    crate::palloc::in_memory_context(mctx, f)
}

pub unsafe fn aggregate_mctx(fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Option<pgrx::pg_sys::MemoryContext> {
    if fcinfo.is_null() {
        return Some(pgrx::pg_sys::CurrentMemoryContext);
    }
    let mut mctx = null_mut();
    let is_aggregate = pgrx::pg_sys::AggCheckCallContext(fcinfo, &mut mctx);
    if is_aggregate == 0 {
        None
    } else {
        debug_assert!(!mctx.is_null());
        Some(mctx)
    }
}
