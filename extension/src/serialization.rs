pub use self::collations::PgCollationId;
pub use self::functions::PgProcId;
pub use self::types::{PgTypId, ShortTypeId};
use std::{
    convert::TryInto,
    os::raw::{c_char, c_int},
};

use pgrx::pg_sys;
use std::ffi::CStr;

pub(crate) mod collations;
mod functions;
mod types;

// basically timestamptz_out
#[no_mangle]
pub extern "C" fn _ts_toolkit_encode_timestamptz(
    dt: pgrx::pg_sys::TimestampTz,
    buf: &mut [c_char; pgrx::pg_sys::MAXDATELEN as _],
) {
    let mut tz: c_int = 0;
    let mut tt: pgrx::pg_sys::pg_tm = unsafe { std::mem::MaybeUninit::zeroed().assume_init() };
    let mut fsec = 0;
    let mut tzn = std::ptr::null();
    unsafe {
        if dt == pgrx::pg_sys::TimestampTz::MAX || dt == pgrx::pg_sys::TimestampTz::MIN {
            return pgrx::pg_sys::EncodeSpecialTimestamp(dt, buf.as_mut_ptr());
        }
        let err = pgrx::pg_sys::timestamp2tm(
            dt,
            &mut tz,
            &mut tt,
            &mut fsec,
            &mut tzn,
            std::ptr::null_mut(),
        );
        if err != 0 {
            panic!("timestamp out of range")
        }
        pgrx::pg_sys::EncodeDateTime(
            &mut tt,
            fsec,
            true,
            tz,
            tzn,
            pgrx::pg_sys::DateStyle,
            buf.as_mut_ptr(),
        )
    }
}

#[no_mangle]
// this is only going to be used to communicate with a rust lib we compile with this one
#[allow(improper_ctypes_definitions)]
pub extern "C" fn _ts_toolkit_decode_timestamptz(text: &str) -> i64 {
    use std::{ffi::CString, mem::MaybeUninit, ptr};
    let str = CString::new(text).unwrap();
    unsafe {
        let mut fsec = 0;
        let mut tt = MaybeUninit::zeroed().assume_init();
        let tm = &mut tt;
        let mut tz = 0;
        let mut dtype = 0;
        let mut nf = 0;
        let mut field = [ptr::null_mut(); pgrx::pg_sys::MAXDATEFIELDS as _];
        let mut ftype = [0; pgrx::pg_sys::MAXDATEFIELDS as _];
        let mut workbuf = [0; pgrx::pg_sys::MAXDATELEN as usize + pgrx::pg_sys::MAXDATEFIELDS as usize];
        let mut dterr = pgrx::pg_sys::ParseDateTime(
            str.as_ptr(),
            workbuf.as_mut_ptr(),
            workbuf.len(),
            field.as_mut_ptr(),
            ftype.as_mut_ptr(),
            pgrx::pg_sys::MAXDATEFIELDS as i32,
            &mut nf,
        );
        if dterr == 0 {
            dterr = pgrx::pg_sys::DecodeDateTime(
                field.as_mut_ptr(),
                ftype.as_mut_ptr(),
                nf,
                &mut dtype,
                tm,
                &mut fsec,
                &mut tz,
            )
        }
        if dterr != 0 {
            pgrx::pg_sys::DateTimeParseError(
                dterr,
                str.as_ptr(),
                b"timestamptz\0".as_ptr().cast::<c_char>(),
            );
            return 0;
        }

        match dtype as u32 {
            pgrx::pg_sys::DTK_DATE => {
                let mut result = 0;
                let err = pgrx::pg_sys::tm2timestamp(tm, fsec, &mut tz, &mut result);
                if err != 0 {
                    // TODO pgx error with correct errcode?
                    panic!("timestamptz \"{}\" out of range", text)
                }
                result
            }
            pgrx::pg_sys::DTK_EPOCH => pgrx::pg_sys::SetEpochTimestamp(),
            pgrx::pg_sys::DTK_LATE => pgrx::pg_sys::TimestampTz::MAX,
            pgrx::pg_sys::DTK_EARLY => pgrx::pg_sys::TimestampTz::MIN,
            _ => panic!(
                "unexpected result {} when parsing timestamptz \"{}\"",
                dtype, text
            ),
        }
    }
}

pub enum EncodedStr<'s> {
    Utf8(&'s str),
    Other(&'s CStr),
}

pub fn str_to_db_encoding(s: &str) -> EncodedStr {
    if unsafe { pgrx::pg_sys::GetDatabaseEncoding() == pgrx::pg_sys::pg_enc_PG_UTF8 as i32 } {
        return EncodedStr::Utf8(s);
    }

    let bytes = s.as_bytes();
    let encoded = unsafe {
        pgrx::pg_sys::pg_any_to_server(
            bytes.as_ptr() as *const c_char,
            bytes.len().try_into().unwrap(),
            pgrx::pg_sys::pg_enc_PG_UTF8 as _,
        )
    };
    if encoded as usize == bytes.as_ptr() as usize {
        return EncodedStr::Utf8(s);
    }

    let cstr = unsafe { CStr::from_ptr(encoded) };
    EncodedStr::Other(cstr)
}

pub fn str_from_db_encoding(s: &CStr) -> &str {
    if unsafe { pgrx::pg_sys::GetDatabaseEncoding() == pgrx::pg_sys::pg_enc_PG_UTF8 as i32 } {
        return s.to_str().unwrap();
    }

    let str_len = s.to_bytes().len().try_into().unwrap();
    let encoded =
        unsafe { pgrx::pg_sys::pg_server_to_any(s.as_ptr(), str_len, pgrx::pg_sys::pg_enc_PG_UTF8 as _) };
    if encoded as usize == s.as_ptr() as usize {
        //TODO redundant check?
        return s.to_str().unwrap();
    }
    return unsafe { CStr::from_ptr(encoded).to_str().unwrap() };
}

pub(crate) mod serde_reference_adaptor {
    pub(crate) fn default_padding() -> [u8; 3] {
        [0; 3]
    }

    pub(crate) fn default_header() -> u32 {
        0
    }
}
