#![allow(non_camel_case_types)]

use pgrx::*;
use pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};

extension_sql!(
    "\n\
        CREATE SCHEMA toolkit_experimental;\n\
    ",
    name = "create_experimental_schema",
    creates = [
        Type(bytea),
        Type(text),
        Type(TimestampTz),
        Type(AnyElement),
        Type(tstzrange),
        Type(Interval),
        Type(regproc)
    ],
    bootstrap,
);

// TODO temporary holdover types while we migrate from nominal types to actual

macro_rules! raw_type {
    ($name:ident, $tyid: path, $arrayid: path) => {
        impl FromDatum for $name {
            unsafe fn from_polymorphic_datum(
                datum: pgrx::pg_sys::Datum,
                is_null: bool,
                _typoid: pgrx::pg_sys::Oid,
            ) -> Option<Self>
            where
                Self: Sized,
            {
                if is_null {
                    return None;
                }
                Some(Self(datum))
            }
        }

        impl IntoDatum for $name {
            fn into_datum(self) -> Option<pgrx::pg_sys::Datum> {
                Some(self.0)
            }
            fn type_oid() -> pgrx::pg_sys::Oid {
                $tyid
            }
            fn array_type_oid() -> pgrx::pg_sys::Oid {
                $arrayid
            }
        }

        impl From<pgrx::pg_sys::Datum> for $name {
            fn from(d: pgrx::pg_sys::Datum) -> Self {
                Self(d)
            }
        }

        impl From<$name> for pgrx::pg_sys::Datum {
            fn from(v: $name) -> Self {
                v.0
            }
        }

        // SAFETY: all calls to raw_type! use type names that are valid SQL
        unsafe impl SqlTranslatable for $name {
            fn argument_sql() -> Result<SqlMapping, ArgumentError> {
                Ok(SqlMapping::literal(stringify!($name)))
            }
            fn return_sql() -> Result<Returns, ReturnsError> {
                Ok(Returns::One(SqlMapping::literal(stringify!($name))))
            }
        }
    };
}

#[derive(Clone, Copy)]
pub struct bytea(pub pgrx::pg_sys::Datum);

raw_type!(bytea, pgrx::pg_sys::BYTEAOID, pgrx::pg_sys::BYTEAARRAYOID);

#[derive(Clone, Copy)]
pub struct text(pub pgrx::pg_sys::Datum);

raw_type!(text, pgrx::pg_sys::TEXTOID, pgrx::pg_sys::TEXTARRAYOID);

pub struct TimestampTz(pub pgrx::pg_sys::Datum);

raw_type!(
    TimestampTz,
    pgrx::pg_sys::TIMESTAMPTZOID,
    pgrx::pg_sys::TIMESTAMPTZARRAYOID
);

impl From<TimestampTz> for pgrx::pg_sys::TimestampTz {
    fn from(tstz: TimestampTz) -> Self {
        tstz.0.value() as _
    }
}

impl From<pgrx::pg_sys::TimestampTz> for TimestampTz {
    fn from(ts: pgrx::pg_sys::TimestampTz) -> Self {
        Self(pgrx::pg_sys::Datum::from(ts))
    }
}

pub struct AnyElement(pub pgrx::pg_sys::Datum);

raw_type!(AnyElement, pgrx::pg_sys::ANYELEMENTOID, pgrx::pg_sys::ANYARRAYOID);

pub struct tstzrange(pub pgrx::pg_sys::Datum);

raw_type!(tstzrange, pgrx::pg_sys::TSTZRANGEOID, pgrx::pg_sys::TSTZRANGEARRAYOID);

pub struct Interval(pub pgrx::pg_sys::Datum);

raw_type!(Interval, pgrx::pg_sys::INTERVALOID, pgrx::pg_sys::INTERVALARRAYOID);

impl From<i64> for Interval {
    fn from(interval: i64) -> Self {
        let interval = pgrx::pg_sys::Interval {
            time: interval,
            ..Default::default()
        };
        let interval = unsafe {
            let ptr =
                pgrx::pg_sys::palloc(std::mem::size_of::<pgrx::pg_sys::Interval>()) as *mut pgrx::pg_sys::Interval;
            *ptr = interval;
            Interval(pgrx::pg_sys::Datum::from(ptr))
        };
        // Now we have a valid Interval in at least one sense.  But we have the
        // microseconds in the `time` field and `day` and `month` are both 0,
        // which is legal.  However, directly converting one of these to TEXT
        // comes out quite ugly if the number of microseconds is greater than 1 day:
        //   8760:02:00
        // Should be:
        //   365 days 00:02:00
        // How does postgresql do it?  It happens in src/backend/utils/adt/timestamp.c:timestamp_mi:
        //  result->time = dt1 - dt2;
        //  result = DatumGetIntervalP(DirectFunctionCall1(interval_justify_hours,
        //                                                 IntervalPGetDatum(result)));
        // So if we want the same behavior, we need to call interval_justify_hours too:
        let function_args = vec![Some(pgrx::pg_sys::Datum::from(interval))];
        unsafe { pgrx::direct_function_call(pgrx::pg_sys::interval_justify_hours, &function_args) }
            .expect("interval_justify_hours does not return None")
    }
}

pub struct regproc(pub pgrx::pg_sys::Datum);

raw_type!(regproc, pgrx::pg_sys::REGPROCOID, pgrx::pg_sys::REGPROCARRAYOID);
