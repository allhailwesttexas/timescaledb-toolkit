
use std::mem::replace;

use pgx::*;

use super::*;

use crate::{
    ron_inout_funcs, pg_type, build,
    stats_agg::{self, InternalStatsSummary1D, StatsSummary1D},
};


pg_type! {
    #[derive(Debug)]
    struct PipelineThenStatsAgg<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

ron_inout_funcs!(PipelineThenStatsAgg);

// hack to allow us to qualify names with "toolkit_experimental"
// so that pgx generates the correct SQL
pub mod toolkit_experimental {
    pub(crate) use super::*;
    pub(crate) use crate::accessors::*;
    varlena_type!(PipelineThenStatsAgg);
    varlena_type!(PipelineThenSum);
    varlena_type!(PipelineThenAverage);
    varlena_type!(PipelineThenNumVals);
}



#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn run_pipeline_then_stats_agg<'s, 'p>(
    mut timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::PipelineThenStatsAgg<'p>,
) -> StatsSummary1D<'static> {
    timeseries = run_pipeline_elements(timeseries, pipeline.elements.iter());
    let mut stats = InternalStatsSummary1D::new();
    for TSPoint{ val, ..} in timeseries.iter() {
        stats.accum(val).expect("error while running stats_agg");
    }
    StatsSummary1D::from_internal(stats)
}

#[pg_extern(immutable, parallel_safe, schema="toolkit_experimental")]
pub fn finalize_with_stats_agg<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenStatsAgg<'e>,
) -> toolkit_experimental::PipelineThenStatsAgg<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineThenStatsAgg {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenStatsAgg {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    name="stats_agg",
    schema="toolkit_experimental"
)]
pub fn pipeline_stats_agg<'e>() -> toolkit_experimental::PipelineThenStatsAgg<'e> {
    build! {
        PipelineThenStatsAgg {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

type Internal = usize;
#[pg_extern(
    immutable,
    parallel_safe,
    schema="toolkit_experimental"
)]
pub unsafe fn pipeline_stats_agg_support(input: Internal)
-> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| unsafe {
        let new_element = PipelineThenStatsAgg::from_datum(new_element, false, 0)
            .unwrap();
        finalize_with_stats_agg(old_pipeline, new_element).into_datum().unwrap()
    })
}

// using this instead of pg_operator since the latter doesn't support schemas yet
// FIXME there is no CREATE OR REPLACE OPERATOR need to update post-install.rs
//       need to ensure this works with out unstable warning
extension_sql!(r#"
ALTER FUNCTION toolkit_experimental."run_pipeline_then_stats_agg" SUPPORT toolkit_experimental.pipeline_stats_agg_support;

CREATE OPERATOR -> (
    PROCEDURE=toolkit_experimental."run_pipeline_then_stats_agg",
    LEFTARG=toolkit_experimental.TimeSeries,
    RIGHTARG=toolkit_experimental.PipelineThenStatsAgg
);

CREATE OPERATOR -> (
    PROCEDURE=toolkit_experimental."finalize_with_stats_agg",
    LEFTARG=toolkit_experimental.UnstableTimeseriesPipeline,
    RIGHTARG=toolkit_experimental.PipelineThenStatsAgg
);
"#);

//
// SUM
//
pg_type! {
    #[derive(Debug)]
    struct PipelineThenSum<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

ron_inout_funcs!(PipelineThenSum);

#[pg_extern(
    immutable,
    parallel_safe,
    name="sum_cast",
    schema="toolkit_experimental"
)]
pub fn sum_pipeline_element<'p, 'e>(
    accessor: toolkit_experimental::AccessorSum<'p>,
) -> toolkit_experimental::PipelineThenSum<'e> {
    let _ = accessor;
    build ! {
        PipelineThenSum {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

extension_sql!(r#"
    CREATE CAST (toolkit_experimental.AccessorSum AS toolkit_experimental.PipelineThenSum)
        WITH FUNCTION toolkit_experimental.sum_cast
        AS IMPLICIT;
"#);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_pipeline_then_sum<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::PipelineThenSum<'p>,
) -> Option<f64> {
    let pipeline = pipeline.0;
    let pipeline = build! {
        PipelineThenStatsAgg {
            num_elements: pipeline.num_elements,
            elements: pipeline.elements,
        }
    };
    let stats_agg = run_pipeline_then_stats_agg(timeseries, pipeline);
    stats_agg::stats1d_sum(stats_agg)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn finalize_with_sum<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenSum<'e>,
) -> toolkit_experimental::PipelineThenSum<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineThenSum {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenSum {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema="toolkit_experimental"
)]
pub unsafe fn pipeline_sum_support(input: Internal)
-> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| unsafe {
        let new_element = PipelineThenSum::from_datum(new_element, false, 0)
            .unwrap();
        finalize_with_sum(old_pipeline, new_element).into_datum().unwrap()
    })
}

extension_sql!(r#"
ALTER FUNCTION "arrow_pipeline_then_sum" SUPPORT toolkit_experimental.pipeline_sum_support;
"#);


//
// AVERAGE
//
pg_type! {
    #[derive(Debug)]
    struct PipelineThenAverage<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

ron_inout_funcs!(PipelineThenAverage);

#[pg_extern(
    immutable,
    parallel_safe,
    name="average_cast",
    schema="toolkit_experimental"
)]
pub fn average_pipeline_element<'p, 'e>(
    accessor: toolkit_experimental::AccessorAverage<'p>,
) -> toolkit_experimental::PipelineThenAverage<'e> {
    let _ = accessor;
    build ! {
        PipelineThenAverage {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

extension_sql!(r#"
    CREATE CAST (toolkit_experimental.AccessorAverage AS toolkit_experimental.PipelineThenAverage)
        WITH FUNCTION toolkit_experimental.average_cast
        AS IMPLICIT;
"#);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_pipeline_then_average<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::PipelineThenAverage<'p>,
) -> Option<f64> {
    let pipeline = pipeline.0;
    let pipeline = build! {
        PipelineThenStatsAgg {
            num_elements: pipeline.num_elements,
            elements: pipeline.elements,
        }
    };
    let stats_agg = run_pipeline_then_stats_agg(timeseries, pipeline);
    stats_agg::stats1d_average(stats_agg)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn finalize_with_average<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenAverage<'e>,
) -> toolkit_experimental::PipelineThenAverage<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineThenAverage {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenAverage {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema="toolkit_experimental"
)]
pub unsafe fn pipeline_average_support(input: Internal)
-> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| unsafe {
        let new_element = PipelineThenAverage::from_datum(new_element, false, 0)
            .unwrap();
        finalize_with_average(old_pipeline, new_element).into_datum().unwrap()
    })
}

extension_sql!(r#"
ALTER FUNCTION "arrow_pipeline_then_average" SUPPORT toolkit_experimental.pipeline_average_support;
"#);


//
// NUM_VALS
//
pg_type! {
    #[derive(Debug)]
    struct PipelineThenNumVals<'input> {
        num_elements: u64,
        elements: [Element; self.num_elements],
    }
}

ron_inout_funcs!(PipelineThenNumVals);

#[pg_extern(
    immutable,
    parallel_safe,
    name="num_vals_cast",
    schema="toolkit_experimental"
)]
pub fn num_vals_pipeline_element<'p, 'e>(
    accessor: toolkit_experimental::AccessorNumVals<'p>,
) -> toolkit_experimental::PipelineThenNumVals<'e> {
    let _ = accessor;
    build ! {
        PipelineThenNumVals {
            num_elements: 0,
            elements: vec![].into(),
        }
    }
}

extension_sql!(r#"
    CREATE CAST (toolkit_experimental.AccessorNumVals AS toolkit_experimental.PipelineThenNumVals)
        WITH FUNCTION toolkit_experimental.num_vals_cast
        AS IMPLICIT;
"#);

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn arrow_pipeline_then_num_vals<'s, 'p>(
    timeseries: toolkit_experimental::TimeSeries<'s>,
    pipeline: toolkit_experimental::PipelineThenNumVals<'p>,
) -> i64 {
    run_pipeline_elements(timeseries, pipeline.elements.iter())
        .num_vals() as _
}

#[pg_operator(immutable, parallel_safe)]
#[opname(->)]
pub fn finalize_with_num_vals<'p, 'e>(
    mut pipeline: toolkit_experimental::UnstableTimeseriesPipeline<'p>,
    then_stats_agg: toolkit_experimental::PipelineThenNumVals<'e>,
) -> toolkit_experimental::PipelineThenNumVals<'e> {
    if then_stats_agg.num_elements == 0 {
        // flatten immediately so we don't need a temporary allocation for elements
        return unsafe {flatten! {
            PipelineThenNumVals {
                num_elements: pipeline.0.num_elements,
                elements: pipeline.0.elements,
            }
        }}
    }

    let mut elements = replace(pipeline.elements.as_owned(), vec![]);
    elements.extend(then_stats_agg.elements.iter());
    build! {
        PipelineThenNumVals {
            num_elements: elements.len().try_into().unwrap(),
            elements: elements.into(),
        }
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    schema="toolkit_experimental"
)]
pub unsafe fn pipeline_num_vals_support(input: Internal)
-> Internal {
    pipeline_support_helper(input, |old_pipeline, new_element| unsafe {
        let new_element = PipelineThenNumVals::from_datum(new_element, false, 0)
            .unwrap();
        finalize_with_num_vals(old_pipeline, new_element).into_datum().unwrap()
    })
}

extension_sql!(r#"
ALTER FUNCTION "arrow_pipeline_then_num_vals" SUPPORT toolkit_experimental.pipeline_num_vals_support;
"#);

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_stats_agg_finalizer() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timeseries(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client.select(
                &format!("SELECT (series -> stats_agg())::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "(version:1,n:5,sx:100,sx2:250,sx3:0,sx4:21250)");
        });
    }

    #[pg_test]
    fn test_stats_agg_pipeline_folding() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // `-> series()` should force materialization, but otherwise the
            // pipeline-folding optimization should proceed
            let output = client.select(
                "EXPLAIN (verbose) SELECT \
                timeseries('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> stats_agg() -> average();",
                None,
                None
            ).skip(1)
                .next().unwrap()
                .by_ordinal(1).unwrap()
                .value::<String>().unwrap();
            assert_eq!(output.trim(), "Output: (\
                run_pipeline_then_stats_agg(\
                    timeseries('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethenstatsagg\
                ) -> '(version:1)'::accessoraverage)");
        });
    }


    #[pg_test]
    fn test_sum_finalizer() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timeseries(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client.select(
                &format!("SELECT (series -> sum())::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "100");
        });
    }

    #[pg_test]
    fn test_sum_pipeline_folding() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // `-> series()` should force materialization, but otherwise the
            // pipeline-folding optimization should proceed
            let output = client.select(
                "EXPLAIN (verbose) SELECT \
                timeseries('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> sum();",
                None,
                None
            ).skip(1)
                .next().unwrap()
                .by_ordinal(1).unwrap()
                .value::<String>().unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_pipeline_then_sum(\
                    timeseries('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethensum\
                )");
        });
    }

    #[pg_test]
    fn test_average_finalizer() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timeseries(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client.select(
                &format!("SELECT (series -> average())::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "20");
        });
    }

    #[pg_test]
    fn test_average_pipeline_folding() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // `-> series()` should force materialization, but otherwise the
            // pipeline-folding optimization should proceed
            let output = client.select(
                "EXPLAIN (verbose) SELECT \
                timeseries('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> average();",
                None,
                None
            ).skip(1)
                .next().unwrap()
                .by_ordinal(1).unwrap()
                .value::<String>().unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_pipeline_then_average(\
                    timeseries('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethenaverage\
                )");
        });
    }

    #[pg_test]
    fn test_num_vals_finalizer() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // we use a subselect to guarantee order
            let create_series = "SELECT timeseries(time, value) as series FROM \
                (VALUES ('2020-01-04 UTC'::TIMESTAMPTZ, 25.0), \
                    ('2020-01-01 UTC'::TIMESTAMPTZ, 10.0), \
                    ('2020-01-03 UTC'::TIMESTAMPTZ, 20.0), \
                    ('2020-01-02 UTC'::TIMESTAMPTZ, 15.0), \
                    ('2020-01-05 UTC'::TIMESTAMPTZ, 30.0)) as v(time, value)";

            let val = client.select(
                &format!("SELECT (series -> num_vals())::TEXT FROM ({}) s", create_series),
                None,
                None
            )
                .first()
                .get_one::<String>();
            assert_eq!(val.unwrap(), "5");
        });
    }

    #[pg_test]
    fn test_num_vals_pipeline_folding() {
        Spi::execute(|client| {
            client.select("SET timezone TO 'UTC'", None, None);
            // using the search path trick for this test b/c the operator is
            // difficult to spot otherwise.
            let sp = client.select("SELECT format(' %s, toolkit_experimental',current_setting('search_path'))", None, None).first().get_one::<String>().unwrap();
            client.select(&format!("SET LOCAL search_path TO {}", sp), None, None);
            client.select("SET timescaledb_toolkit_acknowledge_auto_drop TO 'true'", None, None);

            // `-> series()` should force materialization, but otherwise the
            // pipeline-folding optimization should proceed
            let output = client.select(
                "EXPLAIN (verbose) SELECT \
                timeseries('1930-04-05'::timestamptz, 123.0) \
                -> ceil() -> abs() -> floor() \
                -> num_vals();",
                None,
                None
            ).skip(1)
                .next().unwrap()
                .by_ordinal(1).unwrap()
                .value::<String>().unwrap();
            assert_eq!(output.trim(), "Output: \
                arrow_pipeline_then_num_vals(\
                    timeseries('1930-04-05 00:00:00+00'::timestamp with time zone, '123'::double precision), \
                    '(version:1,num_elements:3,elements:[\
                        Arithmetic(function:Ceil,rhs:0),\
                        Arithmetic(function:Abs,rhs:0),\
                        Arithmetic(function:Floor,rhs:0)\
                    ])'::pipelinethennumvals\
                )");
        });
    }
}