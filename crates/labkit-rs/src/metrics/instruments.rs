use opentelemetry::metrics::{Histogram, Meter, UpDownCounter};
use opentelemetry_semantic_conventions::metric::{
    HTTP_SERVER_ACTIVE_REQUESTS, HTTP_SERVER_REQUEST_BODY_SIZE, HTTP_SERVER_REQUEST_DURATION,
    HTTP_SERVER_RESPONSE_BODY_SIZE, RPC_SERVER_DURATION,
};

/// OTel-recommended histogram buckets for duration in seconds.
pub const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// OTel-recommended histogram buckets for body size in bytes.
pub const SIZE_BUCKETS: &[f64] = &[
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
];

/// Pre-created instruments for HTTP server metrics.
#[derive(Clone)]
pub struct HttpServerInstruments {
    pub request_duration: Histogram<f64>,
    pub active_requests: UpDownCounter<i64>,
    pub request_body_size: Option<Histogram<u64>>,
    pub response_body_size: Option<Histogram<u64>>,
}

impl HttpServerInstruments {
    pub fn new(meter: &Meter, record_body_size: bool) -> Self {
        let request_duration = meter
            .f64_histogram(HTTP_SERVER_REQUEST_DURATION)
            .with_unit("s")
            .with_description("Duration of HTTP server requests")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let active_requests = meter
            .i64_up_down_counter(HTTP_SERVER_ACTIVE_REQUESTS)
            .with_unit("{request}")
            .with_description("Number of active HTTP requests")
            .build();

        let (request_body_size, response_body_size) = if record_body_size {
            (
                Some(
                    meter
                        .u64_histogram(HTTP_SERVER_REQUEST_BODY_SIZE)
                        .with_unit("By")
                        .with_description("Size of HTTP request bodies")
                        .with_boundaries(SIZE_BUCKETS.to_vec())
                        .build(),
                ),
                Some(
                    meter
                        .u64_histogram(HTTP_SERVER_RESPONSE_BODY_SIZE)
                        .with_unit("By")
                        .with_description("Size of HTTP response bodies")
                        .with_boundaries(SIZE_BUCKETS.to_vec())
                        .build(),
                ),
            )
        } else {
            (None, None)
        };

        Self {
            request_duration,
            active_requests,
            request_body_size,
            response_body_size,
        }
    }
}

#[derive(Clone)]
pub struct GrpcServerInstruments {
    pub call_duration: Histogram<f64>,
}

impl GrpcServerInstruments {
    pub fn new(meter: &Meter) -> Self {
        let call_duration = meter
            .f64_histogram(RPC_SERVER_DURATION)
            .with_unit("s")
            .with_description("Duration of gRPC server calls")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        Self { call_duration }
    }
}
