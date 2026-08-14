//! Serves the pre-rename wire surface. Published clients built from the old
//! proto still call /gkg.v1.KnowledgeGraphService/*; the messages are
//! byte-identical to orbit.v1, so only the request path needs translation.
//! Delete this module once the old paths show zero traffic for a milestone
//! (knowledge-graph#1152, chain T10). Traffic is visible per service label in
//! the labkit gRPC metrics, which run outside this facade.

use tonic::codegen::http;
use tonic::server::NamedService;
use tower::Service;

const LEGACY_SERVICE: &str = "gkg.v1.KnowledgeGraphService";
const CURRENT_SERVICE: &str = "orbit.v1.OrbitService";

#[derive(Clone)]
pub struct LegacyGkgService<S> {
    inner: S,
}

impl<S> LegacyGkgService<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> NamedService for LegacyGkgService<S> {
    const NAME: &'static str = LEGACY_SERVICE;
}

impl<S, B> Service<http::Request<B>> for LegacyGkgService<S>
where
    S: Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        let path = req.uri().path();
        if let Some(method) = path.strip_prefix(&format!("/{LEGACY_SERVICE}/")[..]) {
            let rewritten = format!("/{CURRENT_SERVICE}/{method}");
            let mut parts = req.uri().clone().into_parts();
            parts.path_and_query = Some(
                http::uri::PathAndQuery::try_from(rewritten)
                    .expect("service prefix swap keeps the path valid"),
            );
            *req.uri_mut() =
                http::Uri::from_parts(parts).expect("URI rebuilt from its own parts is valid");
        }
        self.inner.call(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::task::{Context, Poll};

    #[derive(Clone)]
    struct CapturePath;

    impl Service<http::Request<()>> for CapturePath {
        type Response = String;
        type Error = Infallible;
        type Future = std::future::Ready<Result<String, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<()>) -> Self::Future {
            std::future::ready(Ok(req.uri().path().to_string()))
        }
    }

    fn call(path: &str) -> String {
        let mut svc = LegacyGkgService::new(CapturePath);
        let req = http::Request::builder().uri(path).body(()).unwrap();
        futures::executor::block_on(svc.call(req)).unwrap()
    }

    #[test]
    fn legacy_path_rewrites_to_current_service() {
        assert_eq!(
            call("/gkg.v1.KnowledgeGraphService/ExecuteQuery"),
            "/orbit.v1.OrbitService/ExecuteQuery"
        );
    }

    #[test]
    fn current_path_is_untouched() {
        assert_eq!(
            call("/orbit.v1.OrbitService/ExecuteQuery"),
            "/orbit.v1.OrbitService/ExecuteQuery"
        );
    }

    #[test]
    fn named_service_advertises_the_legacy_name() {
        assert_eq!(
            <LegacyGkgService<CapturePath> as NamedService>::NAME,
            "gkg.v1.KnowledgeGraphService"
        );
    }
}
