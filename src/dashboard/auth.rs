//! Authentication and CSRF protection for the dashboard.
//!
//! Two axum middlewares are exposed:
//!
//! - [`require_auth`] — gates every dashboard request on a configured
//!   [`DashboardAuth`]. Supports HTTP Basic and Bearer tokens; comparisons
//!   are constant-time to keep the timing side-channel closed.
//! - [`csrf_guard`] — on state-changing methods (`POST`/`PUT`/`PATCH`/
//!   `DELETE`), requires that the `Origin` header (falling back to `Referer`)
//!   names the same authority as the request's `Host` header. Rejects with
//!   `403` otherwise. Same-origin browser requests set `Origin` automatically,
//!   so the legitimate dashboard UI keeps working; a cross-site `<form>`
//!   submit from an attacker page does not.

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::sync::Arc;

/// How to authenticate requests to the dashboard.
#[derive(Debug, Clone)]
pub enum DashboardAuth {
    /// HTTP Basic authentication. The supplied `username`/`password` are
    /// compared in constant time against the credentials in the request's
    /// `Authorization: Basic …` header.
    Basic { username: String, password: String },
    /// HTTP Bearer token authentication. The supplied `token` is compared in
    /// constant time against the token in the request's `Authorization:
    /// Bearer …` header.
    Bearer { token: String },
}

/// Axum middleware that gates every request on the configured [`DashboardAuth`].
///
/// On a missing or invalid credential the middleware returns `401
/// Unauthorized`; for Basic auth the response also carries a
/// `WWW-Authenticate` challenge so browsers will prompt for credentials.
pub async fn require_auth(
    State(auth): State<Arc<DashboardAuth>>,
    req: Request,
    next: Next,
) -> Response {
    let provided = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let ok = match (auth.as_ref(), provided) {
        (DashboardAuth::Basic { username, password }, Some(h)) => {
            check_basic(h, username, password)
        }
        (DashboardAuth::Bearer { token }, Some(h)) => check_bearer(h, token),
        _ => false,
    };

    if ok {
        next.run(req).await
    } else {
        unauthorized(&auth)
    }
}

/// Axum middleware that enforces same-origin on state-changing requests.
///
/// Applied to every route, but only inspects `POST`/`PUT`/`PATCH`/`DELETE` —
/// safe methods pass through untouched. For unsafe methods, the request's
/// `Host` header is compared to the authority of `Origin` (or `Referer` if
/// `Origin` is absent); mismatches and missing values both fail closed with
/// `403 Forbidden`.
pub async fn csrf_guard(req: Request, next: Next) -> Response {
    let is_mutation = matches!(
        *req.method(),
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    if !is_mutation {
        return next.run(req).await;
    }

    let headers = req.headers();
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let source = headers
        .get(header::ORIGIN)
        .or_else(|| headers.get(header::REFERER))
        .and_then(|v| v.to_str().ok())
        .and_then(authority_of);

    match (host, source) {
        (Some(host), Some(source)) if host.eq_ignore_ascii_case(&source) => next.run(req).await,
        _ => (StatusCode::FORBIDDEN, "CSRF check failed").into_response(),
    }
}

/// Return `true` if `host` is a loopback interface; used to decide whether
/// the dashboard may start without an auth guard.
pub(crate) fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // IPv6 literals may appear bracketed (`[::1]`) in config strings.
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    trimmed
        .parse::<std::net::IpAddr>()
        .map(|ip| ip.is_loopback())
        .unwrap_or(false)
}

fn unauthorized(auth: &DashboardAuth) -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    if matches!(auth, DashboardAuth::Basic { .. })
        && let Ok(challenge) = header::HeaderValue::from_str("Basic realm=\"qml-dashboard\"")
    {
        resp.headers_mut()
            .insert(header::WWW_AUTHENTICATE, challenge);
    }
    resp
}

fn check_basic(header_value: &str, expected_user: &str, expected_pass: &str) -> bool {
    let Some(encoded) = header_value.strip_prefix("Basic ") else {
        return false;
    };
    let Some(bytes) = base64_decode(encoded.trim()) else {
        return false;
    };
    let Ok(decoded) = std::str::from_utf8(&bytes) else {
        return false;
    };
    let Some((user, pass)) = decoded.split_once(':') else {
        return false;
    };
    // Always run both comparisons so the total time doesn't depend on which
    // field mismatched first.
    let user_ok = constant_time_eq(user.as_bytes(), expected_user.as_bytes());
    let pass_ok = constant_time_eq(pass.as_bytes(), expected_pass.as_bytes());
    user_ok & pass_ok
}

fn check_bearer(header_value: &str, expected: &str) -> bool {
    let Some(token) = header_value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(token.trim().as_bytes(), expected.as_bytes())
}

/// Length-tolerant constant-time equality, delegated to the `subtle`
/// crate. The earlier hand-rolled XOR-fold did the right thing but the
/// audited version is simpler to reason about and keeps the dashboard
/// off custom crypto code.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// Extract the authority (`host[:port]`) from a URL. Returns `None` if the
/// input doesn't start with a valid scheme — this mirrors browser-emitted
/// `Origin` / `Referer` headers, which are always absolute URLs.
fn authority_of(url: &str) -> Option<String> {
    let (_, rest) = url.split_once("://")?;
    let authority = rest.split(['/', '?', '#']).next()?;
    if authority.is_empty() {
        None
    } else {
        Some(authority.to_ascii_lowercase())
    }
}

/// Standard Base64 decoder for the `Authorization: Basic ...` header.
///
/// Delegated to the `base64` crate's standard engine. The earlier
/// version was a 25-line hand-rolled implementation; the crate
/// version is audited, handles padding edge cases the same way, and
/// keeps the dashboard off custom decoding code.
///
/// The input is passed through verbatim — the crate handles the `=`
/// padding internally. The previous hand-rolled implementation called
/// `input.trim_end_matches('=')` (stripping padding), and a brief
/// intermediate of this function called `.trim()` (stripping
/// surrounding whitespace) which would have widened the contract; the
/// only caller (`check_basic`) already trims its input, so this
/// matches the old strict-format behavior.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(input).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Body,
        http::{Method, Request, StatusCode, header},
        middleware,
        routing::{delete, get, post},
    };
    use tower::ServiceExt;

    #[test]
    fn base64_decodes_standard_input() {
        assert_eq!(
            base64_decode("YWRtaW46c2VjcmV0"),
            Some(b"admin:secret".to_vec())
        );
        assert_eq!(base64_decode("Zm9vOmJhcg=="), Some(b"foo:bar".to_vec()));
        assert_eq!(base64_decode(""), Some(Vec::new()));
        assert!(base64_decode("not*base64!").is_none());
    }

    #[test]
    fn check_basic_accepts_matching_creds() {
        let header = "Basic YWRtaW46c2VjcmV0"; // admin:secret
        assert!(check_basic(header, "admin", "secret"));
        assert!(!check_basic(header, "admin", "wrong"));
        assert!(!check_basic(header, "root", "secret"));
        assert!(!check_basic("Bearer xyz", "admin", "secret"));
        assert!(!check_basic("Basic !!!", "admin", "secret"));
    }

    #[test]
    fn check_bearer_accepts_matching_token() {
        assert!(check_bearer("Bearer abc123", "abc123"));
        assert!(!check_bearer("Bearer abc123", "xyz"));
        assert!(!check_bearer("Basic abc123", "abc123"));
    }

    #[test]
    fn constant_time_eq_handles_length_mismatch() {
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
    }

    #[test]
    fn authority_of_strips_path_and_scheme() {
        assert_eq!(
            authority_of("http://example.com:8080/foo?bar=1"),
            Some("example.com:8080".to_string())
        );
        assert_eq!(
            authority_of("https://DASH.local"),
            Some("dash.local".to_string())
        );
        assert_eq!(authority_of("not-a-url"), None);
    }

    #[test]
    fn is_loopback_host_recognizes_expected_values() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));
        assert!(!is_loopback_host("10.0.0.1"));
        assert!(!is_loopback_host("example.com"));
    }

    fn test_app(auth: Option<DashboardAuth>) -> Router {
        let mut app = Router::new()
            .route("/api/health", get(|| async { "ok" }))
            .route(
                "/api/jobs/{id}/retry",
                post(|| async { StatusCode::NO_CONTENT }),
            )
            .route(
                "/api/jobs/{id}",
                delete(|| async { StatusCode::NO_CONTENT }),
            );

        app = app.layer(middleware::from_fn(csrf_guard));

        if let Some(auth) = auth {
            app = app.layer(middleware::from_fn_with_state(Arc::new(auth), require_auth));
        }
        app
    }

    async fn send(app: Router, req: Request<Body>) -> StatusCode {
        app.oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn require_auth_rejects_missing_credentials() {
        let app = test_app(Some(DashboardAuth::Bearer {
            token: "secret".into(),
        }));
        let req = Request::builder()
            .uri("/api/health")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_accepts_matching_bearer_token() {
        let app = test_app(Some(DashboardAuth::Bearer {
            token: "secret".into(),
        }));
        let req = Request::builder()
            .uri("/api/health")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn require_auth_rejects_wrong_bearer_token() {
        let app = test_app(Some(DashboardAuth::Bearer {
            token: "secret".into(),
        }));
        let req = Request::builder()
            .uri("/api/health")
            .header(header::AUTHORIZATION, "Bearer wrong")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn require_auth_accepts_matching_basic_credentials() {
        let app = test_app(Some(DashboardAuth::Basic {
            username: "admin".into(),
            password: "secret".into(),
        }));
        // base64("admin:secret") = "YWRtaW46c2VjcmV0"
        let req = Request::builder()
            .uri("/api/health")
            .header(header::AUTHORIZATION, "Basic YWRtaW46c2VjcmV0")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn csrf_guard_blocks_mutation_without_origin() {
        let app = test_app(None);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/jobs/abc/retry")
            .header(header::HOST, "localhost:8080")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn csrf_guard_allows_same_origin_mutation() {
        let app = test_app(None);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/jobs/abc/retry")
            .header(header::HOST, "localhost:8080")
            .header(header::ORIGIN, "http://localhost:8080")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn csrf_guard_rejects_cross_origin_mutation() {
        let app = test_app(None);
        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/api/jobs/abc")
            .header(header::HOST, "localhost:8080")
            .header(header::ORIGIN, "https://evil.example.com")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn csrf_guard_ignores_safe_methods() {
        let app = test_app(None);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/health")
            .header(header::HOST, "localhost:8080")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn csrf_guard_falls_back_to_referer() {
        let app = test_app(None);
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/jobs/abc/retry")
            .header(header::HOST, "localhost:8080")
            .header(header::REFERER, "http://localhost:8080/jobs")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(app, req).await, StatusCode::NO_CONTENT);
    }
}
