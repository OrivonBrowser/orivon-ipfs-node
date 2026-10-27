// A minimal HTTP/1.1 GET client, standing in for the reqwest client that
// wasm32-wasip2 has no backend for (see the Cargo.toml comment on this
// crate's `[target.'cfg(not(target_os = "wasi"))'.dependencies]`). It is
// built on every target so its tests run natively; only wasm32-wasip2 uses it.
//
// reqwest itself already resolves hyper, hyper-rustls, tokio-rustls and
// webpki-roots into this workspace's Cargo.lock on native targets (via its
// own rustls-tls feature); this module drives the same crates directly for
// one GET request at a time, hand-rolling the HTTP/1.1 handshake rather than
// wiring up hyper-util's generic connector service, since a one-shot,
// one-connection GET has no need for its connection pooling.
//
// DNS goes through `tokio::net::lookup_host` (this crate's std resolver
// path): on wasi, that funnels through the vendored tokio's inline blocking
// pool (see vendor/tokio in this workspace), the same mechanism that already
// makes bootstrap-peer DNS work for libp2p-dns.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::header::HOST;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

#[derive(Debug, thiserror::Error)]
pub enum WasiHttpError {
    #[error("invalid url: {0}")]
    InvalidUrl(String),
    #[error("dns lookup failed for {host}: {source}")]
    Dns {
        host: String,
        source: std::io::Error,
    },
    #[error("no address found for {0}")]
    NoAddress(String),
    #[error("tcp connect failed: {0}")]
    Connect(std::io::Error),
    #[error("tls handshake failed: {0}")]
    Tls(std::io::Error),
    #[error("http error: {0}")]
    Http(String),
    #[error("response exceeded {0} byte limit")]
    TooLarge(usize),
    #[error("request timed out")]
    Timeout,
}

#[derive(Debug)]
pub struct WasiHttpResponse {
    pub status: u16,
    pub headers: hyper::HeaderMap,
    pub body: Vec<u8>,
}

impl WasiHttpResponse {
    pub fn header_str(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

/// Issue one HTTPS GET request and return its status, headers and full
/// (size-bounded) body. `url` must be `https://`; redirects are never
/// followed (a 3xx is returned as the response).
pub async fn get(
    url: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
    max_body_bytes: usize,
) -> Result<WasiHttpResponse, WasiHttpError> {
    get_with(url, headers, timeout, max_body_bytes, false).await
}

/// Like `get`, and `http://` is accepted too. Only for a body the caller
/// verifies itself (a block checked against its CID): plaintext carries no
/// integrity of its own.
pub async fn get_allow_plaintext(
    url: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
    max_body_bytes: usize,
) -> Result<WasiHttpResponse, WasiHttpError> {
    get_with(url, headers, timeout, max_body_bytes, true).await
}

async fn get_with(
    url: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
    max_body_bytes: usize,
    allow_plaintext: bool,
) -> Result<WasiHttpResponse, WasiHttpError> {
    match tokio::time::timeout(
        timeout,
        get_inner(url, headers, max_body_bytes, allow_plaintext),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(WasiHttpError::Timeout),
    }
}

async fn get_inner(
    url: &str,
    headers: &[(&str, &str)],
    max_body_bytes: usize,
    allow_plaintext: bool,
) -> Result<WasiHttpResponse, WasiHttpError> {
    let uri: hyper::Uri = url
        .parse()
        .map_err(|_| WasiHttpError::InvalidUrl(url.to_string()))?;
    let tls = match uri.scheme_str() {
        Some("https") => true,
        Some("http") if allow_plaintext => false,
        _ => {
            return Err(WasiHttpError::InvalidUrl(format!(
                "{url}: only https:// is supported here"
            )))
        }
    };
    let host = uri
        .host()
        .ok_or_else(|| WasiHttpError::InvalidUrl(url.to_string()))?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string();
    let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_string();
    let host_header = match uri.port_u16() {
        Some(port) => format!("{}:{port}", uri.host().unwrap_or_default()),
        None => uri.host().unwrap_or_default().to_string(),
    };

    let mut addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|source| WasiHttpError::Dns {
            host: host.clone(),
            source,
        })?;
    let sock_addr = addrs
        .next()
        .ok_or_else(|| WasiHttpError::NoAddress(host.clone()))?;

    let tcp = TcpStream::connect(sock_addr)
        .await
        .map_err(WasiHttpError::Connect)?;

    if tls {
        let connector = TlsConnector::from(tls_client_config());
        let server_name = ServerName::try_from(host.clone())
            .map_err(|_| WasiHttpError::InvalidUrl(url.to_string()))?;
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(WasiHttpError::Tls)?;
        request_over(
            TokioIo::new(tls_stream),
            &host_header,
            path_and_query,
            headers,
            max_body_bytes,
        )
        .await
    } else {
        request_over(
            TokioIo::new(tcp),
            &host_header,
            path_and_query,
            headers,
            max_body_bytes,
        )
        .await
    }
}

async fn request_over<I>(
    io: I,
    host_header: &str,
    path_and_query: String,
    headers: &[(&str, &str)],
    max_body_bytes: usize,
) -> Result<WasiHttpResponse, WasiHttpError>
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|err| WasiHttpError::Http(err.to_string()))?;
    // The Connection<T, B> driver future is Send (TokioIo<TlsStream<TcpStream>>
    // and Empty<Bytes> both are), so tokio::spawn accepts it even though this
    // whole program runs a `current_thread` runtime on one OS thread -- that
    // flavor still executes ordinary spawned tasks, it just never creates a
    // second worker thread for them.
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "wasi_http: connection driver ended");
        }
    });

    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(path_and_query)
        .header(HOST, host_header);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = builder
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .map_err(|err| WasiHttpError::Http(err.to_string()))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|err| WasiHttpError::Http(err.to_string()))?;
    let status = response.status().as_u16();
    let response_headers = response.headers().clone();
    let body = read_body_limited(response.into_body(), max_body_bytes).await?;

    Ok(WasiHttpResponse {
        status,
        headers: response_headers,
        body,
    })
}

async fn read_body_limited(
    mut body: Incoming,
    max_bytes: usize,
) -> Result<Vec<u8>, WasiHttpError> {
    let mut collected = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|err| WasiHttpError::Http(err.to_string()))?;
        if let Some(chunk) = frame.data_ref() {
            if collected.len().saturating_add(chunk.len()) > max_bytes {
                return Err(WasiHttpError::TooLarge(max_bytes));
            }
            collected.extend_from_slice(chunk);
        }
    }
    Ok(collected)
}

fn tls_client_config() -> Arc<ClientConfig> {
    static CONFIG: OnceLock<Arc<ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            // Only the "ring" crypto-provider feature is enabled below (no
            // aws-lc-rs in this dependency graph), so there is no ambiguity
            // for rustls to resolve on its own -- but install it explicitly
            // so a future added dependency can't introduce one and panic
            // this call site instead of failing to compile.
            let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn serve_once(response: Vec<u8>) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let n = stream.read(&mut request).unwrap();
            let _ = stream.write_all(&response);
            String::from_utf8_lossy(&request[..n]).to_string()
        });
        (format!("http://{addr}"), handle)
    }

    fn http_response(status: &str, extra: &str, body: &[u8]) -> Vec<u8> {
        let mut out = format!(
            "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(body);
        out
    }

    const WAIT: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn plaintext_get_sends_the_headers_and_returns_status_headers_and_body() {
        let (base, server) = serve_once(http_response("200 OK", "X-Seen: yes\r\n", b"hello"));

        let response = get_allow_plaintext(
            &format!("{base}/ipfs/abc?format=raw"),
            &[("accept", "application/vnd.ipld.raw")],
            WAIT,
            1024,
        )
        .await
        .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.header_str("x-seen"), Some("yes"));
        assert_eq!(response.body, b"hello");
        let request = server.join().unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /ipfs/abc?format=raw http/1.1"), "{request}");
        assert!(request.contains("accept: application/vnd.ipld.raw"), "{request}");
        assert!(request.contains(&format!("host: {}", base.trim_start_matches("http://"))), "{request}");
    }

    #[tokio::test]
    async fn the_https_only_entry_point_refuses_plaintext() {
        let (base, _server) = serve_once(http_response("200 OK", "", b"x"));

        let err = get(&format!("{base}/"), &[], WAIT, 1024).await.unwrap_err();

        assert!(matches!(err, WasiHttpError::InvalidUrl(_)), "{err}");
    }

    #[tokio::test]
    async fn a_redirect_is_returned_not_followed() {
        let (base, _server) = serve_once(http_response(
            "302 Found",
            "Location: http://127.0.0.1:1/elsewhere\r\n",
            b"",
        ));

        let response = get_allow_plaintext(&format!("{base}/"), &[], WAIT, 1024)
            .await
            .unwrap();

        assert_eq!(response.status, 302);
        assert!(response.body.is_empty());
    }

    #[tokio::test]
    async fn a_body_over_the_limit_is_an_error() {
        let (base, _server) = serve_once(http_response("200 OK", "", &[7u8; 2048]));

        let err = get_allow_plaintext(&format!("{base}/"), &[], WAIT, 1024)
            .await
            .unwrap_err();

        assert!(matches!(err, WasiHttpError::TooLarge(1024)), "{err}");
    }

    #[tokio::test]
    async fn a_silent_server_times_out() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let _held = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_secs(2));
        });

        let err = get_allow_plaintext(
            &format!("http://{addr}/"),
            &[],
            Duration::from_millis(200),
            1024,
        )
        .await
        .unwrap_err();

        assert!(matches!(err, WasiHttpError::Timeout), "{err}");
    }
}
