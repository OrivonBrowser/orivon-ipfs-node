use axum::http::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use axum::http::{HeaderValue, StatusCode};
use orivon_ipfs_gateway::router;
use orivon_ipfs_store::SqliteBlockStore;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;
use std::process::Command;
use tokio::net::TcpListener;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires KUBO_BIN=/path/to/ipfs; generates a local Kubo CAR fixture"]
async fn kubo_generated_unixfs_site_matches_local_gateway_bytes() {
    let kubo = env::var("KUBO_BIN").expect("set KUBO_BIN=/path/to/ipfs");
    let tempdir = tempfile::tempdir().unwrap();
    let repo = tempdir.path().join("kubo-repo");
    let site = tempdir.path().join("site");
    fs::create_dir_all(site.join("assets")).unwrap();
    fs::write(
        site.join("index.html"),
        b"<html><body>orivon</body></html>",
    )
    .unwrap();
    fs::write(site.join("assets/style.css"), b"body { color: #111; }\n").unwrap();
    fs::write(site.join("assets/empty.txt"), b"").unwrap();
    fs::write(
        site.join("assets/space name #1.txt"),
        b"encoded path fixture\n",
    )
    .unwrap();
    let large = (0..600_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    fs::write(site.join("assets/blob.bin"), &large).unwrap();

    kubo_ok(&kubo, &repo, ["init", "--empty-repo"]);
    let root = kubo_stdout(
        &kubo,
        &repo,
        [
            OsStr::new("add"),
            OsStr::new("-Qr"),
            OsStr::new("--cid-version=1"),
            OsStr::new("--raw-leaves=true"),
            site.as_os_str(),
        ],
    );
    let root = String::from_utf8(root).unwrap().trim().to_string();
    let car = kubo_stdout(
        &kubo,
        &repo,
        [OsStr::new("dag"), OsStr::new("export"), OsStr::new(&root)],
    );

    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    store.import_car(&car).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(store);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let expected_index = kubo_stdout(
        &kubo,
        &repo,
        [
            OsStr::new("cat"),
            OsStr::new(&format!("/ipfs/{root}/index.html")),
        ],
    );
    let response = reqwest::get(format!("http://{addr}/ipfs/{root}"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "text/html"
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), expected_index);

    for path in [
        "index.html",
        "assets/style.css",
        "assets/empty.txt",
        "assets/blob.bin",
    ] {
        let kubo_path = format!("/ipfs/{root}/{path}");
        let expected = kubo_stdout(&kubo, &repo, [OsStr::new("cat"), OsStr::new(&kubo_path)]);
        let url = format!("http://{addr}/ipfs/{root}/{path}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        if path == "assets/empty.txt" {
            assert_eq!(
                response.headers().get(CONTENT_LENGTH).unwrap(),
                HeaderValue::from_static("0")
            );
        }
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            expected.as_slice()
        );
    }

    let encoded_url = format!("http://{addr}/ipfs/{root}/assets/space%20name%20%231.txt");
    let response = reqwest::get(encoded_url).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        b"encoded path fixture\n"
    );

    let listing_url = format!("http://{addr}/ipfs/{root}/assets");
    let response = reqwest::get(&listing_url).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        HeaderValue::from_static("text/html; charset=utf-8")
    );
    let body = response.text().await.unwrap();
    assert!(body.contains(&format!("Index of /ipfs/{root}/assets")));
    assert!(body.contains(&format!(r#"href="/ipfs/{root}/assets/blob.bin""#)));
    assert!(body.contains(&format!(r#"href="/ipfs/{root}/assets/empty.txt""#)));
    assert!(body.contains(&format!(
        r#"href="/ipfs/{root}/assets/space%20name%20%231.txt""#
    )));

    let response = reqwest::Client::new()
        .head(&listing_url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        HeaderValue::from_static("text/html; charset=utf-8")
    );
    assert!(response.bytes().await.unwrap().is_empty());

    let response = reqwest::Client::new()
        .get(&listing_url)
        .header(RANGE, "bytes=0-127")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let range_start = 123_456usize;
    let range_end = 124_567usize;
    let range_len = range_end - range_start + 1;
    let url = format!("http://{addr}/ipfs/{root}/assets/blob.bin");
    let response = reqwest::Client::new()
        .get(url)
        .header(RANGE, format!("bytes={range_start}-{range_end}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("bytes {range_start}-{range_end}/{}", large.len())
    );
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &large[range_start..=range_end]
    );

    let url = format!("http://{addr}/ipfs/{root}/assets/blob.bin");
    let response = reqwest::Client::new().head(&url).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_LENGTH).unwrap(),
        HeaderValue::from_str(&large.len().to_string()).unwrap()
    );
    assert_eq!(
        response.headers().get(ACCEPT_RANGES).unwrap(),
        HeaderValue::from_static("bytes")
    );
    assert!(response.bytes().await.unwrap().is_empty());

    let response = reqwest::Client::new()
        .head(&url)
        .header(RANGE, format!("bytes={range_start}-{range_end}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response.headers().get(CONTENT_RANGE).unwrap(),
        HeaderValue::from_str(&format!("bytes {range_start}-{range_end}/{}", large.len())).unwrap()
    );
    assert_eq!(
        response.headers().get(CONTENT_LENGTH).unwrap(),
        HeaderValue::from_str(&range_len.to_string()).unwrap()
    );
    assert!(response.bytes().await.unwrap().is_empty());

    let open_start = large.len() - 16;
    let url = format!("http://{addr}/ipfs/{root}/assets/blob.bin");
    let response = reqwest::Client::new()
        .get(url)
        .header(RANGE, format!("bytes={open_start}-"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("bytes {open_start}-{}/{}", large.len() - 1, large.len())
    );
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &large[open_start..]
    );

    let suffix_len = 32usize;
    let suffix_start = large.len() - suffix_len;
    let url = format!("http://{addr}/ipfs/{root}/assets/blob.bin");
    let response = reqwest::Client::new()
        .get(url)
        .header(RANGE, format!("bytes=-{suffix_len}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("bytes {suffix_start}-{}/{}", large.len() - 1, large.len())
    );
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &large[suffix_start..]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires KUBO_BIN=/path/to/ipfs; generates a local Kubo CIDv0 CAR fixture"]
async fn kubo_generated_cidv0_dagpb_site_matches_local_gateway_bytes() {
    let kubo = env::var("KUBO_BIN").expect("set KUBO_BIN=/path/to/ipfs");
    let tempdir = tempfile::tempdir().unwrap();
    let repo = tempdir.path().join("kubo-repo");
    let site = tempdir.path().join("cidv0-site");
    fs::create_dir_all(site.join("docs")).unwrap();
    fs::write(site.join("index.html"), b"<html><body>cidv0</body></html>").unwrap();
    fs::write(site.join("docs/readme.txt"), b"cidv0 dag-pb fixture\n").unwrap();
    fs::write(site.join("docs/empty.txt"), b"").unwrap();
    let large = (0..700_000)
        .map(|index| (255 - (index % 251)) as u8)
        .collect::<Vec<_>>();
    fs::write(site.join("docs/blob.bin"), &large).unwrap();

    kubo_ok(&kubo, &repo, ["init", "--empty-repo"]);
    let root = kubo_stdout(
        &kubo,
        &repo,
        [
            OsStr::new("add"),
            OsStr::new("-Qr"),
            OsStr::new("--cid-version=0"),
            OsStr::new("--raw-leaves=false"),
            site.as_os_str(),
        ],
    );
    let root = String::from_utf8(root).unwrap().trim().to_string();
    assert!(root.starts_with("Qm"), "expected CIDv0 root, got {root}");
    let car = kubo_stdout(
        &kubo,
        &repo,
        [OsStr::new("dag"), OsStr::new("export"), OsStr::new(&root)],
    );

    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    store.import_car(&car).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(store);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let expected_index = kubo_stdout(
        &kubo,
        &repo,
        [
            OsStr::new("cat"),
            OsStr::new(&format!("/ipfs/{root}/index.html")),
        ],
    );
    let response = reqwest::get(format!("http://{addr}/ipfs/{root}"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "text/html"
    );
    assert_eq!(response.bytes().await.unwrap().as_ref(), expected_index);

    for path in [
        "index.html",
        "docs/readme.txt",
        "docs/empty.txt",
        "docs/blob.bin",
    ] {
        let kubo_path = format!("/ipfs/{root}/{path}");
        let expected = kubo_stdout(&kubo, &repo, [OsStr::new("cat"), OsStr::new(&kubo_path)]);
        let url = format!("http://{addr}/ipfs/{root}/{path}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        if path == "docs/empty.txt" {
            assert_eq!(
                response.headers().get(CONTENT_LENGTH).unwrap(),
                HeaderValue::from_static("0")
            );
        }
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            expected.as_slice()
        );
    }

    let listing_url = format!("http://{addr}/ipfs/{root}/docs");
    let response = reqwest::get(&listing_url).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        HeaderValue::from_static("text/html; charset=utf-8")
    );
    let body = response.text().await.unwrap();
    assert!(body.contains(&format!("Index of /ipfs/{root}/docs")));
    assert!(body.contains(&format!(r#"href="/ipfs/{root}/docs/blob.bin""#)));
    assert!(body.contains(&format!(r#"href="/ipfs/{root}/docs/empty.txt""#)));
    assert!(body.contains(&format!(r#"href="/ipfs/{root}/docs/readme.txt""#)));

    let response = reqwest::Client::new()
        .get(&listing_url)
        .header(RANGE, "bytes=0-127")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let range_start = 321_000usize;
    let range_end = 322_222usize;
    let url = format!("http://{addr}/ipfs/{root}/docs/blob.bin");
    let response = reqwest::Client::new()
        .get(url)
        .header(RANGE, format!("bytes={range_start}-{range_end}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(CONTENT_RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("bytes {range_start}-{range_end}/{}", large.len())
    );
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &large[range_start..=range_end]
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires KUBO_BIN=/path/to/ipfs; generates a local Kubo HAMT fixture"]
async fn kubo_generated_hamt_directory_matches_local_gateway_bytes() {
    let kubo = env::var("KUBO_BIN").expect("set KUBO_BIN=/path/to/ipfs");
    let tempdir = tempfile::tempdir().unwrap();
    let repo = tempdir.path().join("kubo-repo");
    let site = tempdir.path().join("hamt");
    fs::create_dir_all(&site).unwrap();
    for index in 0..64 {
        fs::write(
            site.join(format!("file-{index:02}.txt")),
            format!("hamt-file-{index:02}\n"),
        )
        .unwrap();
    }

    kubo_ok(&kubo, &repo, ["init", "--empty-repo"]);
    let root = kubo_stdout(
        &kubo,
        &repo,
        [
            OsStr::new("add"),
            OsStr::new("-Qr"),
            OsStr::new("--cid-version=1"),
            OsStr::new("--raw-leaves=true"),
            OsStr::new("--max-directory-links=1"),
            site.as_os_str(),
        ],
    );
    let root = String::from_utf8(root).unwrap().trim().to_string();
    let car = kubo_stdout(
        &kubo,
        &repo,
        [OsStr::new("dag"), OsStr::new("export"), OsStr::new(&root)],
    );

    let store = SqliteBlockStore::in_memory(16 * 1024 * 1024).unwrap();
    store.import_car(&car).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(store);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    for path in ["file-00.txt", "file-17.txt", "file-63.txt"] {
        let kubo_path = format!("/ipfs/{root}/{path}");
        let expected = kubo_stdout(&kubo, &repo, [OsStr::new("cat"), OsStr::new(&kubo_path)]);
        let url = format!("http://{addr}/ipfs/{root}/{path}");
        let response = reqwest::get(url).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.bytes().await.unwrap().as_ref(),
            expected.as_slice()
        );
    }

    let listing_url = format!("http://{addr}/ipfs/{root}");
    let response = reqwest::get(&listing_url).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        HeaderValue::from_static("text/html; charset=utf-8")
    );
    let body = response.text().await.unwrap();
    assert!(body.contains(&format!("Index of /ipfs/{root}")));
    for path in ["file-00.txt", "file-17.txt", "file-63.txt"] {
        assert!(body.contains(&format!(r#"href="/ipfs/{root}/{path}""#)));
    }

    let response = reqwest::Client::new()
        .head(&listing_url)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        HeaderValue::from_static("text/html; charset=utf-8")
    );
    assert!(response.bytes().await.unwrap().is_empty());

    let response = reqwest::Client::new()
        .get(&listing_url)
        .header(RANGE, "bytes=0-127")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

fn kubo_ok<I, S>(kubo: &str, repo: &Path, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = kubo_command(kubo, repo, args).output().unwrap();
    assert!(
        output.status.success(),
        "kubo command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn kubo_stdout<I, S>(kubo: &str, repo: &Path, args: I) -> Vec<u8>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = kubo_command(kubo, repo, args).output().unwrap();
    assert!(
        output.status.success(),
        "kubo command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn kubo_command<I, S>(kubo: &str, repo: &Path, args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(kubo);
    command.env("IPFS_PATH", repo).args(args);
    command
}
