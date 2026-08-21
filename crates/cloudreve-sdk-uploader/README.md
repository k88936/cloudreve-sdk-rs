# Cloudreve SDK Uploader

Provider-aware, chunked file uploads for Cloudreve CLI clients.

Add both SDK crates to your project:

```toml
[dependencies]
cloudreve-sdk-api = "0.1"
cloudreve-sdk-uploader = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The runnable example logs in, creates an uploader, reports progress, and uploads one file. The
remote argument must be a complete Cloudreve file URI, including the destination filename.

```bash
export CLOUDREVE_SERVER="https://cloudreve.example"
export CLOUDREVE_EMAIL="user@example.com"
export CLOUDREVE_PASSWORD="your-password"

cargo run -p cloudreve-sdk-uploader --example upload_file -- \
  ./photo.jpg cloudreve://my/Uploads/photo.jpg
```

See [`examples/upload_file.rs`](examples/upload_file.rs) for the complete code.
