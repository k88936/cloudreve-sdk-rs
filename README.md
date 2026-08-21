# Cloudreve Rust SDK

most code are mirrored from official maintained [sync desktop](https://github.com/cloudreve/desktop)

Cloudreve Rust SDK provides reusable Rust clients for Cloudreve v4. The workspace contains
`cloudreve-sdk-api`, an asynchronous HTTP API client, and `cloudreve-sdk-uploader`, a
provider-aware uploader supporting chunking, encryption, retries, cancellation, and progress
reporting. Runnable examples demonstrate login, directory listing, and file upload with credentials
supplied through environment variables.

For a reproducible integration check, run `./scripts/e2e_podman.py`. It starts the official
Cloudreve 4.18.0 image with Podman, creates the first administrator, executes both examples, verifies
the uploaded file, and always removes the container. The runner explicitly disables image volumes
and confirms that the container has no mounts, so every run begins with a clean database.
