#!/usr/bin/env python3
"""Run the SDK examples against an ephemeral Cloudreve container."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request


REPOSITORY = Path(__file__).resolve().parent.parent
IMAGE = os.environ.get(
    "CLOUDREVE_E2E_IMAGE", "docker.io/cloudreve/cloudreve:4.19.1"
)
CONTAINER = f"cloudreve-sdk-e2e-{os.getpid()}"
EMAIL = "e2e-admin@example.test"
PASSWORD = "e2e-password-change-me"
UPLOAD_NAME = "cloudreve-sdk-e2e-upload.toml"


def run(*command: str, capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=REPOSITORY,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def api_request(url: str, body: dict[str, str] | None = None) -> object:
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    request = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.load(response)
    if payload.get("code") != 0:
        raise RuntimeError(f"Cloudreve API request failed: {payload!r}")
    return payload.get("data")


def wait_until_ready(server: str) -> None:
    deadline = time.monotonic() + 90
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            version = api_request(f"{server}/api/v4/site/ping")
            print(f"Cloudreve {version} is ready", flush=True)
            return
        except (OSError, RuntimeError, ValueError, urllib.error.URLError) as error:
            last_error = error
            time.sleep(1)
    raise TimeoutError(f"Cloudreve did not become ready: {last_error}")


def remove_container() -> None:
    if shutil.which("podman") is None:
        return
    subprocess.run(
        ("podman", "rm", "--force", CONTAINER),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def stop_on_signal(_signum: int, _frame: object) -> None:
    raise KeyboardInterrupt


def main() -> None:
    for command in ("podman", "cargo"):
        if shutil.which(command) is None:
            raise RuntimeError(f"required command not found: {command}")

    print(f"Pulling Cloudreve image {IMAGE}", flush=True)
    run("podman", "pull", IMAGE)

    print(f"Starting ephemeral Cloudreve container from {IMAGE}", flush=True)
    run(
        "podman",
        "run",
        "--detach",
        "--rm",
        "--pull",
        "never",
        "--image-volume",
        "ignore",
        "--name",
        CONTAINER,
        "--publish",
        "127.0.0.1::5212",
        IMAGE,
        capture_output=True,
    )

    inspection = json.loads(
        run("podman", "inspect", CONTAINER, capture_output=True).stdout
    )[0]
    mounts = inspection.get("Mounts", [])
    if mounts:
        raise RuntimeError(f"ephemeral container unexpectedly has mounts: {mounts!r}")
    print("Verified that the container has no mounts", flush=True)

    port = run(
        "podman", "port", CONTAINER, "5212/tcp", capture_output=True
    ).stdout.strip()
    server = f"http://{port}"
    print(f"Waiting for Cloudreve at {server}", flush=True)
    wait_until_ready(server)

    print("Registering the first user as administrator", flush=True)
    api_request(
        f"{server}/api/v4/user",
        {"email": EMAIL, "password": PASSWORD, "language": "en-US"},
    )

    example_environment = os.environ.copy()
    example_environment.update(
        {
            "CLOUDREVE_SERVER": server,
            "CLOUDREVE_EMAIL": EMAIL,
            "CLOUDREVE_PASSWORD": PASSWORD,
            "CLOUDREVE_URI": "cloudreve://my/",
        }
    )

    print("Running API login/list example", flush=True)
    subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "-p",
            "cloudreve-sdk-api",
            "--example",
            "login_and_list",
        ),
        cwd=REPOSITORY,
        env=example_environment,
        check=True,
    )

    print("Running uploader example", flush=True)
    subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "-p",
            "cloudreve-sdk-uploader",
            "--example",
            "upload_file",
            "--",
            "Cargo.toml",
            f"cloudreve://my/{UPLOAD_NAME}",
        ),
        cwd=REPOSITORY,
        env=example_environment,
        check=True,
    )

    print(
        "Running resumable chunk upload example (first phase: persist one chunk)",
        flush=True,
    )
    resume_dir = REPOSITORY / "target" / f"resume-e2e-{os.getpid()}"
    shutil.rmtree(resume_dir, ignore_errors=True)
    resume_environment = example_environment.copy()
    resume_environment.update(
        {
            "CLOUDREVE_RESUME_DIR": str(resume_dir),
            "CLOUDREVE_RESUME_NAME": "cloudreve-sdk-resume-e2e.bin",
            "CLOUDREVE_RESUME_MB": "64",
        }
    )
    subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "-p",
            "cloudreve-sdk-uploader",
            "--example",
            "resume_upload",
        ),
        cwd=REPOSITORY,
        env={**resume_environment, "CLOUDREVE_RESUME_PHASE": "first"},
        check=True,
    )

    print(
        "Running resumable chunk upload example (second phase: new process resumes)",
        flush=True,
    )
    subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "-p",
            "cloudreve-sdk-uploader",
            "--example",
            "resume_upload",
        ),
        cwd=REPOSITORY,
        env={**resume_environment, "CLOUDREVE_RESUME_PHASE": "second"},
        check=True,
    )
    shutil.rmtree(resume_dir, ignore_errors=True)

    print("Running share-link CRUD example", flush=True)
    subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "-p",
            "cloudreve-sdk-api",
            "--example",
            "share_crud",
        ),
        cwd=REPOSITORY,
        env=example_environment,
        check=True,
    )

    print("Verifying the uploaded file through the API example", flush=True)
    result = subprocess.run(
        (
            "cargo",
            "run",
            "--quiet",
            "-p",
            "cloudreve-sdk-api",
            "--example",
            "login_and_list",
        ),
        cwd=REPOSITORY,
        env=example_environment,
        check=True,
        text=True,
        capture_output=True,
    )
    print(result.stdout, end="")
    if UPLOAD_NAME not in result.stdout:
        raise RuntimeError("uploaded file was not returned by the list example")

    print("E2E test passed", flush=True)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop_on_signal)
    failed = False
    try:
        main()
    except BaseException:
        failed = True
        raise
    finally:
        if failed:
            subprocess.run(
                ("podman", "logs", CONTAINER),
                stdout=sys.stderr,
                stderr=subprocess.DEVNULL,
                check=False,
                text=True,
            )
        remove_container()
