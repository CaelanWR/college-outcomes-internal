"""Private linked pathways; credentials and cohort approval stay with the current API."""
import base64
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import urllib.error
import urllib.request
import zipfile
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.concurrency import run_in_threadpool

from pathways import build_pathways

UPSTREAM = "https://college-outcomes-api.onrender.com"
MAX_BODY = 65536
SLOT = threading.BoundedSemaphore(1)
MEMBERS = {"platform_manifest.json", "base_fact/scripps.parquet"}


def unpack_bundle(filename, destination):
    """Accept only the two known files; never serve or bake the bundle into an image."""
    raw = Path(filename).read_bytes()
    if len(raw) > 1_000_000:
        raise ValueError("Private bundle exceeds the configured size limit")
    decoded = base64.b64decode(raw, validate=True)
    with zipfile.ZipFile(io.BytesIO(decoded)) as archive:
        entries = archive.infolist()
        if len(entries) != 2 or {entry.filename for entry in entries} != MEMBERS:
            raise ValueError("Unexpected bundle members")
        if sum(entry.file_size for entry in entries) > 8_000_000:
            raise ValueError("Private bundle expands beyond the configured limit")
        for entry in entries:
            target = Path(destination) / entry.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(entry))
            target.chmod(0o600)


@asynccontextmanager
async def lifespan(app):
    with tempfile.TemporaryDirectory(prefix="private-pathways-") as root:
        unpack_bundle(os.environ.get("PATHWAYS_BUNDLE", "/etc/secrets/pathways.bundle"), root)
        app.state.data_root = Path(root)
        yield


app = FastAPI(title="Protected Outcomes Pathways", docs_url=None, redoc_url=None,
              openapi_url=None, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(CORSMiddleware, allow_origins=["https://caelanwr.github.io"],
                   allow_methods=["GET", "POST"],
                   allow_headers=["Content-Type", "X-Outcomes-Password"])


@app.middleware("http")
async def private_responses(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    return response


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_):
        return None


def authorize(query, token):
    auth_query = {**query, "active_tab": "career", "view_mode": "snapshot",
                  "compare_mode": False, "top_n": 5}
    req = urllib.request.Request(UPSTREAM + "/api/dashboard",
        data=json.dumps(auth_query).encode(), method="POST",
        headers={"Content-Type": "application/json", "X-Outcomes-Password": token})
    try:
        with urllib.request.build_opener(NoRedirect).open(req, timeout=90) as response:
            authorized = json.loads(response.read(8_000_000))
    except urllib.error.HTTPError as exc:
        status = exc.code if 400 <= exc.code < 500 else 502
        raise HTTPException(status, "The current tracker did not authorize this Career cohort.") from None
    except (OSError, ValueError):
        raise HTTPException(502, "The current tracker could not verify this cohort. Try again shortly.") from None
    meta = authorized.get("meta", {})
    approved = meta.get("filters")
    if not isinstance(approved, dict) or approved.get("schools") != query.get("schools"):
        raise HTTPException(502, "The authorized cohort could not be verified.")
    approved = {**approved, "_pathway_min_cell_weight": meta.get("min_cell_weight", 0)}
    return approved, meta.get("data_version")


def query_pathways(root, query, token):
    approved, tracker_version = authorize(query, token)
    if approved["schools"] != ["123165"]:
        raise HTTPException(422, "The hosted role-pathway demo currently covers Scripps College. Choose Scripps to explore its programs and cohorts.")
    data = build_pathways(root, approved)
    data["meta"].update(source_kind="protected-linked-snapshots",
        source_label="Protected linked snapshot bundle", tracker_data_version=tracker_version)
    return data


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/pathways")
async def pathways(request: Request):
    token = request.headers.get("X-Outcomes-Password", "").strip()
    if not token:
        raise HTTPException(401, "Enter your existing tracker access token.")
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > MAX_BODY:
            raise HTTPException(413, "Expected a JSON object under 64 KiB.")
    try:
        query = json.loads(body)
        if not isinstance(query, dict):
            raise ValueError()
    except (ValueError, UnicodeError):
        raise HTTPException(400, "Expected a JSON object under 64 KiB.") from None
    if len(query.get("schools") or []) != 1 or query.get("compare_mode"):
        raise HTTPException(400, "Choose one institution or major for role pathways.")
    if not SLOT.acquire(blocking=False):
        raise HTTPException(429, "A pathway query is running. Try again shortly.")
    try:
        return await run_in_threadpool(query_pathways, request.app.state.data_root, query, token)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(502, "The linked pathway query could not be completed.") from None
    finally:
        SLOT.release()
