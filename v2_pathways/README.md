# Protected pathway demo service

Deploy separately from the current tracker. Root and Docker build context are
`v2_pathways`; Dockerfile is `Dockerfile`; health path is `/api/health`.
The Render free plan is sufficient for this small Scripps demo. Free instances
can sleep when idle, so the first request may take longer.

Add `pathways.bundle` as a Render **secret file**. It contains the private
Scripps linked snapshot subset, not a password. Never commit it or copy it into
the Docker image. Runtime expands it into a private temporary directory. There
is no file-serving endpoint. The public image contains only source code.

Every query requires the existing tracker token. The current API must authorize
the Career cohort before the private snapshot is queried; its normalized filters
and cell threshold are used. Invalid credentials and unverifiable cohorts fail
closed. Tokens and request bodies are not logged or persisted. Public health
returns only service status.

Hosted pathways cover Scripps, including program, graduation cohort and other
supported filters. Other tracker views continue to cover the current API's full
institution set. The linked source release and current tracker release are
retained separately in response metadata. Expand the hosted pathway institution
set by moving to a private object store or persistent backend; do not publish
Parquet or test fixtures with GitHub Pages.

Tests: `python -m unittest discover -s tests` from this directory.
