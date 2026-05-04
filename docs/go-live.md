# Go Live Plan

## What Can Go On GitHub

GitHub Pages should host only:

- `site/index.html`
- `site/config.js`
- static frontend assets

Do not put the generated data bundle in GitHub. The current browser JSON bundle resolves to multiple GB and the new platform `base_fact` is row-level API input.

## Deployment Shape

```text
GitHub Pages frontend
  -> calls an authenticated API
      -> queries platform_parquet/base_fact and aggregate_facts
```

For the current static HTML, `site/config.js` can point at a static aggregate data URL. For the next platform version, the React app should call the API.

## Step 1: Create The GitHub Repo

Create a private repo:

```text
CaelanWR/college-outcomes-internal
```

Do not initialize it with a README because this local repo already has commits.

## Step 2: Push The Frontend Repo

From this folder:

```bash
cd /path/to/college_outcomes_platform
./scripts/publish_to_github.sh git@github.com:CaelanWR/college-outcomes-internal.git
```

If SSH is not configured, use HTTPS:

```bash
./scripts/publish_to_github.sh https://github.com/CaelanWR/college-outcomes-internal.git
```

## Step 3: Enable GitHub Pages

In GitHub:

```text
Settings > Pages > Source > GitHub Actions
```

The workflow at `.github/workflows/pages.yml` deploys the `site/` folder.

## Step 4: Decide Access Control

For internal-only access, do not use a client-side JavaScript password. It is not real security because the frontend code is public to anyone who can load the page.

Use one of:

- GitHub Enterprise private Pages, if enabled for your GitHub account or organization.
- Cloudflare Access in front of the frontend and API. This is the recommended path if private GitHub Pages is unavailable.
- Vercel/Render/Fly with organization SSO.

If private Pages is not available, a private GitHub repo can still publish a public Pages site. Do not put sensitive data in that case. The API and data store must still be private.

## Step 5: Deploy The API

The starter API is in `api/`.

Local test:

```bash
cd /path/to/college_outcomes_platform/api
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
OUTCOMES_PARQUET_ROOT=/path/to/platform_parquet/base_fact uvicorn app:app --reload --port 8000
```

Production options:

- Render/Fly/Cloud Run running `api/Dockerfile`.
- Internal VM running the same Docker container.
- Snowflake-backed API if data governance matters more than infrastructure simplicity.

Set:

```text
OUTCOMES_PARQUET_ROOT=/mounted/path/to/platform_parquet/base_fact
SUPPRESSION_THRESHOLD=25
OUTCOMES_APP_PASSWORD=<long random password>
OUTCOMES_ALLOWED_ORIGINS=https://YOUR_FRONTEND_DOMAIN
```

`OUTCOMES_APP_PASSWORD` is a fallback API password guard. It expects clients to send:

```text
X-Outcomes-Password: <password>
```

This is acceptable only over HTTPS and only as a stopgap. Prefer SSO or Cloudflare Access for the actual internal rollout.

## Step 6: Put Data Somewhere Safe

After the precompute finishes, upload:

```text
OUT_DIR/platform_parquet/
```

to one of:

- private object storage
- a mounted disk on the API server
- an internal file share available to the API

The frontend should not directly expose `base_fact`.

## Current Blockers

- GitHub CLI is not installed on this machine.
- The local shell currently cannot resolve `github.com` without network approval.
- This local repo is ready to push once the personal GitHub repo exists and the machine has GitHub authentication.
