# Paid Online Deployment

This is the practical deployment shape for a shareable internal demo:

```text
GitHub Pages static frontend
  -> protected HTTPS API
      -> private platform_parquet data on an attached disk
```

Do not put `platform_parquet/` or generated data bundles in GitHub.

## Recommended First Host

Use a paid container web service that supports:

- Docker deployments from GitHub.
- Persistent disk or volume storage.
- HTTPS.
- Environment variables/secrets.

Render, Fly.io, Cloud Run, Railway, or an internal VM can all work. The simplest first setup is a container service with a persistent disk. The API image can download the data archive once at startup and reuse the mounted disk after that.

## 1. Package The Data

On the machine that has the latest data:

```bash
cd /Users/caelan/Downloads/Untitled/college_outcomes_platform
bash scripts/package_platform_data.sh \
  /Users/caelan/Downloads/school_outcomes_data_v4_3 \
  /Users/caelan/Downloads/outcomes-platform.tar.gz
```

Upload `outcomes-platform.tar.gz` somewhere private that can issue a temporary HTTPS download URL. Good options:

- Cloudflare R2 private object with a presigned URL.
- AWS S3 private object with a presigned URL.
- Google Cloud Storage signed URL.
- An internal file host.

Do not upload this archive to the public GitHub repo.

## 2. Deploy The API Container

Create a paid web service from this GitHub repo using:

```text
Dockerfile path: api/Dockerfile
Docker context/root: api
```

Attach a persistent disk/volume with at least `10 GB`, mounted at:

```text
/var/data
```

Set environment variables:

```text
OUTCOMES_DATA_DIR=/var/data/outcomes
OUTCOMES_DATA_ARCHIVE_URL=<private signed URL to outcomes-platform.tar.gz>
OUTCOMES_ALLOWED_ORIGINS=https://caelanwr.github.io,http://localhost:8124
OUTCOMES_APP_PASSWORD=<long random password>
```

You do not need to set `OUTCOMES_PARQUET_ROOT` if the archive contains `platform_parquet/base_fact`; the startup script will find it and set the root automatically.

After the first successful boot, remove or rotate `OUTCOMES_DATA_ARCHIVE_URL` if your host keeps the persistent disk. The data will already be extracted on the disk.

## Manual Upload Instead Of Object Storage

If you do not have private S3/R2/GCS storage, use Render's disk-backed service shell to transfer the archive without creating a public URL.

Set this temporary environment variable so the service can boot before data exists:

```text
OUTCOMES_ALLOW_EMPTY_STARTUP=true
```

Deploy the service. `/api/health` should respond, but dashboard queries will not work until data is uploaded.

Open the service's **Shell** page in Render and run:

```bash
mkdir -p /var/data/outcomes
cd /var/data/outcomes
wormhole receive
```

On your laptop, send the archive:

```bash
wormhole send /Users/caelan/Downloads/Untitled/outcomes-platform.tar.gz
```

Enter the code shown by `wormhole send` into the Render shell. This transfers directly to the Render service; it does not publish the archive.

Then, in the Render shell:

```bash
cd /var/data/outcomes
tar -xzf outcomes-platform.tar.gz
rm outcomes-platform.tar.gz
find /var/data/outcomes/platform_parquet/base_fact -name '*.parquet' | head
```

After extraction succeeds:

1. Remove `OUTCOMES_ALLOW_EMPTY_STARTUP` from Render.
2. Leave `OUTCOMES_DATA_DIR=/var/data/outcomes`.
3. Redeploy the service.
4. Check `/api/health`.

Render also supports SCP for disk-backed paid services after SSH setup. SCP is secure too, but Magic-Wormhole is usually faster to set up for a one-time data transfer.

## 3. Verify The API

Open:

```text
https://YOUR_API_HOST/api/health
```

Expected:

```json
{"status":"ok","data_version":"...","base_fact_exists":true}
```

## 4. Point The Site At The API

Use the GitHub Pages frontend URL. In Advanced Options:

```text
Protected API URL: https://YOUR_API_HOST
API Password: <OUTCOMES_APP_PASSWORD>
```

If you want the site to prefill the API URL, edit `site/config.js`:

```js
window.OUTCOMES_API_URL = "https://YOUR_API_HOST";
```

Then commit and push.

## 5. Access Control

For a real internal rollout, put SSO in front of both the frontend and API. The API password is a workable stopgap over HTTPS, but it is not a substitute for SSO.

Best options:

- Cloudflare Access for both the GitHub Pages site and the API.
- Provider-native identity-aware proxy.
- Internal VPN-only API URL.

Keep the API password enabled even behind SSO if you want a second layer.
