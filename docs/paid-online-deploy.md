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

## Batch Uploads For More Schools

Use batch uploads when you want to add schools gradually, for example 100 schools at a time, without uploading the full multi-GB platform archive every round.

This works because the API recursively reads every Parquet file under:

```text
/var/data/outcomes/platform_parquet/
```

Each batch should extract into its own non-overwriting subfolder, such as:

```text
platform_parquet/base_fact/schools-001/part-00000.parquet
platform_parquet/work_facts/annual_salary/schools-001/part-00000.parquet
platform_parquet/base_fact/schools-002/part-00000.parquet
platform_parquet/work_facts/annual_salary/schools-002/part-00000.parquet
```

Do not drop batch files directly into a shared directory with generic names like `part-00000.parquet`, because later batches can overwrite earlier ones.

### Create A School Batch Archive

On the machine that has the latest extracted `platform_parquet`, create a text file with one `unitid` per line:

```text
110635
190150
243744
```

Then package the batch:

```bash
cd /Users/caelan/Downloads/Untitled/college_outcomes_platform
.venv/bin/python scripts/package_school_batch.py \
  --source /Users/caelan/Downloads/school_outcomes_data_v4_3/platform_parquet \
  --schools-file /Users/caelan/Downloads/school-batch-001.txt \
  --batch-name schools-001 \
  --output /Users/caelan/Downloads/outcomes-schools-001.tar.gz
```

You can also pass unitids inline:

```bash
.venv/bin/python scripts/package_school_batch.py \
  --source /Users/caelan/Downloads/school_outcomes_data_v4_3/platform_parquet \
  --school 110635,190150,243744 \
  --batch-name schools-001 \
  --output /Users/caelan/Downloads/outcomes-schools-001.tar.gz
```

The archive contains only rows for those schools for datasets with a `unitid` column. Reference datasets are copied in because they are small.

### Upload And Activate A Batch On Render

In the Render Shell:

```bash
mkdir -p /var/data/outcomes
cd /var/data/outcomes
wormhole receive
```

On your laptop:

```bash
wormhole send /Users/caelan/Downloads/outcomes-schools-001.tar.gz
```

Back in the Render Shell:

```bash
cd /var/data/outcomes
tar -xzf outcomes-schools-001.tar.gz
rm outcomes-schools-001.tar.gz
find platform_parquet/base_fact/schools-001 -name '*.parquet' | head
```

Restart or redeploy the Render service after each extracted batch. The API caches dataset file lists in process memory, so it will not reliably see new batch files until restart.

### Warm The New Schools

After redeploy, open the site and query one of the new schools, or call `/api/warm-cache` for the schools you expect users to open first. Warming is useful because the API creates per-school cache Parquet files on first use.

### When To Use A Full Replacement Instead

Use a full archive replacement when the precompute changes logic globally, for example:

- Weighting/calibration changes.
- CIP mapping changes.
- New columns or renamed columns.
- Rebuilt work facts or career features.
- A bug fix that changes existing schools, not just adds new schools.

For full replacement, move the old `platform_parquet` aside, extract the new full archive, and redeploy:

```bash
cd /var/data/outcomes
mv platform_parquet platform_parquet_old_$(date +%Y%m%d_%H%M%S)
tar -xzf outcomes-platform.tar.gz
```

Batch uploads are best for additive school rollout from the same precompute version.

Avoid putting the same `unitid` in multiple active batches. If you need to replace an existing school's data, do a full replacement or remove the old batch files and clear the school cache before redeploying:

```bash
rm -rf /var/data/outcomes_school_cache/*
```

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
