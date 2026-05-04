# Security Model

## Do Not Use Client-Side Passwords

A password prompt written in frontend JavaScript does not secure a GitHub Pages site. Anyone who can load the page can inspect the source, bypass the prompt, or call any public data URLs directly.

## Recommended Internal Setup

Use this order of preference:

1. GitHub Enterprise private Pages for the frontend, if available.
2. Cloudflare Access in front of both the frontend and the API.
3. Vercel/Render/Fly/Cloud Run with organization SSO or an identity-aware proxy.

The important rule is that the data API must require server-side authentication before it returns chart data.

## Data Boundary

Safe to publish in the frontend repo:

- Static HTML/JS/CSS.
- Non-sensitive copy and UI code.
- Empty `config.js` or a config pointing at an authenticated API.

Do not publish:

- `platform_parquet/base_fact`.
- Raw source rows.
- Any row-level alumni/person data.
- Large generated JSON bundles.

## API Fallback Password

The starter API supports a fallback password using:

```text
OUTCOMES_APP_PASSWORD=<long random password>
```

Requests to `/api/query` must include:

```text
X-Outcomes-Password: <password>
```

This only protects the API if the API is served over HTTPS and the password is not embedded directly in public frontend code. It is a fallback, not the target production auth model.

## Production Auth Recommendation

For the internal demo, use Cloudflare Access or equivalent SSO:

```text
Internal user -> SSO gate -> GitHub Pages frontend
Internal user -> SSO gate -> Outcomes API -> Parquet/Snowflake data
```

Then leave `OUTCOMES_APP_PASSWORD` unset or keep it as a second layer.
