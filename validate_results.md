# Campaign Site Validation Results

## Pipeline Stage: Validate

The **validate** stage is the 4th and final stage of the camplinks pipeline (`scrape -> enrich -> search -> validate`). Campaign websites go stale over time as domains expire or sites are taken down. This stage recovers those dead links by checking every candidate's `campaign_site` URL for accessibility and, for any that are down, querying the Internet Archive's Wayback Machine for the most recent archived snapshot. Archived URLs are written back to the database as a new `campaign_site_archived` contact link, preserving the original URL alongside its archive.

The stage is idempotent and resumable -- candidates with an existing archived link are skipped, and progress is cached to `validate_cache.json` so interrupted runs can pick up where they left off.

```bash
# Run validate for a specific year/race
python -m camplinks --year 2024 --race all --stage validate
```

## Results

**Run date:** 2026-02-27

| Year | Checked | Accessible | Inaccessible | Archived | No Archive |
|------|---------|------------|--------------|----------|------------|
| 2024 | 807 | 701 | 106 | 87 | 19 |
| 2025 | 751 | 596 | 155 | 128 | 27 |
| **Total** | **1,558** | **1,297** | **261** | **215** | **46** |

## Key Metrics

- **83.2%** of campaign sites are still accessible
- **82.4%** of inaccessible sites had a Wayback Machine archive available
- **215** archived URLs written to the database as `campaign_site_archived` links

## Method

1. HTTP HEAD request with 10s timeout (GET fallback on 405)
2. Status < 400 considered accessible
3. Inaccessible sites queried against `https://archive.org/wayback/available`
4. Archived snapshots stored with `link_type = "campaign_site_archived"` and `source = "wayback"`
