# 🔭 GitHub Scout

**Production-grade GitHub repository intelligence spider with analytical scoring.**

GitHub Scout crawls GitHub's GraphQL & REST APIs, persists structured repository data into DuckDB, and computes a composite *potential score* to surface emerging high-impact Python and Jupyter projects.

---

## ✨ Features

- **Paginated GraphQL v4 search** — cursor-based pagination with unlimited page support
- **Automatic query slicing** — bypasses GitHub's 1,000-result search limit by splitting queries into date-range partitions
- **REST enrichment** — README quality analysis, contributor counts, release metadata
- **DuckDB persistence** — embedded columnar database with historical snapshots
- **Polars scoring pipeline** — multi-factor composite score (0–100) combining star velocity, recency, activity, README quality, and 7-day momentum
- **Typer CLI** — 6 commands: `crawl`, `score`, `top`, `stats`, `export`, `clean`
- **Database maintenance** — flexible `clean` command to purge stale, low-score, archived, or forked repos with dry-run preview
- **Smart re-crawl** — TTL-based three-tier strategy: skip enrichment for fresh repos, re-enrich stale ones, and always insert new repos; saves API quota dramatically
- **Incremental updates** — delta scraping with upsert logic, preserving original scrape timestamps
- **Smart rate-limit handling** — aligned with GitHub's official API policies for both primary and secondary limits
- **Live progress panel** — real-time Rich dashboard showing pages, repos, quotas, and elapsed time

---

## 📦 Installation

```bash
# Clone the repo
git clone https://github.com/your-user/github-scout.git
cd github-scout

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### Requirements

- Python 3.12+
- A [GitHub Personal Access Token](https://github.com/settings/tokens) with `public_repo` scope

---

## ⚙️ Configuration

Copy the example env file and add your token:

```bash
cp .env.example .env
```

Edit `.env`:

```env
GITHUB_TOKEN=ghp_your_token_here
```

All settings can be overridden via environment variables:

| Variable | Default | Description |
|---|---|---|
| `GITHUB_TOKEN` | *(required)* | GitHub PAT |
| `DB_PATH` | `./github_scout.duckdb` | DuckDB database path |
| `LOG_LEVEL` | `INFO` | Logging level |
| `DEFAULT_QUERY` | `language:python language:"Jupyter Notebook" stars:>100 created:>2026-01-01` | Search query |
| `MAX_PAGES` | `None` *(unlimited)* | Max pages to paginate per query slice (set to cap) |
| `MAX_CONCURRENT_ENRICHMENTS` | `10` | Concurrent REST enrichment calls |
| `REFRESH_TTL_HOURS` | `24` | Hours before a repo is considered stale and re-enriched |
| `SNAPSHOT_TTL_HOURS` | `6` | Minimum hours between snapshots (avoids spam) |
| `FORCE_REFRESH` | `false` | Force full re-enrichment on all repos regardless of TTL |

---

## 🚀 Usage

### 1. Crawl repositories

```bash
# Use default query (fetches ALL results, auto-slicing if >1,000)
github-scout crawl

# Custom query with limited pages
github-scout crawl --query "machine learning stars:>500" --max-pages 5

# Force re-enrichment for all repos (ignore TTL)
github-scout crawl --force-refresh

# Set a custom TTL (re-enrich repos older than 12 hours)
github-scout crawl --refresh-ttl 12
```

During the crawl, a **live progress panel** displays real-time status:

```
┌── Crawling GitHub  language:python stars:>50 created:>2025-01-01...  ──┐
│                                                                        │
│  Slice:        3/19          Page:    7 / ~10                          │
│  Repos found:  2,700         Elapsed: 8m 34s                          │
│  🆕 New:       2,580         🔄 Refreshed: 18                         │
│  ⏩ Skipped:   102           📸 Snapshots: 2,698                      │
│  Errors:       0             GraphQL quota: 4,832/5,000 (cost: 1)     │
│  Status:       Enriching 100 repos (REST)...                           │
│                              REST quota:    4,215/5,000                │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

After crawl completion, a **summary panel** is displayed:

```
╭─── Crawl Summary ────────────────────────────╮
│  🆕 New repos:          42                   │
│  🔄 Refreshed (stale):  18                   │
│  ⏩ Skipped (fresh):    95                   │
│  📸 Snapshots taken:    60                   │
│  ❌ Errors:             2                    │
│  ⏱  Duration:           47.3s                │
╰──────────────────────────────────────────────╯
```

#### Smart Re-crawl Strategy

On re-crawl, each repository is classified into one of three tiers:

| Tier | Condition | Action | REST calls |
|---|---|---|---|
| **🆕 NEW** | Not in DB | Full enrichment + insert + snapshot | ✅ Yes |
| **🔄 REFRESH** | In DB, older than `REFRESH_TTL_HOURS` | Full re-enrichment + upsert + snapshot | ✅ Yes |
| **⏩ SKIP-ENRICH** | In DB, fresher than `REFRESH_TTL_HOURS` | Lightweight update (stars, forks, issues only) + conditional snapshot | ❌ No |

Snapshots in the SKIP-ENRICH tier are only taken if the last snapshot is older than `SNAPSHOT_TTL_HOURS`, avoiding snapshot spam.

Use `--force-refresh` to override TTL and re-enrich all repos.

### 2. Compute scores

```bash
github-scout score
```

### 3. View top repositories

```bash
github-scout top
github-scout top --limit 50
```

### 4. View analytics

```bash
github-scout stats
```

Displays:
- Language distribution
- Topic heatmap
- Star velocity percentiles (p50 / p90 / p99)
- Score distribution histogram
- 7-day trending repos

### 5. Export data

```bash
# CSV
github-scout export -o results.csv

# Parquet
github-scout export -o results.parquet
```

### 6. Clean / purge data

Remove stale or unwanted repositories from the database. **Dry-run is ON by default** — no data is modified until you pass `--execute`.

```bash
# Preview what would be deleted (dry-run)
github-scout clean --archived --forks

# Delete repos scraped before a date
github-scout clean --before 2026-01-01 --execute

# Delete low-score repos (score < 10 or unscored)
github-scout clean --score-below 10 --execute

# Delete repos by language
github-scout clean --language Ruby --execute

# Remove orphan snapshots (no matching repo)
github-scout clean --orphan-snapshots --execute

# Full purge: truncate repositories, snapshots, and crawl runs
github-scout clean --all --execute

# Skip confirmation prompt (for CI/automation)
github-scout clean --archived --execute --yes
```

Filters can be combined (AND logic):

```bash
# Archived forks with score below 20
github-scout clean --archived --forks --score-below 20 --execute
```

| Flag | Filter |
|---|---|
| `--all` | Truncate all three tables |
| `--before YYYY-MM-DD` | `scraped_at < date` |
| `--score-below N` | `potential_score < N` or unscored |
| `--archived` | `is_archived = true` |
| `--forks` | `is_fork = true` |
| `--language X` | `primary_language` (case-insensitive) |
| `--orphan-snapshots` | Snapshots with no matching repo |
| `--dry-run / --execute` | Preview vs. apply (default: dry-run) |
| `--yes, -y` | Skip confirmation prompt |

---

## 🛡️ API Resilience

GitHub Scout implements a multi-layered strategy for dealing with API rate limits and transient errors, aligned with [GitHub's official documentation](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).

### Rate-limit handling

| Policy | How it's handled |
|---|---|
| **Primary rate limit** (5,000 req/h REST, 5,000 points/h GraphQL) | Preemptive pause when remaining quota drops below 200, sleeping until `x-ratelimit-reset` |
| **Secondary rate limit** (429/403 with `retry-after`) | Respects the `retry-after` header exactly as GitHub specifies |
| **GraphQL body errors** | Detects rate-limit errors returned as 200 status with error in JSON body |
| **Quota visibility** | Live progress panel shows remaining/total for both GraphQL and REST with color-coded status (green → yellow → red) |

### Smart retry logic

The HTTP client uses **tenacity** with a custom retry predicate that only retries transient errors:

| Error type | Retried? | Reason |
|---|---|---|
| `429 Too Many Requests` | ✅ | Primary rate limit exceeded |
| `403` with `retry-after` header | ✅ | Secondary rate limit |
| `5xx` (502 Bad Gateway, etc.) | ✅ | Transient server errors |
| `TransportError` (network) | ✅ | Connection issues |
| `400`, `401`, `404`, `422` | ❌ | Client bugs — retrying won't help |

Retry configuration:
- **GraphQL**: up to 8 attempts, exponential backoff 4s → 120s (multiplier × 2)
- **REST**: up to 7 attempts, same backoff strategy
- **Per-page retry**: up to 3 consecutive same-cursor retries with 30s delay

### Automatic query slicing (1,000-result bypass)

GitHub's Search API returns **at most 1,000 results** per query, regardless of pagination. When a query matches more results, the spider:

1. **Probes** the query to get `repositoryCount`
2. **Splits** the date range into slices, each sized to return <1,000 results
3. **Iterates** each slice independently, accumulating all repos

```
Query: language:python stars:>50 created:>2025-01-01
Total: 13,000 results → 19 date slices (~23 days each)

Slice  1: language:python stars:>50 created:2025-01-02..2025-01-24
Slice  2: language:python stars:>50 created:2025-01-25..2025-02-16
...
Slice 19: language:python stars:>50 created:2026-02-20..2026-02-24
```

Supported `created:` qualifier formats:

| Format | Example |
|---|---|
| `created:>YYYY-MM-DD` | `created:>2025-01-01` |
| `created:>=YYYY-MM-DD` | `created:>=2025-01-01` |
| `created:START..END` | `created:2025-01-01..2025-06-30` |
| *(no created qualifier)* | Uses full range 2008-01-01 to today |

---

## 🧮 Scoring Algorithm

The **potential score** (0–100) is a weighted composite designed to surface emerging high-impact projects. Instead of comparing all repositories linearly, repositories are evaluated using **percentiles** within their respective **age** and **maturity** cohorts to prevent massive established projects from overshadowing smaller, promising ones.

### Cohort Tiers

| Tier | Definition |
|---|---|
| **Age** | `Emerging` (< 6 months), `Growing` (6-24 months), `Established` (> 24 months) |
| **Maturity** | `Seed` (< 100 stars), `Traction` (100–1,000 stars), `Scale` (> 1,000 stars) |

### Scoring Factors

| Factor | Weight | Description |
|---|---|---|
| Star velocity | 35% | Percentile rank of stars per day since creation within tier |
| Recency decay | 20% | Exponential decay for repos older than 90 days |
| Activity | 20% | Percentile rank of `log1p(forks + open_issues + contributors)` within tier |
| 7-day momentum | 15% | Percentile rank of star growth delta from historical snapshots within tier |
| README quality | 10% | Badges, sections, install instructions, demos |

---

## 🏗️ Architecture

```
github_scout/
├── config/          # Settings + GraphQL queries
├── models/          # Pydantic data models
├── client/          # httpx async client, rate limiter, paginator
├── database/        # DuckDB connection, schema DDL, DAO + clean/purge
├── crawler/         # REST enricher, spider orchestrator, query slicer
├── scoring/         # Polars feature engineering + scorer
├── analytics/       # SQL query constants
└── cli/             # Typer CLI entry point
```

### Key modules

| Module | Purpose |
|---|---|
| `client/github_client.py` | Async HTTP client with tenacity retry and rate-limit awareness |
| `client/rate_limiter.py` | Primary + secondary rate-limit handling per GitHub's docs |
| `client/paginator.py` | Cursor-based GraphQL pagination + probe helper |
| `crawler/query_slicer.py` | Date-range partitioning to bypass 1,000-result cap |
| `crawler/spider.py` | Crawl orchestrator with Rich live progress panel |
| `crawler/enricher.py` | REST-based README, contributor, and release enrichment |

### Database Tables

- **`repositories`** — main table with all repo metadata, enrichment, and scores
- **`repo_snapshots`** — point-in-time star/fork/issue counts for momentum tracking
- **`crawl_runs`** — metadata for each crawl execution

---

## 🧪 Testing

```bash
pytest tests/ -v
```

Tests cover:
- **Pagination** — stops on `hasNextPage=false`
- **Retry** — 429 responses trigger tenacity exponential backoff
- **DAO** — upsert idempotency (1 row in repos, 2 in snapshots)
- **Scoring** — scores always in `[0, 100]`, no nulls in output

---

## 📄 License

[MIT](LICENSE)
